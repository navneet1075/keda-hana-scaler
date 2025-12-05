package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SAP/go-hdb/driver"
	pb "github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
)

type HANAScaler struct {
	pb.UnimplementedExternalScalerServer
}

type ConnectionConfig struct {
	Host     string
	Port     string
	User     string
	Password string
}

// ScalerMetadata holds scaler configuration
type ScalerMetadata struct {
	Query               string
	Threshold           int64
	ActivationThreshold int64
	ConnectionConfig
}

func parseConnectionFromEnv() (*ConnectionConfig, error) {
	config := &ConnectionConfig{
		Host:     os.Getenv("HANA_HOST"),
		Port:     os.Getenv("HANA_PORT"),
		User:     os.Getenv("HANA_USERNAME"),
		Password: os.Getenv("HANA_PASSWORD"),
	}

	missingFields := []string{}
	if config.Host == "" {
		missingFields = append(missingFields, "HANA_HOST")
	}
	if config.Port == "" {
		missingFields = append(missingFields, "HANA_PORT")
	}
	if config.User == "" {
		missingFields = append(missingFields, "HANA_USER")
	}
	if config.Password == "" {
		missingFields = append(missingFields, "HANA_PASSWORD")
	}

	if len(missingFields) > 0 {
		log.Printf("FATAL ERROR: Required environment variables missing: %v", missingFields)
		return nil, fmt.Errorf("missing required environment variables: %v", missingFields)
	}

	log.Printf("Connection Config loaded from ENV")
	log.Printf("  Host: %s", config.Host)
	log.Printf("  User: %s", config.User)
	return config, nil
}

// parseMetadata extracts configuration from ScaledObject metadata
func parseMetadata(metadata map[string]string) (*ScalerMetadata, error) {
	log.Printf("=== Parsing Metadata from KEDA ===")
	for key, value := range metadata {
		if key == "password" {
			log.Printf("  %s: [REDACTED]", key)
		} else {
			log.Printf("  %s: %s", key, value)
		}
	}

	meta := &ScalerMetadata{
		Threshold:           10,
		ActivationThreshold: 1,
	}

	if val, ok := metadata["query"]; ok {
		meta.Query = val
	} else {
		return nil, fmt.Errorf("query is required")
	}

	if val, ok := metadata["threshold"]; ok {
		threshold, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid threshold: %v", err)
		}
		meta.Threshold = threshold
	}

	if val, ok := metadata["activationThreshold"]; ok {
		threshold, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid activationThreshold: %v", err)
		}
		meta.ActivationThreshold = threshold
	}

	connConfig, err := parseConnectionFromEnv()
	if err != nil {
		return nil, err
	}
	meta.ConnectionConfig = *connConfig

	log.Printf(" Metadata parsed successfully")
	return meta, nil
}

// getConnection establishes connection to HANA DB
func getConnection(config ConnectionConfig) (*sql.DB, error) {
	isHANACloud := config.Port == "443"
	var db *sql.DB
	var err error

	addr := net.JoinHostPort(config.Host, config.Port)

	if isHANACloud {
		log.Printf(" Connecting to HANA Cloud: %s", addr)
		tlsConfig := &tls.Config{
			ServerName:         config.Host,
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}

		connector := driver.NewBasicAuthConnector(
			addr,
			config.User, // Use updated field name
			config.Password,
		)
		connector.SetTLSConfig(tlsConfig)
		log.Printf(" TLS config set successfully on connector")

		db = sql.OpenDB(connector)

	} else {
		log.Printf("Connecting to standard HANA: %s", addr)

		connStr := fmt.Sprintf("hdb://%s:%s@%s",
			config.User, config.Password, addr)

		db, err = sql.Open("hdb", connStr)
		if err != nil {
			log.Printf(" Failed to open connection: %v", err)
			return nil, fmt.Errorf("failed to open connection: %v", err)
		}
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Minute * 5)

	log.Printf(" Testing connection with Ping...")
	err = db.Ping()
	if err != nil {
		log.Printf(" Failed to ping database: %v", err)
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	log.Printf(" Successfully connected to HANA database")
	return db, nil
}

func queryMetric(db *sql.DB, query string) (int64, error) {
	var result int64
	err := db.QueryRow(query).Scan(&result)
	if err != nil {
		log.Printf(" Query execution failed: %v", err)
		return 0, fmt.Errorf("query execution failed: %v", err)
	}
	log.Printf("Query result: %d", result)
	return result, nil
}

func (s *HANAScaler) IsActive(ctx context.Context, req *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	log.Printf("IsActive called")
	log.Printf("Received metadata: %v", req.GetScalerMetadata())
	metadata, err := parseMetadata(req.GetScalerMetadata())
	if err != nil {
		log.Printf("Failed to parse metadata: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse metadata: %v", err)
	}
	db, err := getConnection(metadata.ConnectionConfig)
	if err != nil {
		log.Printf(" Failed to connect to HANA: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to connect to HANA: %v", err)
	}
	defer db.Close()

	metricValue, err := queryMetric(db, metadata.Query)
	if err != nil {
		log.Printf("Failed to query metric: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to query metric: %v", err)
	}
	isActive := metricValue >= metadata.ActivationThreshold
	log.Printf("IsActive check: metricValue=%d, activationThreshold=%d, isActive=%v",
		metricValue, metadata.ActivationThreshold, isActive)

	return &pb.IsActiveResponse{
		Result: isActive,
	}, nil
}

// GetMetricSpec returns the metric specification
func (s *HANAScaler) GetMetricSpec(ctx context.Context, req *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	metadata, err := parseMetadata(req.GetScalerMetadata())
	if err != nil {
		log.Printf(" Failed to parse metadata: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse metadata: %v", err)
	}
	log.Printf("Returning metric spec: threshold=%d", metadata.Threshold)
	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{{
			MetricName: "hana-metric",
			TargetSize: metadata.Threshold,
		}},
	}, nil
}

// GetMetrics returns the current metric values
func (s *HANAScaler) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	log.Printf("GetMetrics called")

	metadata, err := parseMetadata(req.ScaledObjectRef.ScalerMetadata)
	if err != nil {
		log.Printf("Failed to parse metadata: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse metadata: %v", err)
	}

	db, err := getConnection(metadata.ConnectionConfig)
	if err != nil {
		log.Printf("Failed to connect to HANA: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to connect to HANA: %v", err)
	}
	defer db.Close()

	metricValue, err := queryMetric(db, metadata.Query)
	if err != nil {
		log.Printf("Failed to query metric: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to query metric: %v", err)
	}

	log.Printf(" GetMetrics: metricValue=%d", metricValue)

	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{{
			MetricName:  "hana-metric",
			MetricValue: metricValue,
		}},
	}, nil
}

// StreamIsActive handles streaming IsActive checks (optional)
func (s *HANAScaler) StreamIsActive(req *pb.ScaledObjectRef, stream pb.ExternalScaler_StreamIsActiveServer) error {
	return status.Errorf(codes.Unimplemented, "streaming is not supported")
}

func main() {
	log.Printf("🚀 Starting HANA KEDA External Scaler...")
	
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "6000"
	}
	
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", port, err)
	}
	
	grpcServer := grpc.NewServer()
	pb.RegisterExternalScalerServer(grpcServer, &HANAScaler{})
	
	serverErrors := make(chan error, 1)
	
	go func() {
		log.Printf(" gRPC server listening on port %s", port)
		serverErrors <- grpcServer.Serve(lis)
	}()
	
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	
	// Block until we receive a signal or an error
	select {
	case err := <-serverErrors:
		log.Fatalf("Failed to serve: %v", err)
	case sig := <-shutdown:
		log.Printf("Received signal: %v, initiating graceful shutdown...", sig)
		
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		
		// Wait for graceful shutdown or timeout
		select {
		case <-ctx.Done():
			log.Printf("⚠️  Shutdown timeout, forcing stop...")
			grpcServer.Stop()
		case <-stopped:
			log.Printf("✅ Server stopped gracefully")
		}
	}
}