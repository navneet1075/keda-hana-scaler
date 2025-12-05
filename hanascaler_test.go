package main

import (
	"context"
	"database/sql"
	"os"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	pb "github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConnectionFromEnv(t *testing.T) {
	tests := []struct {
		name        string
		envVars     map[string]string
		wantErr     bool
		errContains string
	}{
		{
			name: "valid configuration",
			envVars: map[string]string{
				"HANA_HOST":     "localhost",
				"HANA_PORT":     "30015",
				"HANA_USERNAME": "testuser",
				"HANA_PASSWORD": "testpass",
			},
			wantErr: false,
		},
		{
			name: "missing host",
			envVars: map[string]string{
				"HANA_PORT":     "30015",
				"HANA_USERNAME": "testuser",
				"HANA_PASSWORD": "testpass",
			},
			wantErr:     true,
			errContains: "HANA_HOST",
		},
		{
			name: "missing port",
			envVars: map[string]string{
				"HANA_HOST":     "localhost",
				"HANA_USERNAME": "testuser",
				"HANA_PASSWORD": "testpass",
			},
			wantErr:     true,
			errContains: "HANA_PORT",
		},
		{
			name: "missing username",
			envVars: map[string]string{
				"HANA_HOST":     "localhost",
				"HANA_PORT":     "30015",
				"HANA_PASSWORD": "testpass",
			},
			wantErr:     true,
			errContains: "HANA_USER",
		},
		{
			name: "missing password",
			envVars: map[string]string{
				"HANA_HOST":     "localhost",
				"HANA_PORT":     "30015",
				"HANA_USERNAME": "testuser",
			},
			wantErr:     true,
			errContains: "HANA_PASSWORD",
		},
		{
			name:        "all missing",
			envVars:     map[string]string{},
			wantErr:     true,
			errContains: "missing required environment variables",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all relevant env vars first
			os.Unsetenv("HANA_HOST")
			os.Unsetenv("HANA_PORT")
			os.Unsetenv("HANA_USERNAME")
			os.Unsetenv("HANA_PASSWORD")

			// Set test env vars
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			config, err := parseConnectionFromEnv()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)
				assert.Equal(t, tt.envVars["HANA_HOST"], config.Host)
				assert.Equal(t, tt.envVars["HANA_PORT"], config.Port)
				assert.Equal(t, tt.envVars["HANA_USERNAME"], config.User)
				assert.Equal(t, tt.envVars["HANA_PASSWORD"], config.Password)
			}

			// Cleanup
			for k := range tt.envVars {
				os.Unsetenv(k)
			}
		})
	}
}

func TestParseMetadata(t *testing.T) {
	// Setup env vars for connection config
	os.Setenv("HANA_HOST", "localhost")
	os.Setenv("HANA_PORT", "30015")
	os.Setenv("HANA_USERNAME", "testuser")
	os.Setenv("HANA_PASSWORD", "testpass")
	defer func() {
		os.Unsetenv("HANA_HOST")
		os.Unsetenv("HANA_PORT")
		os.Unsetenv("HANA_USERNAME")
		os.Unsetenv("HANA_PASSWORD")
	}()

	tests := []struct {
		name                    string
		metadata                map[string]string
		wantErr                 bool
		expectedQuery           string
		expectedThreshold       int64
		expectedActivationThres int64
	}{
		{
			name: "valid metadata with defaults",
			metadata: map[string]string{
				"query": "SELECT COUNT(*) FROM messages",
			},
			wantErr:                 false,
			expectedQuery:           "SELECT COUNT(*) FROM messages",
			expectedThreshold:       10,
			expectedActivationThres: 1,
		},
		{
			name: "valid metadata with custom thresholds",
			metadata: map[string]string{
				"query":               "SELECT COUNT(*) FROM messages",
				"threshold":           "50",
				"activationThreshold": "5",
			},
			wantErr:                 false,
			expectedQuery:           "SELECT COUNT(*) FROM messages",
			expectedThreshold:       50,
			expectedActivationThres: 5,
		},
		{
			name:     "missing query",
			metadata: map[string]string{},
			wantErr:  true,
		},
		{
			name: "invalid threshold",
			metadata: map[string]string{
				"query":     "SELECT COUNT(*) FROM messages",
				"threshold": "invalid",
			},
			wantErr: true,
		},
		{
			name: "invalid activation threshold",
			metadata: map[string]string{
				"query":               "SELECT COUNT(*) FROM messages",
				"activationThreshold": "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, err := parseMetadata(tt.metadata)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, meta)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, meta)
				assert.Equal(t, tt.expectedQuery, meta.Query)
				assert.Equal(t, tt.expectedThreshold, meta.Threshold)
				assert.Equal(t, tt.expectedActivationThres, meta.ActivationThreshold)
				assert.Equal(t, "localhost", meta.Host)
				assert.Equal(t, "30015", meta.Port)
			}
		})
	}
}

func TestQueryMetric(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		mockResult     int64
		mockError      error
		expectedResult int64
		wantErr        bool
	}{
		{
			name:           "successful query",
			query:          "SELECT COUNT(*) FROM messages",
			mockResult:     42,
			mockError:      nil,
			expectedResult: 42,
			wantErr:        false,
		},
		{
			name:           "query returns zero",
			query:          "SELECT COUNT(*) FROM messages WHERE status='pending'",
			mockResult:     0,
			mockError:      nil,
			expectedResult: 0,
			wantErr:        false,
		},
		{
			name:      "query execution fails",
			query:     "SELECT COUNT(*) FROM nonexistent",
			mockError: sql.ErrNoRows,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Use regexp.QuoteMeta to escape special regex characters in SQL
			escapedQuery := "(?i)" + regexp.QuoteMeta(tt.query)

			if tt.mockError != nil {
				mock.ExpectQuery(escapedQuery).WillReturnError(tt.mockError)
			} else {
				rows := sqlmock.NewRows([]string{"count"}).AddRow(tt.mockResult)
				mock.ExpectQuery(escapedQuery).WillReturnRows(rows)
			}

			result, err := queryMetric(db, tt.query)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestHANAScaler_IsActive(t *testing.T) {
	// Setup env vars
	os.Setenv("HANA_HOST", "localhost")
	os.Setenv("HANA_PORT", "30015")
	os.Setenv("HANA_USERNAME", "testuser")
	os.Setenv("HANA_PASSWORD", "testpass")
	defer func() {
		os.Unsetenv("HANA_HOST")
		os.Unsetenv("HANA_PORT")
		os.Unsetenv("HANA_USERNAME")
		os.Unsetenv("HANA_PASSWORD")
	}()

	tests := []struct {
		name            string
		metadata        map[string]string
		mockQueryResult int64
		expectedActive  bool
		wantErr         bool
	}{
		{
			name: "active when above threshold",
			metadata: map[string]string{
				"query":               "SELECT COUNT(*) FROM messages",
				"activationThreshold": "5",
			},
			mockQueryResult: 10,
			expectedActive:  true,
			wantErr:         false,
		},
		{
			name: "inactive when below threshold",
			metadata: map[string]string{
				"query":               "SELECT COUNT(*) FROM messages",
				"activationThreshold": "5",
			},
			mockQueryResult: 3,
			expectedActive:  false,
			wantErr:         false,
		},
		{
			name: "active when equal to threshold",
			metadata: map[string]string{
				"query":               "SELECT COUNT(*) FROM messages",
				"activationThreshold": "5",
			},
			mockQueryResult: 5,
			expectedActive:  true,
			wantErr:         false,
		},
		{
			name: "invalid metadata",
			metadata: map[string]string{
				"threshold": "10",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scaler := &HANAScaler{}
			req := &pb.ScaledObjectRef{
				ScalerMetadata: tt.metadata,
			}
			_, err := scaler.IsActive(context.Background(), req)

			if tt.wantErr {
				assert.Error(t, err)
			}
		})
	}
}

func TestHANAScaler_GetMetricSpec(t *testing.T) {
	// Setup env vars
	os.Setenv("HANA_HOST", "localhost")
	os.Setenv("HANA_PORT", "30015")
	os.Setenv("HANA_USERNAME", "testuser")
	os.Setenv("HANA_PASSWORD", "testpass")
	defer func() {
		os.Unsetenv("HANA_HOST")
		os.Unsetenv("HANA_PORT")
		os.Unsetenv("HANA_USERNAME")
		os.Unsetenv("HANA_PASSWORD")
	}()

	tests := []struct {
		name              string
		metadata          map[string]string
		expectedThreshold int64
		wantErr           bool
	}{
		{
			name: "returns correct metric spec",
			metadata: map[string]string{
				"query":     "SELECT COUNT(*) FROM messages",
				"threshold": "25",
			},
			expectedThreshold: 25,
			wantErr:           false,
		},
		{
			name: "uses default threshold",
			metadata: map[string]string{
				"query": "SELECT COUNT(*) FROM messages",
			},
			expectedThreshold: 10,
			wantErr:           false,
		},
		{
			name: "invalid metadata",
			metadata: map[string]string{
				"threshold": "10",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scaler := &HANAScaler{}
			req := &pb.ScaledObjectRef{
				ScalerMetadata: tt.metadata,
			}

			resp, err := scaler.GetMetricSpec(context.Background(), req)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, resp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.Len(t, resp.MetricSpecs, 1)
				assert.Equal(t, "hana-metric", resp.MetricSpecs[0].MetricName)
				assert.Equal(t, tt.expectedThreshold, resp.MetricSpecs[0].TargetSize)
			}
		})
	}
}

func TestHANAScaler_StreamIsActive(t *testing.T) {
	scaler := &HANAScaler{}
	req := &pb.ScaledObjectRef{
		ScalerMetadata: map[string]string{
			"query": "SELECT COUNT(*) FROM messages",
		},
	}

	err := scaler.StreamIsActive(req, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestConnectionConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		config ConnectionConfig
		valid  bool
	}{
		{
			name: "valid standard config",
			config: ConnectionConfig{
				Host:     "localhost",
				Port:     "30015",
				User:     "testuser",
				Password: "testpass",
			},
			valid: true,
		},
		{
			name: "valid cloud config",
			config: ConnectionConfig{
				Host:     "example.hanacloud.ondemand.com",
				Port:     "443",
				User:     "clouduser",
				Password: "cloudpass",
			},
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotEmpty(t, tt.config.Host)
			assert.NotEmpty(t, tt.config.Port)
			assert.NotEmpty(t, tt.config.User)
			assert.NotEmpty(t, tt.config.Password)
		})
	}
}

// Benchmark tests
func BenchmarkParseMetadata(b *testing.B) {
	os.Setenv("HANA_HOST", "localhost")
	os.Setenv("HANA_PORT", "30015")
	os.Setenv("HANA_USERNAME", "testuser")
	os.Setenv("HANA_PASSWORD", "testpass")
	defer func() {
		os.Unsetenv("HANA_HOST")
		os.Unsetenv("HANA_PORT")
		os.Unsetenv("HANA_USERNAME")
		os.Unsetenv("HANA_PASSWORD")
	}()

	metadata := map[string]string{
		"query":               "SELECT COUNT(*) FROM messages",
		"threshold":           "50",
		"activationThreshold": "5",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parseMetadata(metadata)
	}
}
