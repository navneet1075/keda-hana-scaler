#!/bin/bash
set -e

REGISTRY="pathaknavneet"
VERSION="v0.0.1"

echo "🚀 Deploying HANA KEDA Scaler..."

# Build and push
echo "📦 Building Docker image..."
docker build -t ${REGISTRY}/hana-keda-scaler:${VERSION} -f Dockerfile .

echo "⬆️  Pushing to registry..."
docker push ${REGISTRY}/hana-keda-scaler:${VERSION}

# Deploy to Kubernetes
echo "☸️  Deploying to Kubernetes..."
kubectl apply -f k8s/hana-credentials.yaml
kubectl apply -f k8s/hana-scaler-deploy.yaml
kubectl apply -f k8s/hana-scaler-service.yaml

echo "⏳ Waiting for deployment..."
kubectl rollout status deployment/keda-hana-scaler

echo "🎯 Creating ScaledObject..."
kubectl apply -f k8s/scaledobject.yaml

echo "✅ Deployment complete!"
# echo ""
# echo "📊 Check status with:"
# echo "  kubectl get pods -l app=hana-scaler"
# echo "  kubectl logs -l app=hana-scaler -f"
# echo "  kubectl get scaledobject my-app-scaler"