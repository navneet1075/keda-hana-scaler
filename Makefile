REGISTRY ?= docker.io/pathaknavneet
IMAGE_NAME ?= hana-keda-scaler
VERSION ?= v0.0.1

.PHONY: build
build:
	docker build -t $(REGISTRY)/$(IMAGE_NAME):$(VERSION) .
	docker tag $(REGISTRY)/$(IMAGE_NAME):$(VERSION) $(REGISTRY)/$(IMAGE_NAME):latest

.PHONY: push
push:
	docker push $(REGISTRY)/$(IMAGE_NAME):$(VERSION)
	docker push $(REGISTRY)/$(IMAGE_NAME):latest

.PHONY: build-push
build-push: build push

.PHONY: test-local
test-local:
	go run main.go