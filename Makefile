# Container image builds via ko (https://ko.build) - no Dockerfile needed,
# ko builds and pushes directly from Go import paths.
#
# KO_DOCKER_REPO must point at the registry/repository to push images to,
# e.g. ghcr.io/02strich/temporal-untrusted-workers. Each image is published
# under its own path within that repo (via --base-import-paths):
#   $(KO_DOCKER_REPO)/temporal-proxy
#   $(KO_DOCKER_REPO)/verify-worker
#
# Usage:
#   make images KO_DOCKER_REPO=ghcr.io/02strich/temporal-untrusted-workers
#   make image-proxy KO_DOCKER_REPO=ghcr.io/you/repo TAGS=v0.1.0
#
# examples/verify-worker lives in its own Go module (examples/go.mod - it
# depends on an older go.temporal.io/sdk that conflicts with this module's
# go.temporal.io/api version), so its build runs with examples/ as the
# working directory.

KO ?= ko
TAGS ?= latest
PLATFORMS ?= linux/amd64,linux/arm64
IMAGE_SOURCE := https://github.com/02strich/temporal-untrusted-workers

.PHONY: images image-proxy image-verify-worker ko-install check-ko check-repo

images: image-proxy image-verify-worker

image-proxy: check-ko check-repo
	KO_DOCKER_REPO=$(KO_DOCKER_REPO) $(KO) build \
		--base-import-paths \
		--platform=$(PLATFORMS) \
		--tags=$(TAGS) \
		--image-label=org.opencontainers.image.source=$(IMAGE_SOURCE) \
		./cmd/temporal-proxy

image-verify-worker: check-ko check-repo
	cd examples && KO_DOCKER_REPO=$(KO_DOCKER_REPO) $(KO) build \
		--base-import-paths \
		--platform=$(PLATFORMS) \
		--tags=$(TAGS) \
		--image-label=org.opencontainers.image.source=$(IMAGE_SOURCE) \
		./verify-worker

ko-install:
	go install github.com/google/ko@latest

check-ko:
	@command -v $(KO) >/dev/null 2>&1 || { echo "ko not found on PATH - install it with 'make ko-install' or see https://ko.build/install/"; exit 1; }

check-repo:
	@test -n "$(KO_DOCKER_REPO)" || { echo "KO_DOCKER_REPO must be set, e.g. make images KO_DOCKER_REPO=ghcr.io/02strich/temporal-untrusted-workers"; exit 1; }
