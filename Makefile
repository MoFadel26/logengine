IMAGE       ?= logengine-api
TAG         ?= latest
PLATFORMS   ?= linux/arm64,linux/amd64
DOCKERFILE  := deploy/docker/Dockerfile
BUILDER     ?= logengine-builder

.PHONY: buildx buildx-builder

# Ensure a buildx builder that supports multi-platform output exists.
buildx-builder:
	docker buildx inspect $(BUILDER) >/dev/null 2>&1 || \
		docker buildx create --name $(BUILDER) --use
	docker buildx use $(BUILDER)

# Cross-build the API image for arm64 + amd64 from this M1 host.
# NOTE: with multiple --platform values and no --push/--output, buildx
# builds and validates both platforms but discards the result (a
# multi-arch manifest can't be loaded into the local docker daemon).
# Add `--push` (with a real registry tag) to publish the manifest list,
# or drop to a single platform with --load for a local, runnable image.
buildx: buildx-builder
	docker buildx build \
		--platform $(PLATFORMS) \
		-f $(DOCKERFILE) \
		-t $(IMAGE):$(TAG) \
		.
