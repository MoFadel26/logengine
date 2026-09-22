# syntax=docker/dockerfile:1

# ---- Stage 1: build ---------------------------------------------------
# --platform=$BUILDPLATFORM pins this stage to the build host's native
# platform (e.g. linux/arm64 on M1 Pro), so `go build` runs natively and
# cross-compiles for TARGETOS/TARGETARCH via GOOS/GOARCH. This is what
# makes `docker buildx build --platform linux/arm64,linux/amd64` fast on
# a single host instead of emulating the target arch with QEMU.
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

# ---- Stage 2: runtime ---------------------------------------------------
FROM gcr.io/distroless/static:nonroot AS runtime

COPY --from=build /out/api /app/api

# distroless:nonroot already runs as UID/GID 65532, set explicitly anyway
# per the non-root requirement.
USER nonroot:nonroot

EXPOSE 8080

STOPSIGNAL SIGTERM

# Exec-form ENTRYPOINT: /app/api runs directly as PID 1 and receives
# SIGTERM straight from the container runtime, so graceful shutdown
# (SHUTDOWN_TIMEOUT) works without anything forwarding the signal.
# Do NOT switch this to shell form (`ENTRYPOINT /app/api` or
# `CMD ["sh", "-c", "/app/api"]`) -- that makes a shell PID 1, and shells
# do not forward signals to children by default, so SIGTERM would never
# reach the process. distroless/static has no shell anyway.
# If a wrapper process is ever genuinely needed (e.g. to reap zombie
# children), use tini instead of a shell:
#   ENTRYPOINT ["/sbin/tini", "--", "/app/api"]
ENTRYPOINT ["/app/api"]
