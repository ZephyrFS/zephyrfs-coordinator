# Multi-stage build for ZephyrFS Coordinator
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Build the application with optimizations
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -a -installsuffix cgo \
    -ldflags='-w -s -extldflags "-static"' \
    -o coordinator cmd/coordinator/main.go

# Runtime stage
FROM alpine:3.18

# Install runtime dependencies
RUN apk --no-cache add \
    ca-certificates \
    tzdata \
    wget \
    && update-ca-certificates

# Create non-root user for security
RUN addgroup -g 1000 zephyrfs && \
    adduser -D -s /bin/sh -u 1000 -G zephyrfs zephyrfs

# Create necessary directories
RUN mkdir -p /data /config /logs && \
    chown -R zephyrfs:zephyrfs /data /config /logs

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder --chown=zephyrfs:zephyrfs /app/coordinator .

# Create default configuration
RUN echo 'database:\n  type: "bbolt"\n  path: "/data/coordinator.db"\ngrpc:\n  port: 8080\nhttp:\n  enabled: true\n  port: 8090\nhealth:\n  metrics_enabled: true\n  metrics_port: 8091' > /config/config.yaml && \
    chown zephyrfs:zephyrfs /config/config.yaml

# Switch to non-root user
USER zephyrfs

# Expose ports
EXPOSE 8080 8090 8091

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8091/health || exit 1

# Set default environment variables
ENV CONFIG_PATH=/config/config.yaml
ENV DATA_PATH=/data
ENV LOG_LEVEL=info

# Run the coordinator
ENTRYPOINT ["./coordinator"]
CMD ["-config", "/config/config.yaml", "-log-level", "info"]