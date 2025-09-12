# Multi-stage build for ZephyrFS Coordinator
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o coordinator ./cmd/coordinator/

# Final runtime image
FROM alpine:3.19

# Install ca-certificates for TLS
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Create non-root user
RUN addgroup -g 1000 zephyr && adduser -D -s /bin/sh -u 1000 -G zephyr zephyr

# Create data directory
RUN mkdir -p /var/lib/zephyrfs && chown zephyr:zephyr /var/lib/zephyrfs

# Copy binary from builder stage
COPY --from=builder /app/coordinator .
COPY --from=builder /app/configs/config.yaml ./config.yaml

USER zephyr

# Expose coordinator API port
EXPOSE 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ./coordinator --health-check || exit 1

ENTRYPOINT ["./coordinator"]