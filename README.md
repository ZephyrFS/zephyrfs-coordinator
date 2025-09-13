# ZephyrFS Coordinator

The coordination server for ZephyrFS distributed storage network, written in Go.

## Overview

The ZephyrFS Coordinator is a centralized service that manages:

- **Node Discovery & Registration**: Track active storage nodes in the network
- **File & Chunk Metadata**: Coordinate file registration and chunk placement
- **Network Health**: Monitor node health and network statistics
- **Replication Management**: Ensure proper chunk replication across nodes

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  ZephyrFS Node  │────│   Coordinator   │────│  ZephyrFS Node  │
│                 │    │                 │    │                 │
│ • Register      │    │ • Node Registry │    │ • Register      │
│ • Heartbeat     │    │ • Chunk Tracker │    │ • Heartbeat     │
│ • Report Stats  │    │ • Health Monitor│    │ • Report Stats  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───── File Storage ────┼───── File Storage ────┘
                                 │
                    ┌─────────────────┐
                    │   Web Client    │
                    │ • File Upload   │
                    │ • Download      │
                    │ • Management    │
                    └─────────────────┘
```

## Features

### Core Functionality
- **Node Management**: Registration, heartbeat processing, health tracking
- **File Coordination**: Metadata storage, chunk placement optimization
- **Network Monitoring**: Real-time statistics and health metrics
- **High Availability**: Support for multiple coordinator instances

### APIs
- **gRPC API**: High-performance binary protocol for node communication
- **REST API**: HTTP/JSON interface for web clients and management
- **Health Endpoints**: Kubernetes-compatible health checks

### Storage Options
- **BBolt**: Embedded key-value database (default)
- **PostgreSQL**: Production-ready relational database

### Monitoring
- **Prometheus Metrics**: Built-in metrics collection
- **Health Checks**: Liveness, readiness, and detailed health status
- **Performance Tracking**: Request times, error rates, resource usage

## Quick Start

### Prerequisites

- **Go 1.21+** for building from source
- **Docker** for containerized deployment
- **PostgreSQL** (optional, for production)

### Development

```bash
# Clone repository
git clone https://github.com/ZephyrFS/zephyrfs-coordinator
cd zephyrfs-coordinator

# Install dependencies
go mod download

# Run with default configuration
go run cmd/coordinator/main.go

# Or with custom config
go run cmd/coordinator/main.go -config config.yaml
```

### Docker Deployment

```bash
# Build image
docker build -t zephyrfs/coordinator .

# Run with default settings
docker run -p 8080:8080 -p 8090:8090 -p 8091:8091 zephyrfs/coordinator

# Run with custom configuration
docker run -v ./config.yaml:/config/config.yaml \
           -v ./data:/data \
           -p 8080:8080 -p 8090:8090 -p 8091:8091 \
           zephyrfs/coordinator
```

### Docker Compose

```yaml
version: '3.8'
services:
  coordinator:
    image: zephyrfs/coordinator:latest
    ports:
      - "8080:8080"   # gRPC
      - "8090:8090"   # HTTP API
      - "8091:8091"   # Metrics
    volumes:
      - ./data:/data
      - ./config.yaml:/config/config.yaml
    environment:
      - LOG_LEVEL=info
    healthcheck:
      test: ["CMD", "wget", "--spider", "http://localhost:8091/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

## Configuration

### Basic Configuration

```yaml
# config.yaml
database:
  type: "bbolt"
  path: "./coordinator.db"

grpc:
  port: 8080

http:
  enabled: true
  port: 8090

coordinator:
  replication_factor: 3
  node_timeout: "30s"
  heartbeat_interval: "10s"

health:
  metrics_enabled: true
  metrics_port: 8091
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CONFIG_PATH` | Path to configuration file | `config.yaml` |
| `LOG_LEVEL` | Logging level (debug/info/warn/error) | `info` |
| `DATA_PATH` | Data directory path | `./data` |
| `DATABASE_URL` | PostgreSQL connection URL | - |
| `GRPC_PORT` | gRPC server port | `8080` |
| `HTTP_PORT` | HTTP API server port | `8090` |
| `METRICS_PORT` | Metrics server port | `8091` |

### Production Configuration

```yaml
database:
  type: "postgres"
  url: "${DATABASE_URL}"

grpc:
  port: 8080
  max_message_size: 16777216  # 16MB

coordinator:
  replication_factor: 5
  cleanup_interval: "10m"
  node_inactive_after: "120s"

health:
  check_interval: "60s"
  metrics_enabled: true
```

## API Reference

### gRPC API

**Node Management:**
```protobuf
service CoordinatorService {
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
  rpc UnregisterNode(UnregisterNodeRequest) returns (UnregisterNodeResponse);
  rpc NodeHeartbeat(NodeHeartbeatRequest) returns (NodeHeartbeatResponse);
  rpc GetActiveNodes(GetActiveNodesRequest) returns (GetActiveNodesResponse);
}
```

**File & Chunk Management:**
```protobuf
rpc RegisterFile(RegisterFileRequest) returns (RegisterFileResponse);
rpc GetFileInfo(GetFileInfoRequest) returns (GetFileInfoResponse);
rpc FindChunkLocations(FindChunkLocationsRequest) returns (FindChunkLocationsResponse);
rpc UpdateChunkLocations(UpdateChunkLocationsRequest) returns (UpdateChunkLocationsResponse);
```

### REST API

**Node Management:**
- `POST /api/v1/nodes/register` - Register a new node
- `GET /api/v1/nodes/active` - Get active nodes
- `POST /api/v1/nodes/{id}/heartbeat` - Send heartbeat
- `POST /api/v1/nodes/{id}/unregister` - Unregister node

**File Management:**
- `POST /api/v1/files/register` - Register a file
- `GET /api/v1/files/{id}` - Get file information
- `DELETE /api/v1/files/{id}` - Delete file

**Network Status:**
- `GET /api/v1/network/status` - Get network status
- `GET /api/v1/network/stats` - Get network statistics

**Health & Monitoring:**
- `GET /health` - Health check
- `GET /ready` - Readiness check
- `GET /live` - Liveness check
- `GET /metrics` - Prometheus metrics

### Example Usage

**Register a Node (REST):**
```bash
curl -X POST http://localhost:8090/api/v1/nodes/register \
  -H "Content-Type: application/json" \
  -d '{
    "addresses": ["127.0.0.1:8080"],
    "storage_capacity": 1000000000,
    "capabilities": {"version": "1.0.0"}
  }'
```

**Get Network Status:**
```bash
curl http://localhost:8090/api/v1/network/status
```

**Health Check:**
```bash
curl http://localhost:8091/health
```

## Monitoring

### Metrics

The coordinator exposes Prometheus-compatible metrics at `/metrics`:

```
# HELP coordinator_nodes_total Total number of registered nodes
# TYPE coordinator_nodes_total gauge
coordinator_nodes_total{status="active"} 5
coordinator_nodes_total{status="inactive"} 1

# HELP coordinator_files_total Total number of registered files
# TYPE coordinator_files_total gauge
coordinator_files_total 150

# HELP coordinator_chunks_total Total number of tracked chunks
# TYPE coordinator_chunks_total gauge
coordinator_chunks_total 1500
```

### Health Checks

**Kubernetes Liveness Probe:**
```yaml
livenessProbe:
  httpGet:
    path: /live
    port: 8091
  initialDelaySeconds: 30
  periodSeconds: 10
```

**Kubernetes Readiness Probe:**
```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 8091
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Logging

Structured JSON logging with configurable levels:

```json
{
  "level": "info",
  "time": "2024-01-15T10:30:45Z",
  "msg": "Node registered",
  "nodeID": "node-123",
  "addresses": ["127.0.0.1:8080"],
  "capacity": 1000000000
}
```

## Development

### Building

```bash
# Build binary
go build -o coordinator cmd/coordinator/main.go

# Build Docker image
docker build -t zephyrfs/coordinator .

# Run tests
go test ./...

# Run with race detection
go test -race ./...

# Generate protobuf code
make proto
```

### Testing

```bash
# Unit tests
go test ./internal/...

# Integration tests
go test -tags=integration ./...

# Benchmark tests
go test -bench=. ./internal/coordinator/

# Coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Write tests for your changes
4. Run tests: `go test ./...`
5. Commit changes: `git commit -m "Add amazing feature"`
6. Push branch: `git push origin feature/amazing-feature`
7. Create Pull Request

## Deployment

### Production Checklist

- [ ] Configure PostgreSQL database
- [ ] Set up TLS certificates
- [ ] Configure monitoring and alerting
- [ ] Set resource limits and requests
- [ ] Configure backup strategy
- [ ] Set up log aggregation
- [ ] Configure service discovery
- [ ] Set up load balancing (for multiple instances)

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zephyrfs-coordinator
spec:
  replicas: 2
  selector:
    matchLabels:
      app: zephyrfs-coordinator
  template:
    metadata:
      labels:
        app: zephyrfs-coordinator
    spec:
      containers:
      - name: coordinator
        image: zephyrfs/coordinator:latest
        ports:
        - containerPort: 8080
          name: grpc
        - containerPort: 8090
          name: http
        - containerPort: 8091
          name: metrics
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: coordinator-secrets
              key: database-url
        livenessProbe:
          httpGet:
            path: /live
            port: 8091
        readinessProbe:
          httpGet:
            path: /ready
            port: 8091
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

## Troubleshooting

### Common Issues

**Database Connection Failed:**
```
Error: failed to open database: connection refused
```
- Check database configuration
- Verify database server is running
- Check network connectivity

**High Memory Usage:**
```
Warning: memory usage above 80%
```
- Monitor node count and file metadata
- Consider increasing memory limits
- Check for memory leaks in logs

**Slow Response Times:**
```
Warning: API response time > 1s
```
- Check database performance
- Monitor active connections
- Consider database indexing

### Debug Mode

Enable debug logging for troubleshooting:

```bash
./coordinator -log-level debug
```

Or set environment variable:
```bash
export LOG_LEVEL=debug
./coordinator
```

### Performance Tuning

**Database Optimization:**
- Use PostgreSQL for production workloads
- Configure appropriate connection pooling
- Add database indexes for frequently queried fields

**Resource Limits:**
- Set appropriate memory limits based on node count
- Monitor CPU usage during peak operations
- Configure garbage collection settings

## License

MIT License - see LICENSE file for details.

## Support

- **Documentation**: [ZephyrFS Docs](https://docs.zephyrfs.io)
- **Issues**: [GitHub Issues](https://github.com/ZephyrFS/zephyrfs-coordinator/issues)
- **Discussions**: [GitHub Discussions](https://github.com/ZephyrFS/zephyrfs-coordinator/discussions)
- **Security**: [security@zephyrfs.io](mailto:security@zephyrfs.io)