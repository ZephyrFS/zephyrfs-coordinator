package models

import "time"

// Node-related models

// NodeInfo represents information about a registered node
type NodeInfo struct {
	NodeID          string            `json:"node_id"`
	Addresses       []string          `json:"addresses"`
	StorageCapacity int64             `json:"storage_capacity"`
	Capabilities    map[string]string `json:"capabilities"`
	Status          string            `json:"status"` // "active", "inactive", "maintenance"
	RegisteredAt    time.Time         `json:"registered_at"`
	LastHeartbeat   time.Time         `json:"last_heartbeat"`
	Stats           *NodeStats        `json:"stats,omitempty"`
}

// NodeStats represents runtime statistics for a node
type NodeStats struct {
	StorageUsed     int64   `json:"storage_used"`
	StorageAvailable int64  `json:"storage_available"`
	ChunksStored    int64   `json:"chunks_stored"`
	BandwidthUp     int64   `json:"bandwidth_up"`
	BandwidthDown   int64   `json:"bandwidth_down"`
	CpuUsage        float64 `json:"cpu_usage"`
	MemoryUsage     float64 `json:"memory_usage"`
	UptimeSeconds   int64   `json:"uptime_seconds"`
}

// NodeStatus represents the status of a node for API responses
type NodeStatus struct {
	NodeID        string     `json:"node_id"`
	Addresses     []string   `json:"addresses"`
	Stats         *NodeStats `json:"stats"`
	LastHeartbeat int64      `json:"last_heartbeat"`
	Status        string     `json:"status"`
}

// File and chunk models

// FileRecord represents metadata about a stored file
type FileRecord struct {
	FileID       string         `json:"file_id"`
	FileName     string         `json:"file_name"`
	FileSize     int64          `json:"file_size"`
	FileHash     string         `json:"file_hash"`
	Chunks       []*ChunkRecord `json:"chunks"`
	OwnerNodeID  string         `json:"owner_node_id"`
	CreatedAt    int64          `json:"created_at"`
	LastAccessed int64          `json:"last_accessed"`
}

// ChunkRecord represents metadata about a file chunk
type ChunkRecord struct {
	ChunkID          string   `json:"chunk_id"`
	Hash             string   `json:"hash"`
	Size             int64    `json:"size"`
	Index            int32    `json:"index"`
	StoredAtNodes    []string `json:"stored_at_nodes"`
	ReplicationCount int32    `json:"replication_count"`
}

// ChunkInfo represents detailed information about a chunk
type ChunkInfo struct {
	ChunkID       string   `json:"chunk_id"`
	Hash          string   `json:"hash"`
	Size          int64    `json:"size"`
	Index         int32    `json:"index"`
	FileID        string   `json:"file_id"`
	StoredAtNodes []string `json:"stored_at_nodes"`
	CreatedAt     int64    `json:"created_at"`
}

// Request/Response models for gRPC API

// RegisterNodeRequest represents a node registration request
type RegisterNodeRequest struct {
	NodeID          string            `json:"node_id"`
	Addresses       []string          `json:"addresses"`
	StorageCapacity int64             `json:"storage_capacity"`
	Capabilities    map[string]string `json:"capabilities"`
}

// RegisterNodeResponse represents a node registration response
type RegisterNodeResponse struct {
	Success        bool     `json:"success"`
	Message        string   `json:"message"`
	AssignedNodeID string   `json:"assigned_node_id"`
	BootstrapPeers []string `json:"bootstrap_peers"`
}

// UnregisterNodeRequest represents a node unregistration request
type UnregisterNodeRequest struct {
	NodeID string `json:"node_id"`
	Reason string `json:"reason"`
}

// UnregisterNodeResponse represents a node unregistration response
type UnregisterNodeResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// GetActiveNodesRequest represents a request for active nodes
type GetActiveNodesRequest struct {
	Limit        int32    `json:"limit"`
	ExcludeNodes []string `json:"exclude_nodes"`
}

// GetActiveNodesResponse represents a response with active nodes
type GetActiveNodesResponse struct {
	Nodes      []*NodeStatus `json:"nodes"`
	TotalNodes int32         `json:"total_nodes"`
}

// NodeHeartbeatRequest represents a node heartbeat
type NodeHeartbeatRequest struct {
	NodeID string     `json:"node_id"`
	Stats  *NodeStats `json:"stats"`
}

// NodeHeartbeatResponse represents a heartbeat response
type NodeHeartbeatResponse struct {
	Success bool     `json:"success"`
	Message string   `json:"message"`
	Tasks   []string `json:"tasks"`
}

// RegisterFileRequest represents a file registration request
type RegisterFileRequest struct {
	FileID      string          `json:"file_id"`
	FileName    string          `json:"file_name"`
	FileSize    int64           `json:"file_size"`
	FileHash    string          `json:"file_hash"`
	Chunks      []*ChunkMetadata `json:"chunks"`
	OwnerNodeID string          `json:"owner_node_id"`
}

// RegisterFileResponse represents a file registration response
type RegisterFileResponse struct {
	Success         bool               `json:"success"`
	Message         string             `json:"message"`
	ChunkPlacements []*ChunkPlacement  `json:"chunk_placements"`
}

// ChunkMetadata represents metadata about a chunk during file registration
type ChunkMetadata struct {
	ChunkID string `json:"chunk_id"`
	Hash    string `json:"hash"`
	Size    int64  `json:"size"`
	Index   int32  `json:"index"`
}

// ChunkPlacement represents where chunks should be stored
type ChunkPlacement struct {
	ChunkID           string   `json:"chunk_id"`
	TargetNodes       []string `json:"target_nodes"`
	ReplicationFactor int32    `json:"replication_factor"`
}

// GetFileInfoRequest represents a file info request
type GetFileInfoRequest struct {
	FileID string `json:"file_id"`
}

// GetFileInfoResponse represents a file info response
type GetFileInfoResponse struct {
	Success  bool        `json:"success"`
	Message  string      `json:"message"`
	FileInfo *FileRecord `json:"file_info"`
}

// UpdateChunkLocationsRequest represents a chunk location update
type UpdateChunkLocationsRequest struct {
	ChunkID   string   `json:"chunk_id"`
	NodeIDs   []string `json:"node_ids"`
	Operation string   `json:"operation"` // "add" or "remove"
}

// UpdateChunkLocationsResponse represents a chunk location update response
type UpdateChunkLocationsResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// FindChunkLocationsRequest represents a chunk location query
type FindChunkLocationsRequest struct {
	ChunkID        string `json:"chunk_id"`
	PreferredCount int32  `json:"preferred_count"`
}

// FindChunkLocationsResponse represents a chunk location response
type FindChunkLocationsResponse struct {
	Success       bool     `json:"success"`
	Message       string   `json:"message"`
	NodeIDs       []string `json:"node_ids"`
	NodeAddresses []string `json:"node_addresses"`
}

// GetNetworkStatusRequest represents a network status request
type GetNetworkStatusRequest struct{}

// GetNetworkStatusResponse represents a network status response
type GetNetworkStatusResponse struct {
	NetworkStats *NetworkStats   `json:"network_stats"`
	ActiveNodes  []*NodeStatus   `json:"active_nodes"`
	Timestamp    int64           `json:"timestamp"`
}

// NetworkStats represents network-wide statistics
type NetworkStats struct {
	TotalNodes             int32   `json:"total_nodes"`
	ActiveNodes            int32   `json:"active_nodes"`
	TotalStorageCapacity   int64   `json:"total_storage_capacity"`
	TotalStorageUsed       int64   `json:"total_storage_used"`
	TotalFiles             int64   `json:"total_files"`
	TotalChunks            int64   `json:"total_chunks"`
	AverageNodeUptime      float64 `json:"average_node_uptime"`
	NetworkUptimeSeconds   int64   `json:"network_uptime_seconds"`
	Timestamp              int64   `json:"timestamp"`
}