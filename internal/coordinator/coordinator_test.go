package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ZephyrFS/zephyrfs-coordinator/internal/config"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/database"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/models"
)

// mockDatabase implements the Database interface for testing
type mockDatabase struct {
	data map[string]map[string][]byte
}

func newMockDatabase() *mockDatabase {
	return &mockDatabase{
		data: make(map[string]map[string][]byte),
	}
}

func (m *mockDatabase) Set(bucket, key string, value []byte) error {
	if m.data[bucket] == nil {
		m.data[bucket] = make(map[string][]byte)
	}
	m.data[bucket][key] = value
	return nil
}

func (m *mockDatabase) Get(bucket, key string) ([]byte, error) {
	if bucketData, exists := m.data[bucket]; exists {
		if value, exists := bucketData[key]; exists {
			return value, nil
		}
	}
	return nil, database.ErrNotFound
}

func (m *mockDatabase) Delete(bucket, key string) error {
	if bucketData, exists := m.data[bucket]; exists {
		delete(bucketData, key)
	}
	return nil
}

func (m *mockDatabase) GetAll(bucket string) (map[string][]byte, error) {
	if bucketData, exists := m.data[bucket]; exists {
		result := make(map[string][]byte)
		for k, v := range bucketData {
			result[k] = v
		}
		return result, nil
	}
	return make(map[string][]byte), nil
}

func (m *mockDatabase) CreateBucket(bucket string) error {
	if m.data[bucket] == nil {
		m.data[bucket] = make(map[string][]byte)
	}
	return nil
}

func (m *mockDatabase) ListBuckets() ([]string, error) {
	var buckets []string
	for bucket := range m.data {
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}

func (m *mockDatabase) Close() error {
	return nil
}

func (m *mockDatabase) Stats() (*database.Stats, error) {
	return &database.Stats{}, nil
}

// Define ErrNotFound for the mock
var ErrNotFound = fmt.Errorf("not found")

// Test helper to create a coordinator for testing
func createTestCoordinator(t *testing.T) *Coordinator {
	mockDB := newMockDatabase()
	cfg := config.CoordinatorConfig{
		NodeTimeout:        30 * time.Second,
		HeartbeatInterval:  10 * time.Second,
		ReplicationFactor:  3,
		MaxNodesPerChunk:   10,
		CleanupInterval:    5 * time.Minute,
		NodeInactiveAfter:  60 * time.Second,
		GeographicSpread:   true,
	}

	coord := New(mockDB, cfg)
	return coord
}

func TestCoordinator_RegisterNode(t *testing.T) {
	coord := createTestCoordinator(t)
	defer coord.Shutdown(context.Background())

	req := &models.RegisterNodeRequest{
		NodeID:          "", // Should be auto-generated
		Addresses:       []string{"127.0.0.1:8080"},
		StorageCapacity: 1000000000, // 1GB
		Capabilities:    map[string]string{"version": "1.0.0"},
	}

	resp, err := coord.RegisterNode(context.Background(), req)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	if !resp.Success {
		t.Errorf("Expected success=true, got %v", resp.Success)
	}

	if resp.AssignedNodeID == "" {
		t.Errorf("Expected assigned node ID to be non-empty")
	}

	if len(resp.BootstrapPeers) != 0 {
		t.Errorf("Expected 0 bootstrap peers for first node, got %d", len(resp.BootstrapPeers))
	}

	// Verify node was stored
	coord.nodesMux.RLock()
	node, exists := coord.nodes[resp.AssignedNodeID]
	coord.nodesMux.RUnlock()

	if !exists {
		t.Errorf("Node was not stored in coordinator")
	}

	if node.StorageCapacity != req.StorageCapacity {
		t.Errorf("Expected storage capacity %d, got %d", req.StorageCapacity, node.StorageCapacity)
	}
}

func TestCoordinator_RegisterNodeWithExistingNodes(t *testing.T) {
	coord := createTestCoordinator(t)
	defer coord.Shutdown(context.Background())

	// Register first node
	req1 := &models.RegisterNodeRequest{
		Addresses:       []string{"127.0.0.1:8080"},
		StorageCapacity: 1000000000,
	}
	resp1, err := coord.RegisterNode(context.Background(), req1)
	if err != nil {
		t.Fatalf("First RegisterNode failed: %v", err)
	}

	// Register second node
	req2 := &models.RegisterNodeRequest{
		Addresses:       []string{"127.0.0.1:8081"},
		StorageCapacity: 2000000000,
	}
	resp2, err := coord.RegisterNode(context.Background(), req2)
	if err != nil {
		t.Fatalf("Second RegisterNode failed: %v", err)
	}

	if len(resp2.BootstrapPeers) == 0 {
		t.Errorf("Expected bootstrap peers for second node, got none")
	}

	// Bootstrap peers should include first node's address
	found := false
	for _, peer := range resp2.BootstrapPeers {
		if peer == req1.Addresses[0] {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Bootstrap peers should include first node's address")
	}
}

func TestCoordinator_NodeHeartbeat(t *testing.T) {
	coord := createTestCoordinator(t)
	defer coord.Shutdown(context.Background())

	// Register a node first
	registerReq := &models.RegisterNodeRequest{
		Addresses:       []string{"127.0.0.1:8080"},
		StorageCapacity: 1000000000,
	}
	registerResp, err := coord.RegisterNode(context.Background(), registerReq)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	nodeID := registerResp.AssignedNodeID

	// Send heartbeat
	heartbeatReq := &models.NodeHeartbeatRequest{
		NodeID: nodeID,
		Stats: &models.NodeStats{
			StorageUsed:      500000000,
			StorageAvailable: 500000000,
			ChunksStored:     100,
			CpuUsage:         25.5,
			MemoryUsage:      60.0,
			UptimeSeconds:    3600,
		},
	}

	heartbeatResp, err := coord.NodeHeartbeat(context.Background(), heartbeatReq)
	if err != nil {
		t.Fatalf("NodeHeartbeat failed: %v", err)
	}

	if !heartbeatResp.Success {
		t.Errorf("Expected heartbeat success=true, got %v", heartbeatResp.Success)
	}

	// Verify stats were updated
	coord.nodesMux.RLock()
	node, exists := coord.nodes[nodeID]
	coord.nodesMux.RUnlock()

	if !exists {
		t.Fatalf("Node not found after heartbeat")
	}

	if node.Stats.StorageUsed != heartbeatReq.Stats.StorageUsed {
		t.Errorf("Expected storage used %d, got %d", heartbeatReq.Stats.StorageUsed, node.Stats.StorageUsed)
	}

	if node.Status != "active" {
		t.Errorf("Expected node status to be 'active', got '%s'", node.Status)
	}
}

func TestCoordinator_RegisterFile(t *testing.T) {
	coord := createTestCoordinator(t)
	defer coord.Shutdown(context.Background())

	// Register some nodes first
	for i := 0; i < 5; i++ {
		registerReq := &models.RegisterNodeRequest{
			Addresses:       []string{fmt.Sprintf("127.0.0.1:808%d", i)},
			StorageCapacity: 1000000000,
		}
		_, err := coord.RegisterNode(context.Background(), registerReq)
		if err != nil {
			t.Fatalf("RegisterNode %d failed: %v", i, err)
		}
	}

	// Register a file
	fileReq := &models.RegisterFileRequest{
		FileID:   "test-file-123",
		FileName: "test.txt",
		FileSize: 1048576, // 1MB
		FileHash: "abcd1234",
		Chunks: []*models.ChunkMetadata{
			{
				ChunkID: "chunk-1",
				Hash:    "hash1",
				Size:    524288, // 512KB
				Index:   0,
			},
			{
				ChunkID: "chunk-2",
				Hash:    "hash2",
				Size:    524288, // 512KB
				Index:   1,
			},
		},
		OwnerNodeID: "owner-node-123",
	}

	fileResp, err := coord.RegisterFile(context.Background(), fileReq)
	if err != nil {
		t.Fatalf("RegisterFile failed: %v", err)
	}

	if !fileResp.Success {
		t.Errorf("Expected file registration success=true, got %v", fileResp.Success)
	}

	if len(fileResp.ChunkPlacements) != len(fileReq.Chunks) {
		t.Errorf("Expected %d chunk placements, got %d", len(fileReq.Chunks), len(fileResp.ChunkPlacements))
	}

	// Verify each chunk has appropriate replication
	for _, placement := range fileResp.ChunkPlacements {
		if len(placement.TargetNodes) < coord.config.ReplicationFactor {
			t.Errorf("Chunk %s has insufficient replication: %d < %d",
				placement.ChunkID, len(placement.TargetNodes), coord.config.ReplicationFactor)
		}
	}

	// Verify file was stored
	coord.filesMux.RLock()
	file, exists := coord.files[fileReq.FileID]
	coord.filesMux.RUnlock()

	if !exists {
		t.Errorf("File was not stored in coordinator")
	}

	if file.FileName != fileReq.FileName {
		t.Errorf("Expected file name '%s', got '%s'", fileReq.FileName, file.FileName)
	}
}

func TestCoordinator_FindChunkLocations(t *testing.T) {
	coord := createTestCoordinator(t)
	defer coord.Shutdown(context.Background())

	// Register nodes and a file first
	nodeIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		registerReq := &models.RegisterNodeRequest{
			Addresses:       []string{fmt.Sprintf("127.0.0.1:808%d", i)},
			StorageCapacity: 1000000000,
		}
		resp, err := coord.RegisterNode(context.Background(), registerReq)
		if err != nil {
			t.Fatalf("RegisterNode %d failed: %v", i, err)
		}
		nodeIDs[i] = resp.AssignedNodeID
	}

	// Register a file
	fileReq := &models.RegisterFileRequest{
		FileID:   "test-file-123",
		FileName: "test.txt",
		FileSize: 524288,
		FileHash: "abcd1234",
		Chunks: []*models.ChunkMetadata{
			{
				ChunkID: "chunk-1",
				Hash:    "hash1",
				Size:    524288,
				Index:   0,
			},
		},
		OwnerNodeID: nodeIDs[0],
	}

	_, err := coord.RegisterFile(context.Background(), fileReq)
	if err != nil {
		t.Fatalf("RegisterFile failed: %v", err)
	}

	// Find chunk locations
	findReq := &models.FindChunkLocationsRequest{
		ChunkID:        "chunk-1",
		PreferredCount: 2,
	}

	findResp, err := coord.FindChunkLocations(context.Background(), findReq)
	if err != nil {
		t.Fatalf("FindChunkLocations failed: %v", err)
	}

	if !findResp.Success {
		t.Errorf("Expected find success=true, got %v", findResp.Success)
	}

	if len(findResp.NodeIDs) == 0 {
		t.Errorf("Expected to find chunk locations, got none")
	}

	// Should respect preferred count
	if len(findResp.NodeIDs) > int(findReq.PreferredCount) {
		t.Errorf("Expected at most %d locations, got %d", findReq.PreferredCount, len(findResp.NodeIDs))
	}

	// Should have corresponding addresses
	if len(findResp.NodeAddresses) != len(findResp.NodeIDs) {
		t.Errorf("Mismatch between node IDs (%d) and addresses (%d)",
			len(findResp.NodeIDs), len(findResp.NodeAddresses))
	}
}

func TestCoordinator_GetActiveNodes(t *testing.T) {
	coord := createTestCoordinator(t)
	defer coord.Shutdown(context.Background())

	// Register some nodes
	nodeIDs := make([]string, 5)
	for i := 0; i < 5; i++ {
		registerReq := &models.RegisterNodeRequest{
			Addresses:       []string{fmt.Sprintf("127.0.0.1:808%d", i)},
			StorageCapacity: 1000000000,
		}
		resp, err := coord.RegisterNode(context.Background(), registerReq)
		if err != nil {
			t.Fatalf("RegisterNode %d failed: %v", i, err)
		}
		nodeIDs[i] = resp.AssignedNodeID
	}

	// Get active nodes
	getReq := &models.GetActiveNodesRequest{
		Limit:        3,
		ExcludeNodes: []string{nodeIDs[0]}, // Exclude first node
	}

	getResp, err := coord.GetActiveNodes(context.Background(), getReq)
	if err != nil {
		t.Fatalf("GetActiveNodes failed: %v", err)
	}

	if len(getResp.Nodes) > int(getReq.Limit) {
		t.Errorf("Expected at most %d nodes, got %d", getReq.Limit, len(getResp.Nodes))
	}

	// Should not include excluded node
	for _, node := range getResp.Nodes {
		if node.NodeID == nodeIDs[0] {
			t.Errorf("Excluded node %s was included in results", nodeIDs[0])
		}
	}

	if getResp.TotalNodes == 0 {
		t.Errorf("Expected total nodes > 0, got %d", getResp.TotalNodes)
	}
}

func TestCoordinator_GetNetworkStatus(t *testing.T) {
	coord := createTestCoordinator(t)
	defer coord.Shutdown(context.Background())

	// Register some nodes
	for i := 0; i < 3; i++ {
		registerReq := &models.RegisterNodeRequest{
			Addresses:       []string{fmt.Sprintf("127.0.0.1:808%d", i)},
			StorageCapacity: 1000000000,
		}
		_, err := coord.RegisterNode(context.Background(), registerReq)
		if err != nil {
			t.Fatalf("RegisterNode %d failed: %v", i, err)
		}
	}

	statusResp, err := coord.GetNetworkStatus(context.Background())
	if err != nil {
		t.Fatalf("GetNetworkStatus failed: %v", err)
	}

	if statusResp.NetworkStats.TotalNodes != 3 {
		t.Errorf("Expected 3 total nodes, got %d", statusResp.NetworkStats.TotalNodes)
	}

	if statusResp.NetworkStats.ActiveNodes != 3 {
		t.Errorf("Expected 3 active nodes, got %d", statusResp.NetworkStats.ActiveNodes)
	}

	if len(statusResp.ActiveNodes) != 3 {
		t.Errorf("Expected 3 active nodes in list, got %d", len(statusResp.ActiveNodes))
	}

	if statusResp.Timestamp == 0 {
		t.Errorf("Expected non-zero timestamp")
	}
}

// Benchmark tests

func BenchmarkCoordinator_RegisterNode(b *testing.B) {
	coord := createTestCoordinator(b)
	defer coord.Shutdown(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := &models.RegisterNodeRequest{
			Addresses:       []string{fmt.Sprintf("127.0.0.1:808%d", i)},
			StorageCapacity: 1000000000,
		}
		_, err := coord.RegisterNode(context.Background(), req)
		if err != nil {
			b.Fatalf("RegisterNode failed: %v", err)
		}
	}
}

func BenchmarkCoordinator_NodeHeartbeat(b *testing.B) {
	coord := createTestCoordinator(b)
	defer coord.Shutdown(context.Background())

	// Register a node first
	registerReq := &models.RegisterNodeRequest{
		Addresses:       []string{"127.0.0.1:8080"},
		StorageCapacity: 1000000000,
	}
	registerResp, _ := coord.RegisterNode(context.Background(), registerReq)
	nodeID := registerResp.AssignedNodeID

	heartbeatReq := &models.NodeHeartbeatRequest{
		NodeID: nodeID,
		Stats: &models.NodeStats{
			StorageUsed:   500000000,
			UptimeSeconds: 3600,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := coord.NodeHeartbeat(context.Background(), heartbeatReq)
		if err != nil {
			b.Fatalf("NodeHeartbeat failed: %v", err)
		}
	}
}