package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"

	"github.com/ZephyrFS/zephyrfs-coordinator/internal/config"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/database"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/models"
)

const (
	nodesBucket  = "nodes"
	filesBucket  = "files"
	chunksBucket = "chunks"
)

// Coordinator manages the ZephyrFS network
type Coordinator struct {
	db     database.Database
	config config.CoordinatorConfig

	// In-memory caches for performance
	nodes     map[string]*models.NodeInfo
	nodesMux  sync.RWMutex
	files     map[string]*models.FileRecord
	filesMux  sync.RWMutex
	chunks    map[string]*models.ChunkInfo
	chunksMux sync.RWMutex

	// Background tasks
	stopChan chan struct{}
	wg       sync.WaitGroup

	// Statistics
	stats *models.NetworkStats
}

// New creates a new Coordinator instance
func New(db database.Database, cfg config.CoordinatorConfig) *Coordinator {
	coord := &Coordinator{
		db:       db,
		config:   cfg,
		nodes:    make(map[string]*models.NodeInfo),
		files:    make(map[string]*models.FileRecord),
		chunks:   make(map[string]*models.ChunkInfo),
		stopChan: make(chan struct{}),
		stats:    &models.NetworkStats{},
	}

	// Load data from database
	if err := coord.loadFromDatabase(); err != nil {
		logrus.WithError(err).Error("Failed to load data from database")
	}

	// Start background tasks
	coord.startBackgroundTasks()

	return coord
}

// RegisterNode registers a new node in the network
func (c *Coordinator) RegisterNode(ctx context.Context, req *models.RegisterNodeRequest) (*models.RegisterNodeResponse, error) {
	c.nodesMux.Lock()
	defer c.nodesMux.Unlock()

	nodeID := req.NodeID
	if nodeID == "" {
		nodeID = generateNodeID()
	}

	node := &models.NodeInfo{
		NodeID:          nodeID,
		Addresses:       req.Addresses,
		StorageCapacity: req.StorageCapacity,
		Capabilities:    req.Capabilities,
		Status:          "active",
		RegisteredAt:    time.Now(),
		LastHeartbeat:   time.Now(),
		Stats:           &models.NodeStats{},
	}

	// Store in memory and database
	c.nodes[nodeID] = node
	if err := c.saveNode(node); err != nil {
		logrus.WithError(err).Error("Failed to save node to database")
		delete(c.nodes, nodeID)
		return nil, fmt.Errorf("failed to register node: %w", err)
	}

	// Get bootstrap peers
	bootstrapPeers := c.getBootstrapPeers(nodeID, 5)

	logrus.WithFields(logrus.Fields{
		"nodeID":         nodeID,
		"addresses":      req.Addresses,
		"capacity":       req.StorageCapacity,
		"bootstrapPeers": len(bootstrapPeers),
	}).Info("Node registered")

	return &models.RegisterNodeResponse{
		Success:        true,
		Message:        "Node registered successfully",
		AssignedNodeID: nodeID,
		BootstrapPeers: bootstrapPeers,
	}, nil
}

// UnregisterNode removes a node from the network
func (c *Coordinator) UnregisterNode(ctx context.Context, req *models.UnregisterNodeRequest) (*models.UnregisterNodeResponse, error) {
	c.nodesMux.Lock()
	defer c.nodesMux.Unlock()

	node, exists := c.nodes[req.NodeID]
	if !exists {
		return &models.UnregisterNodeResponse{
			Success: false,
			Message: "Node not found",
		}, nil
	}

	// Mark as inactive and trigger chunk replication
	node.Status = "inactive"
	node.LastHeartbeat = time.Now()

	if err := c.saveNode(node); err != nil {
		logrus.WithError(err).Error("Failed to update node status")
	}

	// Schedule chunk redistribution
	go c.redistributeChunksFromNode(req.NodeID)

	logrus.WithFields(logrus.Fields{
		"nodeID": req.NodeID,
		"reason": req.Reason,
	}).Info("Node unregistered")

	return &models.UnregisterNodeResponse{
		Success: true,
		Message: "Node unregistered successfully",
	}, nil
}

// NodeHeartbeat processes heartbeat from a node
func (c *Coordinator) NodeHeartbeat(ctx context.Context, req *models.NodeHeartbeatRequest) (*models.NodeHeartbeatResponse, error) {
	c.nodesMux.Lock()
	defer c.nodesMux.Unlock()

	node, exists := c.nodes[req.NodeID]
	if !exists {
		return &models.NodeHeartbeatResponse{
			Success: false,
			Message: "Node not registered",
		}, nil
	}

	// Update node stats and heartbeat
	node.LastHeartbeat = time.Now()
	node.Status = "active"
	if req.Stats != nil {
		node.Stats = req.Stats
	}

	if err := c.saveNode(node); err != nil {
		logrus.WithError(err).Error("Failed to save node heartbeat")
	}

	// Generate tasks for the node
	tasks := c.generateTasksForNode(req.NodeID)

	return &models.NodeHeartbeatResponse{
		Success: true,
		Message: "Heartbeat processed",
		Tasks:   tasks,
	}, nil
}

// GetActiveNodes returns a list of active nodes
func (c *Coordinator) GetActiveNodes(ctx context.Context, req *models.GetActiveNodesRequest) (*models.GetActiveNodesResponse, error) {
	c.nodesMux.RLock()
	defer c.nodesMux.RUnlock()

	var activeNodes []*models.NodeStatus
	excludeSet := make(map[string]bool)
	for _, nodeID := range req.ExcludeNodes {
		excludeSet[nodeID] = true
	}

	for _, node := range c.nodes {
		if node.Status == "active" && !excludeSet[node.NodeID] {
			if time.Since(node.LastHeartbeat) < c.config.NodeInactiveAfter {
				activeNodes = append(activeNodes, &models.NodeStatus{
					NodeID:        node.NodeID,
					Addresses:     node.Addresses,
					Stats:         node.Stats,
					LastHeartbeat: node.LastHeartbeat.Unix(),
					Status:        node.Status,
				})
			}
		}
	}

	// Sort by reliability/reputation if available
	sort.Slice(activeNodes, func(i, j int) bool {
		return activeNodes[i].Stats.UptimeSeconds > activeNodes[j].Stats.UptimeSeconds
	})

	// Apply limit
	if req.Limit > 0 && len(activeNodes) > int(req.Limit) {
		activeNodes = activeNodes[:req.Limit]
	}

	return &models.GetActiveNodesResponse{
		Nodes:      activeNodes,
		TotalNodes: int32(len(activeNodes)),
	}, nil
}

// RegisterFile registers file metadata and determines chunk placement
func (c *Coordinator) RegisterFile(ctx context.Context, req *models.RegisterFileRequest) (*models.RegisterFileResponse, error) {
	c.filesMux.Lock()
	c.chunksMux.Lock()
	defer c.filesMux.Unlock()
	defer c.chunksMux.Unlock()

	// Create file record
	file := &models.FileRecord{
		FileID:       req.FileID,
		FileName:     req.FileName,
		FileSize:     req.FileSize,
		FileHash:     req.FileHash,
		OwnerNodeID:  req.OwnerNodeID,
		CreatedAt:    time.Now().Unix(),
		LastAccessed: time.Now().Unix(),
	}

	// Determine chunk placements
	var chunkPlacements []*models.ChunkPlacement
	for _, chunkMeta := range req.Chunks {
		targetNodes := c.selectNodesForChunk(chunkMeta.ChunkID, c.config.ReplicationFactor)

		placement := &models.ChunkPlacement{
			ChunkID:           chunkMeta.ChunkID,
			TargetNodes:       targetNodes,
			ReplicationFactor: int32(c.config.ReplicationFactor),
		}
		chunkPlacements = append(chunkPlacements, placement)

		// Create chunk record
		chunk := &models.ChunkInfo{
			ChunkID:       chunkMeta.ChunkID,
			Hash:          chunkMeta.Hash,
			Size:          chunkMeta.Size,
			Index:         chunkMeta.Index,
			FileID:        req.FileID,
			StoredAtNodes: targetNodes,
			CreatedAt:     time.Now().Unix(),
		}
		c.chunks[chunkMeta.ChunkID] = chunk
		file.Chunks = append(file.Chunks, &models.ChunkRecord{
			ChunkID:           chunkMeta.ChunkID,
			Hash:              chunkMeta.Hash,
			Size:              chunkMeta.Size,
			Index:             chunkMeta.Index,
			StoredAtNodes:     targetNodes,
			ReplicationCount:  int32(len(targetNodes)),
		})

		if err := c.saveChunk(chunk); err != nil {
			logrus.WithError(err).Error("Failed to save chunk metadata")
		}
	}

	// Save file record
	c.files[req.FileID] = file
	if err := c.saveFile(file); err != nil {
		logrus.WithError(err).Error("Failed to save file metadata")
		return nil, fmt.Errorf("failed to register file: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"fileID":   req.FileID,
		"fileName": req.FileName,
		"fileSize": req.FileSize,
		"chunks":   len(req.Chunks),
	}).Info("File registered")

	return &models.RegisterFileResponse{
		Success:         true,
		Message:         "File registered successfully",
		ChunkPlacements: chunkPlacements,
	}, nil
}

// FindChunkLocations finds nodes storing a specific chunk
func (c *Coordinator) FindChunkLocations(ctx context.Context, req *models.FindChunkLocationsRequest) (*models.FindChunkLocationsResponse, error) {
	c.chunksMux.RLock()
	defer c.chunksMux.RUnlock()

	chunk, exists := c.chunks[req.ChunkID]
	if !exists {
		return &models.FindChunkLocationsResponse{
			Success: false,
			Message: "Chunk not found",
		}, nil
	}

	// Filter out inactive nodes
	var activeNodes []string
	var activeAddresses []string

	c.nodesMux.RLock()
	for _, nodeID := range chunk.StoredAtNodes {
		if node, exists := c.nodes[nodeID]; exists {
			if node.Status == "active" && time.Since(node.LastHeartbeat) < c.config.NodeInactiveAfter {
				activeNodes = append(activeNodes, nodeID)
				activeAddresses = append(activeAddresses, node.Addresses[0]) // Use first address
			}
		}
	}
	c.nodesMux.RUnlock()

	// Apply preferred count
	if req.PreferredCount > 0 && len(activeNodes) > int(req.PreferredCount) {
		// Randomly select preferred count
		rand.Shuffle(len(activeNodes), func(i, j int) {
			activeNodes[i], activeNodes[j] = activeNodes[j], activeNodes[i]
			activeAddresses[i], activeAddresses[j] = activeAddresses[j], activeAddresses[i]
		})
		activeNodes = activeNodes[:req.PreferredCount]
		activeAddresses = activeAddresses[:req.PreferredCount]
	}

	return &models.FindChunkLocationsResponse{
		Success:       true,
		Message:       "Chunk locations found",
		NodeIDs:       activeNodes,
		NodeAddresses: activeAddresses,
	}, nil
}

// GetFileInfo retrieves information about a specific file
func (c *Coordinator) GetFileInfo(ctx context.Context, req *models.GetFileInfoRequest) (*models.GetFileInfoResponse, error) {
	c.filesMux.RLock()
	defer c.filesMux.RUnlock()

	file, exists := c.files[req.FileID]
	if !exists {
		return &models.GetFileInfoResponse{
			Success: false,
			Message: "File not found",
		}, nil
	}

	return &models.GetFileInfoResponse{
		Success:  true,
		Message:  "File info retrieved",
		FileInfo: file,
	}, nil
}

// UpdateChunkLocations updates where chunks are stored
func (c *Coordinator) UpdateChunkLocations(ctx context.Context, req *models.UpdateChunkLocationsRequest) (*models.UpdateChunkLocationsResponse, error) {
	c.chunksMux.Lock()
	defer c.chunksMux.Unlock()

	chunk, exists := c.chunks[req.ChunkID]
	if !exists {
		return &models.UpdateChunkLocationsResponse{
			Success: false,
			Message: "Chunk not found",
		}, nil
	}

	switch req.Operation {
	case "add":
		// Add nodes to the chunk's storage locations
		for _, nodeID := range req.NodeIDs {
			// Check if node is already in the list
			found := false
			for _, existingNodeID := range chunk.StoredAtNodes {
				if existingNodeID == nodeID {
					found = true
					break
				}
			}
			if !found {
				chunk.StoredAtNodes = append(chunk.StoredAtNodes, nodeID)
			}
		}
	case "remove":
		// Remove nodes from the chunk's storage locations
		var newStoredNodes []string
		for _, existingNodeID := range chunk.StoredAtNodes {
			shouldRemove := false
			for _, nodeID := range req.NodeIDs {
				if existingNodeID == nodeID {
					shouldRemove = true
					break
				}
			}
			if !shouldRemove {
				newStoredNodes = append(newStoredNodes, existingNodeID)
			}
		}
		chunk.StoredAtNodes = newStoredNodes
	default:
		return &models.UpdateChunkLocationsResponse{
			Success: false,
			Message: "Invalid operation. Must be 'add' or 'remove'",
		}, nil
	}

	// Save updated chunk
	if err := c.saveChunk(chunk); err != nil {
		logrus.WithError(err).Error("Failed to save updated chunk")
		return &models.UpdateChunkLocationsResponse{
			Success: false,
			Message: "Failed to update chunk locations",
		}, nil
	}

	return &models.UpdateChunkLocationsResponse{
		Success: true,
		Message: "Chunk locations updated successfully",
	}, nil
}

// GetNetworkStatus returns current network statistics
func (c *Coordinator) GetNetworkStatus(ctx context.Context) (*models.GetNetworkStatusResponse, error) {
	c.nodesMux.RLock()
	c.filesMux.RLock()
	c.chunksMux.RLock()
	defer c.nodesMux.RUnlock()
	defer c.filesMux.RUnlock()
	defer c.chunksMux.RUnlock()

	stats := &models.NetworkStats{
		TotalNodes:            int32(len(c.nodes)),
		TotalFiles:            int64(len(c.files)),
		TotalChunks:           int64(len(c.chunks)),
		NetworkUptimeSeconds:  int64(time.Since(time.Now().Add(-24 * time.Hour)).Seconds()), // Placeholder
		Timestamp:             time.Now().Unix(),
	}

	var activeNodes []*models.NodeStatus
	var totalCapacity, totalUsed int64
	var uptimeSum float64
	activeCount := 0

	for _, node := range c.nodes {
		if node.Status == "active" && time.Since(node.LastHeartbeat) < c.config.NodeInactiveAfter {
			activeCount++
			totalCapacity += node.StorageCapacity
			if node.Stats != nil {
				totalUsed += node.Stats.StorageUsed
				uptimeSum += float64(node.Stats.UptimeSeconds)
			}

			activeNodes = append(activeNodes, &models.NodeStatus{
				NodeID:        node.NodeID,
				Addresses:     node.Addresses,
				Stats:         node.Stats,
				LastHeartbeat: node.LastHeartbeat.Unix(),
				Status:        node.Status,
			})
		}
	}

	stats.ActiveNodes = int32(activeCount)
	stats.TotalStorageCapacity = totalCapacity
	stats.TotalStorageUsed = totalUsed
	if activeCount > 0 {
		stats.AverageNodeUptime = uptimeSum / float64(activeCount)
	}

	return &models.GetNetworkStatusResponse{
		NetworkStats: stats,
		ActiveNodes:  activeNodes,
		Timestamp:    time.Now().Unix(),
	}, nil
}

// Shutdown gracefully shuts down the coordinator
func (c *Coordinator) Shutdown(ctx context.Context) {
	logrus.Info("Shutting down coordinator...")
	close(c.stopChan)
	c.wg.Wait()
	logrus.Info("Coordinator shutdown complete")
}

// Private helper methods continue in next file...