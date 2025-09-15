package coordinator

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ZephyrFS/zephyrfs-coordinator/internal/models"
)

// generateNodeID creates a unique node identifier
func generateNodeID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// loadFromDatabase loads all data from persistent storage
func (c *Coordinator) loadFromDatabase() error {
	// Load nodes
	nodes, err := c.db.GetAll(nodesBucket)
	if err != nil {
		return fmt.Errorf("failed to load nodes: %w", err)
	}

	for key, data := range nodes {
		var node models.NodeInfo
		if err := json.Unmarshal(data, &node); err != nil {
			logrus.WithError(err).WithField("nodeID", key).Warn("Failed to unmarshal node data")
			continue
		}
		c.nodes[key] = &node
	}

	// Load files
	files, err := c.db.GetAll(filesBucket)
	if err != nil {
		return fmt.Errorf("failed to load files: %w", err)
	}

	for key, data := range files {
		var file models.FileRecord
		if err := json.Unmarshal(data, &file); err != nil {
			logrus.WithError(err).WithField("fileID", key).Warn("Failed to unmarshal file data")
			continue
		}
		c.files[key] = &file
	}

	// Load chunks
	chunks, err := c.db.GetAll(chunksBucket)
	if err != nil {
		return fmt.Errorf("failed to load chunks: %w", err)
	}

	for key, data := range chunks {
		var chunk models.ChunkInfo
		if err := json.Unmarshal(data, &chunk); err != nil {
			logrus.WithError(err).WithField("chunkID", key).Warn("Failed to unmarshal chunk data")
			continue
		}
		c.chunks[key] = &chunk
	}

	logrus.WithFields(logrus.Fields{
		"nodes":  len(c.nodes),
		"files":  len(c.files),
		"chunks": len(c.chunks),
	}).Info("Loaded data from database")

	return nil
}

// saveNode saves a node to the database
func (c *Coordinator) saveNode(node *models.NodeInfo) error {
	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}
	return c.db.Set(nodesBucket, node.NodeID, data)
}

// saveFile saves a file to the database
func (c *Coordinator) saveFile(file *models.FileRecord) error {
	data, err := json.Marshal(file)
	if err != nil {
		return fmt.Errorf("failed to marshal file: %w", err)
	}
	return c.db.Set(filesBucket, file.FileID, data)
}

// saveChunk saves a chunk to the database
func (c *Coordinator) saveChunk(chunk *models.ChunkInfo) error {
	data, err := json.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("failed to marshal chunk: %w", err)
	}
	return c.db.Set(chunksBucket, chunk.ChunkID, data)
}

// getBootstrapPeers returns a list of active nodes for bootstrapping
func (c *Coordinator) getBootstrapPeers(excludeNodeID string, limit int) []string {
	var peers []string
	count := 0

	for nodeID, node := range c.nodes {
		if nodeID == excludeNodeID {
			continue
		}
		if node.Status == "active" && time.Since(node.LastHeartbeat) < c.config.NodeInactiveAfter {
			if len(node.Addresses) > 0 {
				peers = append(peers, node.Addresses[0]) // Use first address
				count++
				if count >= limit {
					break
				}
			}
		}
	}

	return peers
}

// selectNodesForChunk selects the best nodes to store a chunk
func (c *Coordinator) selectNodesForChunk(chunkID string, replicationFactor int) []string {
	var candidates []*nodeCandidate

	c.nodesMux.RLock()
	for nodeID, node := range c.nodes {
		if node.Status == "active" && time.Since(node.LastHeartbeat) < c.config.NodeInactiveAfter {
			if node.Stats == nil {
				continue
			}

			// Calculate availability score
			availableSpace := node.StorageCapacity - node.Stats.StorageUsed
			if availableSpace <= 0 {
				continue
			}

			score := c.calculateNodeScore(node)
			candidates = append(candidates, &nodeCandidate{
				NodeID:    nodeID,
				Node:      node,
				Score:     score,
				Available: availableSpace,
			})
		}
	}
	c.nodesMux.RUnlock()

	if len(candidates) == 0 {
		logrus.Warn("No suitable nodes found for chunk placement")
		return []string{}
	}

	// Sort by score (higher is better)
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[i].Score < candidates[j].Score {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	// Select top nodes up to replication factor
	limit := replicationFactor
	if len(candidates) < limit {
		limit = len(candidates)
	}

	if limit > c.config.MaxNodesPerChunk {
		limit = c.config.MaxNodesPerChunk
	}

	var selectedNodes []string
	for i := 0; i < limit; i++ {
		selectedNodes = append(selectedNodes, candidates[i].NodeID)
	}

	return selectedNodes
}

// nodeCandidate represents a node candidate for chunk storage
type nodeCandidate struct {
	NodeID    string
	Node      *models.NodeInfo
	Score     float64
	Available int64
}

// calculateNodeScore calculates a scoring metric for node selection
func (c *Coordinator) calculateNodeScore(node *models.NodeInfo) float64 {
	if node.Stats == nil {
		return 0.0
	}

	// Factors in scoring:
	// 1. Available storage (normalized)
	// 2. Uptime percentage
	// 3. CPU and memory usage (inverted - lower is better)
	// 4. Bandwidth capacity

	stats := node.Stats

	// Available storage score (0-1)
	storageScore := 0.0
	if node.StorageCapacity > 0 {
		available := float64(node.StorageCapacity - stats.StorageUsed)
		storageScore = math.Min(available/float64(node.StorageCapacity), 1.0)
	}

	// Uptime score (0-1)
	uptimeScore := 0.0
	if stats.UptimeSeconds > 0 {
		// Assume we want at least 24 hours uptime for full score
		targetUptime := 24 * 60 * 60 // 24 hours in seconds
		uptimeScore = math.Min(float64(stats.UptimeSeconds)/float64(targetUptime), 1.0)
	}

	// Resource usage score (0-1, inverted so lower usage = higher score)
	cpuScore := math.Max(0, 1.0-stats.CpuUsage/100.0)
	memoryScore := math.Max(0, 1.0-stats.MemoryUsage/100.0)

	// Bandwidth score (higher is better)
	bandwidthScore := 0.0
	totalBandwidth := stats.BandwidthUp + stats.BandwidthDown
	if totalBandwidth > 0 {
		// Normalize to 100 Mbps as "good" bandwidth
		goodBandwidth := int64(100 * 1024 * 1024 / 8) // 100 Mbps in bytes/sec
		bandwidthScore = math.Min(float64(totalBandwidth)/float64(goodBandwidth), 1.0)
	}

	// Weighted average
	weights := map[string]float64{
		"storage":   0.3,
		"uptime":    0.25,
		"cpu":       0.15,
		"memory":    0.15,
		"bandwidth": 0.15,
	}

	totalScore := weights["storage"]*storageScore +
		weights["uptime"]*uptimeScore +
		weights["cpu"]*cpuScore +
		weights["memory"]*memoryScore +
		weights["bandwidth"]*bandwidthScore

	return totalScore
}

// generateTasksForNode creates tasks for a specific node
func (c *Coordinator) generateTasksForNode(nodeID string) []string {
	var tasks []string

	// Check if node needs to store any chunks
	// Check if node needs to replicate chunks
	// Check if node needs to perform maintenance

	// For now, return empty tasks
	// This will be expanded with specific task types

	return tasks
}

// redistributeChunksFromNode handles chunk redistribution when a node goes offline
func (c *Coordinator) redistributeChunksFromNode(nodeID string) {
	c.chunksMux.Lock()
	defer c.chunksMux.Unlock()

	var affectedChunks []*models.ChunkInfo

	// Find all chunks stored on the offline node
	for _, chunk := range c.chunks {
		for _, storedNodeID := range chunk.StoredAtNodes {
			if storedNodeID == nodeID {
				affectedChunks = append(affectedChunks, chunk)
				break
			}
		}
	}

	logrus.WithFields(logrus.Fields{
		"nodeID":         nodeID,
		"affectedChunks": len(affectedChunks),
	}).Info("Redistributing chunks from offline node")

	// For each affected chunk, find new storage nodes
	for _, chunk := range affectedChunks {
		// Remove the offline node from stored locations
		var newStoredNodes []string
		for _, storedNodeID := range chunk.StoredAtNodes {
			if storedNodeID != nodeID {
				newStoredNodes = append(newStoredNodes, storedNodeID)
			}
		}

		// If we're below replication factor, select new nodes
		needed := c.config.ReplicationFactor - len(newStoredNodes)
		if needed > 0 {
			// Exclude nodes that already have this chunk
			excludeMap := make(map[string]bool)
			for _, nodeID := range newStoredNodes {
				excludeMap[nodeID] = true
			}

			candidates := c.selectNodesForChunkExcluding(chunk.ChunkID, needed, excludeMap)
			newStoredNodes = append(newStoredNodes, candidates...)
		}

		// Update chunk information
		chunk.StoredAtNodes = newStoredNodes
		if err := c.saveChunk(chunk); err != nil {
			logrus.WithError(err).WithField("chunkID", chunk.ChunkID).Error("Failed to update chunk after redistribution")
		}
	}
}

// selectNodesForChunkExcluding selects nodes for chunk storage excluding specific nodes
func (c *Coordinator) selectNodesForChunkExcluding(chunkID string, count int, exclude map[string]bool) []string {
	var candidates []*nodeCandidate

	c.nodesMux.RLock()
	for nodeID, node := range c.nodes {
		if exclude[nodeID] {
			continue
		}
		if node.Status == "active" && time.Since(node.LastHeartbeat) < c.config.NodeInactiveAfter {
			if node.Stats == nil {
				continue
			}

			availableSpace := node.StorageCapacity - node.Stats.StorageUsed
			if availableSpace <= 0 {
				continue
			}

			score := c.calculateNodeScore(node)
			candidates = append(candidates, &nodeCandidate{
				NodeID:    nodeID,
				Node:      node,
				Score:     score,
				Available: availableSpace,
			})
		}
	}
	c.nodesMux.RUnlock()

	// Sort by score
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[i].Score < candidates[j].Score {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	// Select top nodes
	limit := count
	if len(candidates) < limit {
		limit = len(candidates)
	}

	var selectedNodes []string
	for i := 0; i < limit; i++ {
		selectedNodes = append(selectedNodes, candidates[i].NodeID)
	}

	return selectedNodes
}

// startBackgroundTasks starts background maintenance tasks
func (c *Coordinator) startBackgroundTasks() {
	// Node cleanup task
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.config.CleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.cleanupInactiveNodes()
			case <-c.stopChan:
				return
			}
		}
	}()

	// Statistics update task
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.updateNetworkStats()
			case <-c.stopChan:
				return
			}
		}
	}()
}

// cleanupInactiveNodes removes nodes that haven't sent heartbeats
func (c *Coordinator) cleanupInactiveNodes() {
	c.nodesMux.Lock()
	defer c.nodesMux.Unlock()

	var toRemove []string
	cutoff := c.config.NodeInactiveAfter * 3 // 3x the timeout duration

	for nodeID, node := range c.nodes {
		if time.Since(node.LastHeartbeat) > cutoff {
			toRemove = append(toRemove, nodeID)
		}
	}

	for _, nodeID := range toRemove {
		delete(c.nodes, nodeID)
		c.db.Delete(nodesBucket, nodeID)

		// Trigger chunk redistribution
		go c.redistributeChunksFromNode(nodeID)

		logrus.WithField("nodeID", nodeID).Info("Removed inactive node")
	}
}

// updateNetworkStats updates network-wide statistics
func (c *Coordinator) updateNetworkStats() {
	c.nodesMux.RLock()
	c.filesMux.RLock()
	c.chunksMux.RLock()
	defer c.nodesMux.RUnlock()
	defer c.filesMux.RUnlock()
	defer c.chunksMux.RUnlock()

	activeCount := 0
	var totalCapacity, totalUsed int64

	for _, node := range c.nodes {
		if node.Status == "active" && time.Since(node.LastHeartbeat) < c.config.NodeInactiveAfter {
			activeCount++
			totalCapacity += node.StorageCapacity
			if node.Stats != nil {
				totalUsed += node.Stats.StorageUsed
			}
		}
	}

	c.stats = &models.NetworkStats{
		TotalNodes:             int32(len(c.nodes)),
		ActiveNodes:            int32(activeCount),
		TotalStorageCapacity:   totalCapacity,
		TotalStorageUsed:       totalUsed,
		TotalFiles:             int64(len(c.files)),
		TotalChunks:            int64(len(c.chunks)),
		NetworkUptimeSeconds:   int64(time.Since(time.Now().Add(-24 * time.Hour)).Seconds()),
		AverageNodeUptime:      0, // Calculate if needed
		Timestamp:              time.Now().Unix(),
	}
}