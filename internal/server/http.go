package server

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/ZephyrFS/zephyrfs-coordinator/internal/coordinator"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/models"
)

// HTTPServer wraps the coordinator for HTTP API access
type HTTPServer struct {
	coordinator *coordinator.Coordinator
}

// NewHTTPServer creates a new HTTP server instance
func NewHTTPServer(coord *coordinator.Coordinator) *HTTPServer {
	return &HTTPServer{
		coordinator: coord,
	}
}

// SetupHTTPRoutes configures all HTTP API routes
func SetupHTTPRoutes(router *gin.Engine, coord *coordinator.Coordinator) {
	server := NewHTTPServer(coord)

	// Add middleware
	router.Use(server.loggingMiddleware())
	router.Use(server.corsMiddleware())

	// API versioning
	v1 := router.Group("/api/v1")
	{
		// Node management
		nodes := v1.Group("/nodes")
		{
			nodes.POST("/register", server.registerNode)
			nodes.POST("/:nodeId/unregister", server.unregisterNode)
			nodes.GET("/active", server.getActiveNodes)
			nodes.POST("/:nodeId/heartbeat", server.nodeHeartbeat)
			nodes.GET("/:nodeId", server.getNodeInfo)
		}

		// File management
		files := v1.Group("/files")
		{
			files.POST("/register", server.registerFile)
			files.GET("/:fileId", server.getFileInfo)
			files.DELETE("/:fileId", server.deleteFile)
			files.GET("", server.listFiles)
		}

		// Chunk management
		chunks := v1.Group("/chunks")
		{
			chunks.GET("/:chunkId/locations", server.findChunkLocations)
			chunks.PUT("/:chunkId/locations", server.updateChunkLocations)
			chunks.GET("/:chunkId", server.getChunkInfo)
		}

		// Network status and monitoring
		network := v1.Group("/network")
		{
			network.GET("/status", server.getNetworkStatus)
			network.GET("/stats", server.getNetworkStats)
		}

		// Admin endpoints
		admin := v1.Group("/admin")
		{
			admin.GET("/database/stats", server.getDatabaseStats)
			admin.POST("/database/backup", server.backupDatabase)
			admin.POST("/database/cleanup", server.cleanupDatabase)
		}
	}

	// Health check endpoint (no versioning)
	router.GET("/health", server.healthCheck)
	router.GET("/", server.apiInfo)

	logrus.Info("HTTP API routes configured")
}

// Health check endpoint
func (s *HTTPServer) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"service":   "zephyrfs-coordinator",
		"timestamp": gin.H{"unix": gin.H{}},
	})
}

// API information endpoint
func (s *HTTPServer) apiInfo(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"service":     "ZephyrFS Coordinator",
		"version":     "1.0.0",
		"description": "Coordination server for ZephyrFS distributed storage network",
		"endpoints": gin.H{
			"health":       "/health",
			"api_v1":       "/api/v1",
			"nodes":        "/api/v1/nodes",
			"files":        "/api/v1/files",
			"chunks":       "/api/v1/chunks",
			"network":      "/api/v1/network",
			"admin":        "/api/v1/admin",
		},
	})
}

// Node management endpoints

func (s *HTTPServer) registerNode(c *gin.Context) {
	var req models.RegisterNodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format", "details": err.Error()})
		return
	}

	resp, err := s.coordinator.RegisterNode(c.Request.Context(), &req)
	if err != nil {
		logrus.WithError(err).Error("Failed to register node via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register node", "details": err.Error()})
		return
	}

	if resp.Success {
		c.JSON(http.StatusOK, resp)
	} else {
		c.JSON(http.StatusBadRequest, resp)
	}
}

func (s *HTTPServer) unregisterNode(c *gin.Context) {
	nodeID := c.Param("nodeId")
	if nodeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Node ID is required"})
		return
	}

	var reqBody struct {
		Reason string `json:"reason"`
	}
	c.ShouldBindJSON(&reqBody)

	req := &models.UnregisterNodeRequest{
		NodeID: nodeID,
		Reason: reqBody.Reason,
	}

	resp, err := s.coordinator.UnregisterNode(c.Request.Context(), req)
	if err != nil {
		logrus.WithError(err).Error("Failed to unregister node via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to unregister node", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (s *HTTPServer) getActiveNodes(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid limit parameter"})
		return
	}

	excludeNodes := c.QueryArray("exclude")

	req := &models.GetActiveNodesRequest{
		Limit:        int32(limit),
		ExcludeNodes: excludeNodes,
	}

	resp, err := s.coordinator.GetActiveNodes(c.Request.Context(), req)
	if err != nil {
		logrus.WithError(err).Error("Failed to get active nodes via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get active nodes", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (s *HTTPServer) nodeHeartbeat(c *gin.Context) {
	nodeID := c.Param("nodeId")
	if nodeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Node ID is required"})
		return
	}

	var reqBody struct {
		Stats *models.NodeStats `json:"stats"`
	}
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format", "details": err.Error()})
		return
	}

	req := &models.NodeHeartbeatRequest{
		NodeID: nodeID,
		Stats:  reqBody.Stats,
	}

	resp, err := s.coordinator.NodeHeartbeat(c.Request.Context(), req)
	if err != nil {
		logrus.WithError(err).Error("Failed to process heartbeat via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process heartbeat", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (s *HTTPServer) getNodeInfo(c *gin.Context) {
	nodeID := c.Param("nodeId")
	if nodeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Node ID is required"})
		return
	}

	// Implementation would need to be added to coordinator
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Not implemented yet"})
}

// File management endpoints

func (s *HTTPServer) registerFile(c *gin.Context) {
	var req models.RegisterFileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format", "details": err.Error()})
		return
	}

	resp, err := s.coordinator.RegisterFile(c.Request.Context(), &req)
	if err != nil {
		logrus.WithError(err).Error("Failed to register file via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register file", "details": err.Error()})
		return
	}

	if resp.Success {
		c.JSON(http.StatusCreated, resp)
	} else {
		c.JSON(http.StatusBadRequest, resp)
	}
}

func (s *HTTPServer) getFileInfo(c *gin.Context) {
	fileID := c.Param("fileId")
	if fileID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File ID is required"})
		return
	}

	req := &models.GetFileInfoRequest{
		FileID: fileID,
	}

	resp, err := s.coordinator.GetFileInfo(c.Request.Context(), req)
	if err != nil {
		logrus.WithError(err).Error("Failed to get file info via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get file info", "details": err.Error()})
		return
	}

	if resp.Success {
		c.JSON(http.StatusOK, resp)
	} else {
		c.JSON(http.StatusNotFound, resp)
	}
}

func (s *HTTPServer) deleteFile(c *gin.Context) {
	fileID := c.Param("fileId")
	if fileID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File ID is required"})
		return
	}

	// Implementation would need to be added to coordinator
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Not implemented yet"})
}

func (s *HTTPServer) listFiles(c *gin.Context) {
	// Implementation would need to be added to coordinator
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Not implemented yet"})
}

// Chunk management endpoints

func (s *HTTPServer) findChunkLocations(c *gin.Context) {
	chunkID := c.Param("chunkId")
	if chunkID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Chunk ID is required"})
		return
	}

	preferredCountStr := c.DefaultQuery("preferred_count", "0")
	preferredCount, err := strconv.Atoi(preferredCountStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid preferred_count parameter"})
		return
	}

	req := &models.FindChunkLocationsRequest{
		ChunkID:        chunkID,
		PreferredCount: int32(preferredCount),
	}

	resp, err := s.coordinator.FindChunkLocations(c.Request.Context(), req)
	if err != nil {
		logrus.WithError(err).Error("Failed to find chunk locations via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to find chunk locations", "details": err.Error()})
		return
	}

	if resp.Success {
		c.JSON(http.StatusOK, resp)
	} else {
		c.JSON(http.StatusNotFound, resp)
	}
}

func (s *HTTPServer) updateChunkLocations(c *gin.Context) {
	chunkID := c.Param("chunkId")
	if chunkID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Chunk ID is required"})
		return
	}

	var reqBody struct {
		NodeIDs   []string `json:"node_ids" binding:"required"`
		Operation string   `json:"operation" binding:"required"`
	}
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format", "details": err.Error()})
		return
	}

	req := &models.UpdateChunkLocationsRequest{
		ChunkID:   chunkID,
		NodeIDs:   reqBody.NodeIDs,
		Operation: reqBody.Operation,
	}

	resp, err := s.coordinator.UpdateChunkLocations(c.Request.Context(), req)
	if err != nil {
		logrus.WithError(err).Error("Failed to update chunk locations via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update chunk locations", "details": err.Error()})
		return
	}

	if resp.Success {
		c.JSON(http.StatusOK, resp)
	} else {
		c.JSON(http.StatusBadRequest, resp)
	}
}

func (s *HTTPServer) getChunkInfo(c *gin.Context) {
	chunkID := c.Param("chunkId")
	if chunkID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Chunk ID is required"})
		return
	}

	// Implementation would need to be added to coordinator
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Not implemented yet"})
}

// Network status endpoints

func (s *HTTPServer) getNetworkStatus(c *gin.Context) {
	resp, err := s.coordinator.GetNetworkStatus(c.Request.Context())
	if err != nil {
		logrus.WithError(err).Error("Failed to get network status via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get network status", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (s *HTTPServer) getNetworkStats(c *gin.Context) {
	// Simplified version that returns just the network stats
	resp, err := s.coordinator.GetNetworkStatus(c.Request.Context())
	if err != nil {
		logrus.WithError(err).Error("Failed to get network stats via HTTP")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get network stats", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp.NetworkStats)
}

// Admin endpoints

func (s *HTTPServer) getDatabaseStats(c *gin.Context) {
	// Implementation would need database stats access
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Not implemented yet"})
}

func (s *HTTPServer) backupDatabase(c *gin.Context) {
	// Implementation would need backup functionality
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Not implemented yet"})
}

func (s *HTTPServer) cleanupDatabase(c *gin.Context) {
	// Implementation would need cleanup functionality
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Not implemented yet"})
}

// Middleware

func (s *HTTPServer) loggingMiddleware() gin.HandlerFunc {
	return gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		logrus.WithFields(logrus.Fields{
			"method":     param.Method,
			"path":       param.Path,
			"status":     param.StatusCode,
			"latency":    param.Latency,
			"ip":         param.ClientIP,
			"user_agent": param.Request.UserAgent(),
		}).Info("HTTP request")
		return ""
	})
}

func (s *HTTPServer) corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization, X-Requested-With")
		c.Header("Access-Control-Allow-Credentials", "true")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}