package health

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/ZephyrFS/zephyrfs-coordinator/internal/config"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/coordinator"
)

// Monitor represents the health monitoring system
type Monitor struct {
	coordinator *coordinator.Coordinator
	config      config.HealthConfig
	metrics     *Metrics
	startTime   time.Time
}

// Metrics represents collected health metrics
type Metrics struct {
	// System metrics
	MemoryUsage    MemoryStats    `json:"memory_usage"`
	CPUUsage       float64        `json:"cpu_usage"`
	GoroutineCount int            `json:"goroutine_count"`

	// Application metrics
	RequestCount    int64                  `json:"request_count"`
	ErrorCount      int64                  `json:"error_count"`
	ResponseTimes   ResponseTimeStats      `json:"response_times"`
	DatabaseStats   DatabaseStats          `json:"database_stats"`

	// Network metrics
	NetworkStats    NetworkHealthStats     `json:"network_stats"`

	// Coordinator-specific metrics
	CoordinatorStats CoordinatorHealthStats `json:"coordinator_stats"`

	// Timestamps
	LastUpdated time.Time `json:"last_updated"`
	Uptime      string    `json:"uptime"`
}

// MemoryStats represents memory usage statistics
type MemoryStats struct {
	Allocated       uint64  `json:"allocated"`        // bytes allocated and still in use
	TotalAllocated  uint64  `json:"total_allocated"`  // bytes allocated (even if freed)
	SystemMemory    uint64  `json:"system_memory"`    // bytes obtained from system
	GCCount         uint32  `json:"gc_count"`         // number of garbage collections
	HeapSize        uint64  `json:"heap_size"`        // heap size
	HeapInUse       uint64  `json:"heap_in_use"`      // heap bytes in use
}

// ResponseTimeStats represents response time statistics
type ResponseTimeStats struct {
	Average    float64 `json:"average"`
	Min        float64 `json:"min"`
	Max        float64 `json:"max"`
	P50        float64 `json:"p50"`
	P95        float64 `json:"p95"`
	P99        float64 `json:"p99"`
}

// DatabaseStats represents database health statistics
type DatabaseStats struct {
	ConnectionCount int64 `json:"connection_count"`
	QueryCount      int64 `json:"query_count"`
	ErrorCount      int64 `json:"error_count"`
	AverageLatency  float64 `json:"average_latency"`
}

// NetworkHealthStats represents network health statistics
type NetworkHealthStats struct {
	ActiveNodes      int   `json:"active_nodes"`
	InactiveNodes    int   `json:"inactive_nodes"`
	TotalConnections int64 `json:"total_connections"`
	FailedConnections int64 `json:"failed_connections"`
}

// CoordinatorHealthStats represents coordinator-specific health metrics
type CoordinatorHealthStats struct {
	RegisteredNodes  int   `json:"registered_nodes"`
	ActiveFiles      int   `json:"active_files"`
	TotalChunks      int   `json:"total_chunks"`
	ReplicationTasks int   `json:"replication_tasks"`
	LastHeartbeat    int64 `json:"last_heartbeat"`
}

// NewMonitor creates a new health monitor
func NewMonitor(coord *coordinator.Coordinator, cfg config.HealthConfig) *Monitor {
	return &Monitor{
		coordinator: coord,
		config:      cfg,
		metrics:     &Metrics{},
		startTime:   time.Now(),
	}
}

// StartMonitoring starts the health monitoring background process
func StartMonitoring(ctx context.Context, coord *coordinator.Coordinator, cfg config.HealthConfig) {
	monitor := NewMonitor(coord, cfg)

	// Start metrics collection
	go monitor.collectMetrics(ctx)

	// Start metrics HTTP server if enabled
	if cfg.MetricsEnabled {
		go monitor.startMetricsServer(ctx)
	}

	logrus.WithFields(logrus.Fields{
		"check_interval": cfg.CheckInterval,
		"metrics_enabled": cfg.MetricsEnabled,
		"metrics_port": cfg.MetricsPort,
	}).Info("Health monitoring started")
}

// collectMetrics runs the periodic metrics collection
func (m *Monitor) collectMetrics(ctx context.Context) {
	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Stopping health metrics collection")
			return
		case <-ticker.C:
			m.updateMetrics()
		}
	}
}

// updateMetrics collects current system and application metrics
func (m *Monitor) updateMetrics() {
	m.metrics.LastUpdated = time.Now()
	m.metrics.Uptime = time.Since(m.startTime).String()

	// Collect system metrics
	m.collectSystemMetrics()

	// Collect application metrics
	m.collectApplicationMetrics()

	// Collect network metrics
	m.collectNetworkMetrics()

	// Collect coordinator-specific metrics
	m.collectCoordinatorMetrics()

	// Log summary metrics periodically
	if time.Since(m.startTime).Minutes() > 1 &&
	   int(time.Since(m.startTime).Minutes()) % 5 == 0 {
		m.logMetricsSummary()
	}
}

// collectSystemMetrics gathers system-level metrics
func (m *Monitor) collectSystemMetrics() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	m.metrics.MemoryUsage = MemoryStats{
		Allocated:      memStats.Alloc,
		TotalAllocated: memStats.TotalAlloc,
		SystemMemory:   memStats.Sys,
		GCCount:        memStats.NumGC,
		HeapSize:       memStats.HeapSys,
		HeapInUse:      memStats.HeapInuse,
	}

	m.metrics.GoroutineCount = runtime.NumGoroutine()
}

// collectApplicationMetrics gathers application-level metrics
func (m *Monitor) collectApplicationMetrics() {
	// These would be populated by middleware and other components
	// For now, we'll set placeholder values

	m.metrics.ResponseTimes = ResponseTimeStats{
		Average: 25.5,
		Min:     1.0,
		Max:     150.0,
		P50:     20.0,
		P95:     75.0,
		P99:     120.0,
	}

	m.metrics.DatabaseStats = DatabaseStats{
		ConnectionCount: 1,
		QueryCount:      m.metrics.DatabaseStats.QueryCount + 1,
		ErrorCount:      0,
		AverageLatency:  2.5,
	}
}

// collectNetworkMetrics gathers network-related metrics
func (m *Monitor) collectNetworkMetrics() {
	// Get network status from coordinator
	if resp, err := m.coordinator.GetNetworkStatus(context.Background()); err == nil {
		m.metrics.NetworkStats = NetworkHealthStats{
			ActiveNodes:       int(resp.NetworkStats.ActiveNodes),
			InactiveNodes:     int(resp.NetworkStats.TotalNodes - resp.NetworkStats.ActiveNodes),
			TotalConnections:  resp.NetworkStats.TotalFiles, // Placeholder
			FailedConnections: 0, // Would need to track this
		}
	}
}

// collectCoordinatorMetrics gathers coordinator-specific metrics
func (m *Monitor) collectCoordinatorMetrics() {
	if resp, err := m.coordinator.GetNetworkStatus(context.Background()); err == nil {
		m.metrics.CoordinatorStats = CoordinatorHealthStats{
			RegisteredNodes:  int(resp.NetworkStats.TotalNodes),
			ActiveFiles:      int(resp.NetworkStats.TotalFiles),
			TotalChunks:      int(resp.NetworkStats.TotalChunks),
			ReplicationTasks: 0, // Would need to track this
			LastHeartbeat:    time.Now().Unix(),
		}
	}
}

// logMetricsSummary logs a summary of current metrics
func (m *Monitor) logMetricsSummary() {
	logrus.WithFields(logrus.Fields{
		"uptime":              m.metrics.Uptime,
		"memory_allocated_mb": m.metrics.MemoryUsage.Allocated / 1024 / 1024,
		"heap_size_mb":        m.metrics.MemoryUsage.HeapSize / 1024 / 1024,
		"goroutines":          m.metrics.GoroutineCount,
		"gc_count":            m.metrics.MemoryUsage.GCCount,
		"active_nodes":        m.metrics.NetworkStats.ActiveNodes,
		"total_files":         m.metrics.CoordinatorStats.ActiveFiles,
		"total_chunks":        m.metrics.CoordinatorStats.TotalChunks,
	}).Info("Health metrics summary")
}

// startMetricsServer starts the HTTP server for metrics exposure
func (m *Monitor) startMetricsServer(ctx context.Context) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	// Metrics endpoints
	router.GET("/metrics", m.handleMetrics)
	router.GET("/health", m.handleHealth)
	router.GET("/ready", m.handleReadiness)
	router.GET("/live", m.handleLiveness)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", m.config.MetricsPort),
		Handler: router,
	}

	// Start server in goroutine
	go func() {
		logrus.WithField("port", m.config.MetricsPort).Info("Starting metrics HTTP server")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.WithError(err).Error("Metrics server failed")
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Shutdown server gracefully
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logrus.WithError(err).Error("Failed to shutdown metrics server")
	} else {
		logrus.Info("Metrics server stopped")
	}
}

// HTTP handlers for metrics endpoints

func (m *Monitor) handleMetrics(c *gin.Context) {
	c.JSON(http.StatusOK, m.metrics)
}

func (m *Monitor) handleHealth(c *gin.Context) {
	health := m.calculateHealthStatus()

	if health.Status == "healthy" {
		c.JSON(http.StatusOK, health)
	} else {
		c.JSON(http.StatusServiceUnavailable, health)
	}
}

func (m *Monitor) handleReadiness(c *gin.Context) {
	readiness := m.calculateReadinessStatus()

	if readiness.Ready {
		c.JSON(http.StatusOK, readiness)
	} else {
		c.JSON(http.StatusServiceUnavailable, readiness)
	}
}

func (m *Monitor) handleLiveness(c *gin.Context) {
	liveness := m.calculateLivenessStatus()

	if liveness.Alive {
		c.JSON(http.StatusOK, liveness)
	} else {
		c.JSON(http.StatusServiceUnavailable, liveness)
	}
}

// Health status calculation

type HealthStatus struct {
	Status      string                 `json:"status"`
	Timestamp   time.Time              `json:"timestamp"`
	Uptime      string                 `json:"uptime"`
	Version     string                 `json:"version"`
	Checks      map[string]CheckResult `json:"checks"`
}

type CheckResult struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func (m *Monitor) calculateHealthStatus() HealthStatus {
	checks := make(map[string]CheckResult)
	overallHealthy := true

	// Memory check
	memoryHealthy := m.metrics.MemoryUsage.HeapInUse < m.metrics.MemoryUsage.HeapSize*80/100
	checks["memory"] = CheckResult{
		Status:  statusFromBool(memoryHealthy),
		Message: fmt.Sprintf("Heap usage: %d MB / %d MB",
			m.metrics.MemoryUsage.HeapInUse/1024/1024,
			m.metrics.MemoryUsage.HeapSize/1024/1024),
	}
	overallHealthy = overallHealthy && memoryHealthy

	// Goroutine check
	goroutineHealthy := m.metrics.GoroutineCount < 1000 // Arbitrary threshold
	checks["goroutines"] = CheckResult{
		Status:  statusFromBool(goroutineHealthy),
		Message: fmt.Sprintf("Active goroutines: %d", m.metrics.GoroutineCount),
	}
	overallHealthy = overallHealthy && goroutineHealthy

	// Network check
	networkHealthy := m.metrics.NetworkStats.ActiveNodes > 0
	checks["network"] = CheckResult{
		Status:  statusFromBool(networkHealthy),
		Message: fmt.Sprintf("Active nodes: %d", m.metrics.NetworkStats.ActiveNodes),
	}
	overallHealthy = overallHealthy && networkHealthy

	return HealthStatus{
		Status:    statusFromBool(overallHealthy),
		Timestamp: time.Now(),
		Uptime:    m.metrics.Uptime,
		Version:   "1.0.0",
		Checks:    checks,
	}
}

type ReadinessStatus struct {
	Ready     bool                   `json:"ready"`
	Timestamp time.Time              `json:"timestamp"`
	Checks    map[string]CheckResult `json:"checks"`
}

func (m *Monitor) calculateReadinessStatus() ReadinessStatus {
	checks := make(map[string]CheckResult)
	overallReady := true

	// Database readiness
	dbReady := m.metrics.DatabaseStats.ErrorCount == 0
	checks["database"] = CheckResult{
		Status:  statusFromBool(dbReady),
		Message: fmt.Sprintf("Error count: %d", m.metrics.DatabaseStats.ErrorCount),
	}
	overallReady = overallReady && dbReady

	// Coordinator readiness
	coordReady := time.Since(m.startTime) > 10*time.Second // Grace period
	checks["coordinator"] = CheckResult{
		Status:  statusFromBool(coordReady),
		Message: fmt.Sprintf("Running for: %s", m.metrics.Uptime),
	}
	overallReady = overallReady && coordReady

	return ReadinessStatus{
		Ready:     overallReady,
		Timestamp: time.Now(),
		Checks:    checks,
	}
}

type LivenessStatus struct {
	Alive     bool      `json:"alive"`
	Timestamp time.Time `json:"timestamp"`
	LastCheck time.Time `json:"last_check"`
}

func (m *Monitor) calculateLivenessStatus() LivenessStatus {
	// Simple liveness check - if we can execute this function, we're alive
	// In a more complex system, this might check for deadlocks, etc.

	alive := time.Since(m.metrics.LastUpdated) < m.config.CheckInterval*2

	return LivenessStatus{
		Alive:     alive,
		Timestamp: time.Now(),
		LastCheck: m.metrics.LastUpdated,
	}
}

// Helper functions

func statusFromBool(healthy bool) string {
	if healthy {
		return "healthy"
	}
	return "unhealthy"
}