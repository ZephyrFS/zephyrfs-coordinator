package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ZephyrFS/zephyrfs-coordinator/internal/config"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/coordinator"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/database"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/health"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/server"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
	logLevel   = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	version    = "dev" // Set during build
	buildTime  = "unknown"
)

func main() {
	flag.Parse()

	// Configure logging
	setupLogging(*logLevel)

	logrus.WithFields(logrus.Fields{
		"version":   version,
		"buildTime": buildTime,
	}).Info("Starting ZephyrFS Coordinator")

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to load configuration")
	}

	logrus.WithField("config", cfg).Debug("Configuration loaded")

	// Initialize database
	db, err := database.New(cfg.Database)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize database")
	}
	defer db.Close()

	// Initialize coordinator service
	coord := coordinator.New(db, cfg.Coordinator)

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start gRPC server
	go func() {
		if err := startGRPCServer(coord, cfg.GRPC); err != nil {
			logrus.WithError(err).Fatal("gRPC server failed")
		}
	}()

	// Start HTTP server
	go func() {
		if err := startHTTPServer(coord, cfg.HTTP); err != nil {
			logrus.WithError(err).Fatal("HTTP server failed")
		}
	}()

	// Start health monitoring
	go func() {
		health.StartMonitoring(ctx, coord, cfg.Health)
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logrus.Info("Shutdown signal received, gracefully stopping...")

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	cancel() // Cancel background goroutines
	coord.Shutdown(shutdownCtx)

	logrus.Info("ZephyrFS Coordinator stopped")
}

func setupLogging(level string) {
	logrus.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})

	switch level {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}
}

func startGRPCServer(coord *coordinator.Coordinator, cfg config.GRPCConfig) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", cfg.Port, err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(server.LoggingInterceptor),
		grpc.MaxRecvMsgSize(cfg.MaxMessageSize),
		grpc.MaxSendMsgSize(cfg.MaxMessageSize),
	)

	// Register coordinator service
	server.RegisterCoordinatorService(grpcServer, coord)

	// Enable reflection for development
	if cfg.EnableReflection {
		reflection.Register(grpcServer)
	}

	logrus.WithField("port", cfg.Port).Info("Starting gRPC server")
	return grpcServer.Serve(listener)
}

func startHTTPServer(coord *coordinator.Coordinator, cfg config.HTTPConfig) error {
	if !cfg.Enabled {
		return nil
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	// Setup HTTP API routes
	server.SetupHTTPRoutes(router, coord)

	logrus.WithField("port", cfg.Port).Info("Starting HTTP server")
	return http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), router)
}