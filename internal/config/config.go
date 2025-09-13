package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the coordinator configuration
type Config struct {
	Database    DatabaseConfig    `yaml:"database"`
	GRPC        GRPCConfig        `yaml:"grpc"`
	HTTP        HTTPConfig        `yaml:"http"`
	Coordinator CoordinatorConfig `yaml:"coordinator"`
	Health      HealthConfig      `yaml:"health"`
}

// DatabaseConfig contains database settings
type DatabaseConfig struct {
	Type string `yaml:"type"` // "bbolt" or "postgres"
	Path string `yaml:"path"` // For bbolt
	URL  string `yaml:"url"`  // For postgres
}

// GRPCConfig contains gRPC server settings
type GRPCConfig struct {
	Port             int  `yaml:"port"`
	MaxMessageSize   int  `yaml:"max_message_size"`
	EnableReflection bool `yaml:"enable_reflection"`
}

// HTTPConfig contains HTTP server settings
type HTTPConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

// CoordinatorConfig contains coordinator-specific settings
type CoordinatorConfig struct {
	NodeTimeout        time.Duration `yaml:"node_timeout"`
	HeartbeatInterval  time.Duration `yaml:"heartbeat_interval"`
	ReplicationFactor  int           `yaml:"replication_factor"`
	MaxNodesPerChunk   int           `yaml:"max_nodes_per_chunk"`
	CleanupInterval    time.Duration `yaml:"cleanup_interval"`
	NodeInactiveAfter  time.Duration `yaml:"node_inactive_after"`
	GeographicSpread   bool          `yaml:"geographic_spread"`
}

// HealthConfig contains health monitoring settings
type HealthConfig struct {
	CheckInterval     time.Duration `yaml:"check_interval"`
	MetricsEnabled    bool          `yaml:"metrics_enabled"`
	MetricsPort       int           `yaml:"metrics_port"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Database: DatabaseConfig{
			Type: "bbolt",
			Path: "coordinator.db",
		},
		GRPC: GRPCConfig{
			Port:             8080,
			MaxMessageSize:   4 * 1024 * 1024, // 4MB
			EnableReflection: false,
		},
		HTTP: HTTPConfig{
			Enabled: true,
			Port:    8090,
		},
		Coordinator: CoordinatorConfig{
			NodeTimeout:        30 * time.Second,
			HeartbeatInterval:  10 * time.Second,
			ReplicationFactor:  3,
			MaxNodesPerChunk:   10,
			CleanupInterval:    5 * time.Minute,
			NodeInactiveAfter:  60 * time.Second,
			GeographicSpread:   true,
		},
		Health: HealthConfig{
			CheckInterval:  30 * time.Second,
			MetricsEnabled: true,
			MetricsPort:    8091,
		},
	}
}

// Load reads configuration from a YAML file, merging with defaults
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Config file doesn't exist, use defaults
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Database.Type == "" {
		return fmt.Errorf("database type is required")
	}

	if c.Database.Type == "bbolt" && c.Database.Path == "" {
		return fmt.Errorf("database path is required for bbolt")
	}

	if c.Database.Type == "postgres" && c.Database.URL == "" {
		return fmt.Errorf("database URL is required for postgres")
	}

	if c.GRPC.Port <= 0 || c.GRPC.Port > 65535 {
		return fmt.Errorf("invalid gRPC port: %d", c.GRPC.Port)
	}

	if c.HTTP.Enabled && (c.HTTP.Port <= 0 || c.HTTP.Port > 65535) {
		return fmt.Errorf("invalid HTTP port: %d", c.HTTP.Port)
	}

	if c.Coordinator.ReplicationFactor <= 0 {
		return fmt.Errorf("replication factor must be positive")
	}

	if c.Coordinator.MaxNodesPerChunk <= 0 {
		return fmt.Errorf("max nodes per chunk must be positive")
	}

	return nil
}

// Save writes the configuration to a YAML file
func (c *Config) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file %s: %w", path, err)
	}

	return nil
}