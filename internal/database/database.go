package database

import (
	"fmt"

	"github.com/ZephyrFS/zephyrfs-coordinator/internal/config"
)

// Database represents a generic database interface
type Database interface {
	// Set stores a key-value pair in the specified bucket
	Set(bucket, key string, value []byte) error

	// Get retrieves a value by key from the specified bucket
	Get(bucket, key string) ([]byte, error)

	// Delete removes a key from the specified bucket
	Delete(bucket, key string) error

	// GetAll retrieves all key-value pairs from the specified bucket
	GetAll(bucket string) (map[string][]byte, error)

	// CreateBucket creates a new bucket if it doesn't exist
	CreateBucket(bucket string) error

	// ListBuckets returns all bucket names
	ListBuckets() ([]string, error)

	// Close closes the database connection
	Close() error

	// Stats returns database statistics
	Stats() (*Stats, error)
}

// Stats represents database statistics
type Stats struct {
	BucketCount int64            `json:"bucket_count"`
	KeyCount    map[string]int64 `json:"key_count"`    // Keys per bucket
	TotalSize   int64            `json:"total_size"`   // Total database size in bytes
	PageSize    int              `json:"page_size"`
	FreePages   int              `json:"free_pages"`
}

// New creates a new database instance based on configuration
func New(cfg config.DatabaseConfig) (Database, error) {
	switch cfg.Type {
	case "bbolt":
		return NewBBoltDB(cfg.Path)
	case "postgres":
		return NewPostgresDB(cfg.URL)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Type)
	}
}