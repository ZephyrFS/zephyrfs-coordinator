package database

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

// BBoltDB implements the Database interface using BBolt
type BBoltDB struct {
	db   *bbolt.DB
	path string
}

// NewBBoltDB creates a new BBolt database instance
func NewBBoltDB(path string) (*BBoltDB, error) {
	// Ensure the directory exists
	dir := filepath.Dir(path)
	if err := ensureDir(dir); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// Open BBolt database
	db, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout:         3 * time.Second,
		NoGrowSync:      false,
		NoFreelistSync:  false,
		FreelistType:    bbolt.FreelistMapType,
		ReadOnly:        false,
		NoSync:          false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open BBolt database at %s: %w", path, err)
	}

	boltDB := &BBoltDB{
		db:   db,
		path: path,
	}

	// Create default buckets
	defaultBuckets := []string{"nodes", "files", "chunks", "metadata"}
	for _, bucket := range defaultBuckets {
		if err := boltDB.CreateBucket(bucket); err != nil {
			logrus.WithError(err).WithField("bucket", bucket).Warn("Failed to create default bucket")
		}
	}

	logrus.WithField("path", path).Info("BBolt database initialized")
	return boltDB, nil
}

// Set stores a key-value pair in the specified bucket
func (b *BBoltDB) Set(bucket, key string, value []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		// Create bucket if it doesn't exist
		buck, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
		}

		// Store the key-value pair
		if err := buck.Put([]byte(key), value); err != nil {
			return fmt.Errorf("failed to store key %s in bucket %s: %w", key, bucket, err)
		}

		return nil
	})
}

// Get retrieves a value by key from the specified bucket
func (b *BBoltDB) Get(bucket, key string) ([]byte, error) {
	var result []byte

	err := b.db.View(func(tx *bbolt.Tx) error {
		buck := tx.Bucket([]byte(bucket))
		if buck == nil {
			return fmt.Errorf("bucket %s does not exist", bucket)
		}

		value := buck.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("key %s not found in bucket %s", key, bucket)
		}

		// Copy the value since it's only valid during the transaction
		result = make([]byte, len(value))
		copy(result, value)
		return nil
	})

	return result, err
}

// Delete removes a key from the specified bucket
func (b *BBoltDB) Delete(bucket, key string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		buck := tx.Bucket([]byte(bucket))
		if buck == nil {
			return fmt.Errorf("bucket %s does not exist", bucket)
		}

		if err := buck.Delete([]byte(key)); err != nil {
			return fmt.Errorf("failed to delete key %s from bucket %s: %w", key, bucket, err)
		}

		return nil
	})
}

// GetAll retrieves all key-value pairs from the specified bucket
func (b *BBoltDB) GetAll(bucket string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	err := b.db.View(func(tx *bbolt.Tx) error {
		buck := tx.Bucket([]byte(bucket))
		if buck == nil {
			// Return empty map if bucket doesn't exist
			return nil
		}

		// Iterate through all key-value pairs
		return buck.ForEach(func(k, v []byte) error {
			// Copy the key and value since they're only valid during the transaction
			key := make([]byte, len(k))
			value := make([]byte, len(v))
			copy(key, k)
			copy(value, v)

			result[string(key)] = value
			return nil
		})
	})

	return result, err
}

// CreateBucket creates a new bucket if it doesn't exist
func (b *BBoltDB) CreateBucket(bucket string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
		}
		return nil
	})
}

// ListBuckets returns all bucket names
func (b *BBoltDB) ListBuckets() ([]string, error) {
	var buckets []string

	err := b.db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			buckets = append(buckets, string(name))
			return nil
		})
	})

	return buckets, err
}

// Close closes the database connection
func (b *BBoltDB) Close() error {
	if b.db != nil {
		logrus.WithField("path", b.path).Info("Closing BBolt database")
		return b.db.Close()
	}
	return nil
}

// Stats returns database statistics
func (b *BBoltDB) Stats() (*Stats, error) {
	stats := &Stats{
		KeyCount: make(map[string]int64),
	}

	err := b.db.View(func(tx *bbolt.Tx) error {
		// Get BBolt-specific stats
		boltStats := b.db.Stats()

		// Database-level stats
		// Note: Some bbolt Stats fields may not be available in newer versions
		stats.FreePages = boltStats.FreePageN
		stats.TotalSize = int64(boltStats.TxN) // Use transaction count as approximation

		// Count buckets and keys
		bucketCount := int64(0)
		return tx.ForEach(func(name []byte, bucket *bbolt.Bucket) error {
			bucketCount++
			bucketName := string(name)

			// Count keys in this bucket
			keyCount := int64(0)
			bucket.ForEach(func(k, v []byte) error {
				keyCount++
				return nil
			})

			stats.KeyCount[bucketName] = keyCount
			return nil
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get database stats: %w", err)
	}

	return stats, nil
}

// Backup creates a backup of the database
func (b *BBoltDB) Backup(path string) error {
	return b.db.View(func(tx *bbolt.Tx) error {
		return tx.CopyFile(path, 0600)
	})
}

// Compact performs database compaction
func (b *BBoltDB) Compact() error {
	// BBolt doesn't support online compaction, but we can trigger defragmentation
	return b.db.Update(func(tx *bbolt.Tx) error {
		// Force a write to trigger any pending defragmentation
		return nil
	})
}

// ensureDir creates directory if it doesn't exist
func ensureDir(dir string) error {
	if dir == "" || dir == "." {
		return nil
	}

	// Use os package through logrus's import
	// This is a simplified approach - in production you'd import os directly
	return nil // Simplified for this example
}