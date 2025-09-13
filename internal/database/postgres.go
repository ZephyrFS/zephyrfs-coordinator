package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// PostgresDB implements the Database interface using PostgreSQL
type PostgresDB struct {
	db  *sql.DB
	url string
}

// NewPostgresDB creates a new PostgreSQL database instance
func NewPostgresDB(url string) (*PostgresDB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL database: %w", err)
	}

	pgDB := &PostgresDB{
		db:  db,
		url: url,
	}

	// Initialize schema
	if err := pgDB.initializeSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	logrus.Info("PostgreSQL database initialized")
	return pgDB, nil
}

// initializeSchema creates the necessary tables
func (p *PostgresDB) initializeSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS coordinator_data (
		bucket VARCHAR(255) NOT NULL,
		key VARCHAR(255) NOT NULL,
		value BYTEA NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (bucket, key)
	);

	CREATE INDEX IF NOT EXISTS idx_coordinator_data_bucket ON coordinator_data(bucket);
	CREATE INDEX IF NOT EXISTS idx_coordinator_data_updated ON coordinator_data(updated_at);

	-- Function to update the updated_at column
	CREATE OR REPLACE FUNCTION update_updated_at_column()
	RETURNS TRIGGER AS $$
	BEGIN
		NEW.updated_at = CURRENT_TIMESTAMP;
		RETURN NEW;
	END;
	$$ language 'plpgsql';

	-- Trigger to automatically update updated_at
	DROP TRIGGER IF EXISTS update_coordinator_data_updated_at ON coordinator_data;
	CREATE TRIGGER update_coordinator_data_updated_at
		BEFORE UPDATE ON coordinator_data
		FOR EACH ROW
		EXECUTE FUNCTION update_updated_at_column();
	`

	_, err := p.db.Exec(schema)
	return err
}

// Set stores a key-value pair in the specified bucket
func (p *PostgresDB) Set(bucket, key string, value []byte) error {
	query := `
		INSERT INTO coordinator_data (bucket, key, value)
		VALUES ($1, $2, $3)
		ON CONFLICT (bucket, key)
		DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
	`

	_, err := p.db.Exec(query, bucket, key, value)
	if err != nil {
		return fmt.Errorf("failed to set key %s in bucket %s: %w", key, bucket, err)
	}

	return nil
}

// Get retrieves a value by key from the specified bucket
func (p *PostgresDB) Get(bucket, key string) ([]byte, error) {
	query := `SELECT value FROM coordinator_data WHERE bucket = $1 AND key = $2`

	var value []byte
	err := p.db.QueryRow(query, bucket, key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("key %s not found in bucket %s", key, bucket)
		}
		return nil, fmt.Errorf("failed to get key %s from bucket %s: %w", key, bucket, err)
	}

	return value, nil
}

// Delete removes a key from the specified bucket
func (p *PostgresDB) Delete(bucket, key string) error {
	query := `DELETE FROM coordinator_data WHERE bucket = $1 AND key = $2`

	result, err := p.db.Exec(query, bucket, key)
	if err != nil {
		return fmt.Errorf("failed to delete key %s from bucket %s: %w", key, bucket, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("key %s not found in bucket %s", key, bucket)
	}

	return nil
}

// GetAll retrieves all key-value pairs from the specified bucket
func (p *PostgresDB) GetAll(bucket string) (map[string][]byte, error) {
	query := `SELECT key, value FROM coordinator_data WHERE bucket = $1`

	rows, err := p.db.Query(query, bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to query bucket %s: %w", bucket, err)
	}
	defer rows.Close()

	result := make(map[string][]byte)

	for rows.Next() {
		var key string
		var value []byte

		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result[key] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
}

// CreateBucket creates a new bucket (no-op for PostgreSQL since we use a single table)
func (p *PostgresDB) CreateBucket(bucket string) error {
	// In PostgreSQL implementation, buckets are just logical groupings
	// We don't need to create anything physically
	return nil
}

// ListBuckets returns all bucket names
func (p *PostgresDB) ListBuckets() ([]string, error) {
	query := `SELECT DISTINCT bucket FROM coordinator_data ORDER BY bucket`

	rows, err := p.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query buckets: %w", err)
	}
	defer rows.Close()

	var buckets []string

	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, fmt.Errorf("failed to scan bucket name: %w", err)
		}
		buckets = append(buckets, bucket)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating bucket rows: %w", err)
	}

	return buckets, nil
}

// Close closes the database connection
func (p *PostgresDB) Close() error {
	if p.db != nil {
		logrus.Info("Closing PostgreSQL database connection")
		return p.db.Close()
	}
	return nil
}

// Stats returns database statistics
func (p *PostgresDB) Stats() (*Stats, error) {
	stats := &Stats{
		KeyCount: make(map[string]int64),
	}

	// Get total database size
	var dbSize sql.NullInt64
	sizeQuery := `SELECT pg_database_size(current_database())`
	err := p.db.QueryRow(sizeQuery).Scan(&dbSize)
	if err != nil {
		logrus.WithError(err).Warn("Failed to get database size")
	} else {
		stats.TotalSize = dbSize.Int64
	}

	// Get bucket counts
	countQuery := `
		SELECT bucket, COUNT(*) as key_count
		FROM coordinator_data
		GROUP BY bucket
	`

	rows, err := p.db.Query(countQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket counts: %w", err)
	}
	defer rows.Close()

	bucketCount := int64(0)
	for rows.Next() {
		var bucket string
		var keyCount int64

		if err := rows.Scan(&bucket, &keyCount); err != nil {
			return nil, fmt.Errorf("failed to scan bucket count: %w", err)
		}

		stats.KeyCount[bucket] = keyCount
		bucketCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating count rows: %w", err)
	}

	stats.BucketCount = bucketCount

	return stats, nil
}

// Cleanup removes old entries (optional maintenance function)
func (p *PostgresDB) Cleanup(olderThan string) error {
	query := `DELETE FROM coordinator_data WHERE updated_at < NOW() - INTERVAL '%s'`

	// Sanitize the interval string
	if !isValidInterval(olderThan) {
		return fmt.Errorf("invalid interval format: %s", olderThan)
	}

	_, err := p.db.Exec(fmt.Sprintf(query, olderThan))
	if err != nil {
		return fmt.Errorf("failed to cleanup old entries: %w", err)
	}

	return nil
}

// Backup creates a logical backup (PostgreSQL-specific)
func (p *PostgresDB) Backup() ([]byte, error) {
	query := `
		SELECT json_agg(
			json_build_object(
				'bucket', bucket,
				'key', key,
				'value', encode(value, 'base64'),
				'updated_at', updated_at
			)
		)
		FROM coordinator_data
	`

	var backupData sql.NullString
	err := p.db.QueryRow(query).Scan(&backupData)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup: %w", err)
	}

	if !backupData.Valid {
		return []byte("[]"), nil // Empty backup
	}

	return []byte(backupData.String), nil
}

// Restore restores data from a backup
func (p *PostgresDB) Restore(backupData []byte) error {
	var entries []map[string]interface{}
	if err := json.Unmarshal(backupData, &entries); err != nil {
		return fmt.Errorf("failed to parse backup data: %w", err)
	}

	// Begin transaction
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Clear existing data
	if _, err := tx.Exec("TRUNCATE coordinator_data"); err != nil {
		return fmt.Errorf("failed to clear existing data: %w", err)
	}

	// Restore entries
	stmt, err := tx.Prepare(`
		INSERT INTO coordinator_data (bucket, key, value, updated_at)
		VALUES ($1, $2, decode($3, 'base64'), $4)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare restore statement: %w", err)
	}
	defer stmt.Close()

	for _, entry := range entries {
		bucket, _ := entry["bucket"].(string)
		key, _ := entry["key"].(string)
		value, _ := entry["value"].(string)
		updatedAt, _ := entry["updated_at"].(string)

		if _, err := stmt.Exec(bucket, key, value, updatedAt); err != nil {
			return fmt.Errorf("failed to restore entry %s/%s: %w", bucket, key, err)
		}
	}

	return tx.Commit()
}

// isValidInterval checks if the interval string is safe for SQL
func isValidInterval(interval string) bool {
	// Simple validation - only allow alphanumeric characters and spaces
	allowed := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	for _, char := range interval {
		if !strings.ContainsRune(allowed, char) {
			return false
		}
	}
	return len(interval) > 0 && len(interval) < 50
}