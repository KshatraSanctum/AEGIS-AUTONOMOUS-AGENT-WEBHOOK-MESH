package storage

import (
	"database/sql"
	_ "embed" // Required for embedding files
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

//go:embed schema.sql
var schemaSQL string

type PostgresStore struct {
	DB *sql.DB
}

func NewPostgresStore(dbURL string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	store := &PostgresStore{DB: db}
	
	// Auto-Migrate Database on Boot using embedded schema
	if err := store.Migrate(); err != nil {
		log.Printf("⚠️ Warning: Auto-migration failed or tables already exist: %v", err)
	} else {
		log.Println("[Storage] Connected to PostgreSQL. Schema and DLQ verified.")
	}

	return store, nil
}

func (s *PostgresStore) Migrate() error {
	_, err := s.DB.Exec(schemaSQL)
	return err
}

func (s *PostgresStore) InsertEvent(eventID, sourceID, status string) error {
	query := "INSERT INTO webhook_events (event_id, source_id, status) VALUES ($1, $2, $3)"
	_, err := s.DB.Exec(query, eventID, sourceID, status)
	return err
}

func (s *PostgresStore) InsertDLQ(eventID, sourceID, payload, errorReason string) error {
	query := "INSERT INTO webhook_dlq (event_id, source_id, payload, error_reason) VALUES ($1, $2, $3, $4)"
	_, err := s.DB.Exec(query, eventID, sourceID, payload, errorReason)
	return err
}