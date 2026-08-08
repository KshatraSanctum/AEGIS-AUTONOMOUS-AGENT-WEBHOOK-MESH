package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

type WebhookPayload struct {
	EventID   string `json:"event_id"`
	SourceID  string `json:"source_id"`
	Timestamp int64  `json:"timestamp"`
	Payload   []byte `json:"payload"`
}

type TargetExtractor struct {
	TargetURL string `json:"target_url"`
}

const (
	maxRetries     = 3
	baseBackoff    = 2 * time.Second
	failThreshold  = 5
	circuitTimeout = 60 * time.Second
)

var ctx = context.Background()

func main() {
	log.Println("🚀 INITIATING AEGIS WORKER: JetStream Distributed SRE Engine...")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/webhook_mesh?sslmode=disable"
	} else if !strings.Contains(dbURL, "sslmode=") {
		if strings.Contains(dbURL, "?") {
			dbURL += "&sslmode=disable"
		} else {
			dbURL += "?sslmode=disable"
		}
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("❌ DB failed: %v", err)
	}
	defer db.Close()

	// FIXED: Safe Credential Handling via NATS Options
	natsHost := os.Getenv("nats_hostname")
	if natsHost == "" { natsHost = os.Getenv("NATS_HOSTNAME") }
	if natsHost == "" { natsHost = "nats" }
	
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" || strings.Contains(natsURL, "{") || strings.Contains(natsURL, "}") {
		natsURL = "nats://" + natsHost + ":4222"
	}
	
	natsUser := os.Getenv("nats_user")
	if natsUser == "" { natsUser = os.Getenv("NATS_USER") }
	
	natsPass := os.Getenv("nats_password")
	if natsPass == "" { natsPass = os.Getenv("NATS_PASSWORD") }

	var opts []nats.Option
	if natsUser != "" && natsPass != "" {
		opts = append(opts, nats.UserInfo(natsUser, natsPass))
	}
	
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		log.Fatalf("❌ NATS failed: %v", err)
	}
	defer nc.Close()

	valkeyURL := os.Getenv("REDIS_URL")
	if valkeyURL == "" || strings.Contains(valkeyURL, "{") || strings.Contains(valkeyURL, "$") {
		valkeyURL = "cache:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: valkeyURL})

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("❌ JetStream initialization failed: %v", err)
	}

	_, err = js.Subscribe("WEBHOOK.events", func(m *nats.Msg) {
		var payload WebhookPayload
		if err := json.Unmarshal(m.Data, &payload); err != nil {
			m.Ack()
			return
		}
		processWebhookWithTriage(db, rdb, payload)
		m.Ack()
	}, nats.Durable("worker-group"), nats.ManualAck())

	log.Println("🎧 Aegis Worker listening for AI Agent streams via JetStream...")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
}

func processWebhookWithTriage(db *sql.DB, rdb *redis.Client, payload WebhookPayload) {
	var extractor TargetExtractor
	if err := json.Unmarshal(payload.Payload, &extractor); err != nil || extractor.TargetURL == "" {
		sendToDLQ(db, payload, "Fatal: Missing dynamic target_url in payload")
		return
	}
	targetAPI := extractor.TargetURL
	cbKey := "breaker:" + targetAPI
	failKey := "fails:" + targetAPI

	if val, _ := rdb.Get(ctx, cbKey).Result(); val == "OPEN" {
		log.Printf("[🛑 CIRCUIT TRIPPED] Traffic to %s blocked! Routing to DLQ.", targetAPI)
		sendToDLQ(db, payload, "Circuit Breaker OPEN - Downstream API Protected")
		return
	}

	client := &http.Client{Timeout: 5 * time.Second}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, _ := http.NewRequest("POST", targetAPI, bytes.NewBuffer(payload.Payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Aegis-Event-ID", payload.EventID)

		resp, err := client.Do(req)

		if err != nil || resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			fails, _ := rdb.Incr(ctx, failKey).Result()
			rdb.Expire(ctx, failKey, 30*time.Second)

			if fails >= failThreshold {
				rdb.Set(ctx, cbKey, "OPEN", circuitTimeout)
				sendToDLQ(db, payload, fmt.Sprintf("Upstream Outage (HTTP %d) - Circuit Tripped", getStatus(resp)))
				return
			}

			if attempt == maxRetries {
				sendToDLQ(db, payload, fmt.Sprintf("Max Retries Exhausted (HTTP %d)", getStatus(resp)))
				return
			}

			time.Sleep(baseBackoff * time.Duration(1<<attempt))
			continue
		}

		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			rdb.Del(ctx, failKey)
			log.Printf("[✅ DELIVERED] Agent: %s | Target: %s", payload.SourceID, targetAPI)
			return
		}

		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			sendToDLQ(db, payload, fmt.Sprintf("Agent Data Hallucination (HTTP %d)", resp.StatusCode))
			return
		}
	}
}

func getStatus(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func sendToDLQ(db *sql.DB, payload WebhookPayload, reason string) {
	query := `INSERT INTO webhook_dlq (event_id, source_id, payload, error_reason, failed_at) 
		VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP) ON CONFLICT (event_id) DO NOTHING;`
	db.Exec(query, payload.EventID, payload.SourceID, string(payload.Payload), reason)
}