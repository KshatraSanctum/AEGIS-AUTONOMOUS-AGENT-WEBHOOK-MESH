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

func getEnvFallback(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" && !strings.Contains(v, "{") {
			return v
		}
	}
	return ""
}

func main() {
	log.Println("🚀 INITIATING AEGIS WORKER: JetStream Distributed SRE Engine...")

	dbURL := getEnvFallback("DATABASE_URL", "db_connectionString")
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

	natsUser := getEnvFallback("NATS_USER", "nats_user")
	natsPass := getEnvFallback("NATS_PASSWORD", "nats_password")
	natsHost := getEnvFallback("NATS_HOST", "nats_hostname")
	if natsHost == "" {
		natsHost = "nats"
	}

	opts := []nats.Option{
		nats.Timeout(10 * time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
	}
	if natsUser != "" && natsPass != "" {
		opts = append(opts, nats.UserInfo(natsUser, natsPass))
	}

	natsURL := fmt.Sprintf("nats://%s:4222", natsHost)
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		log.Fatalf("❌ NATS failed to connect to %s: %v", natsURL, err)
	}
	defer nc.Close()

	redisHost := getEnvFallback("REDIS_HOST", "cache_hostname")
	if redisHost == "" {
		redisHost = "cache"
	}
	redisPort := getEnvFallback("REDIS_PORT", "cache_port")
	if redisPort == "" {
		redisPort = "6379"
	}

	rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", redisHost, redisPort)})

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
	if payload.SourceID == "AutoGPT-Omega" || strings.Contains(string(payload.Payload), "simulate") || payload.SourceID == "" {
		payload.EventID = fmt.Sprintf("%s-%d", payload.EventID, time.Now().UnixMilli())
		log.Printf("[🛑 SIMULATION TRIGGERED] Routing %s directly to DLQ.", payload.EventID)
		sendToDLQ(db, payload, "Simulated AI Outage - Cascading Failure Intercepted")
		return
	}

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

		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
			_, _ = io.ReadAll(resp.Body)
		}

		if resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			rdb.Del(ctx, failKey)
			log.Printf("[✅ DELIVERED] Agent: %s | Target: %s", payload.SourceID, targetAPI)
			return
		}

		if resp != nil && resp.StatusCode >= 400 && resp.StatusCode < 500 {
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