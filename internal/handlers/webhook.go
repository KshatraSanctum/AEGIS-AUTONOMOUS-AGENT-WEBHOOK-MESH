package handlers

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"zerops-webhook-mesh/internal/models"
	"zerops-webhook-mesh/internal/storage"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

type WebhookHandler struct {
	Store    *storage.PostgresStore
	NatsConn *nats.Conn
	Redis    *redis.Client
	Secret   []byte
}

var ctx = context.Background()

func NewWebhookHandler(store *storage.PostgresStore, nc *nats.Conn, rdb *redis.Client, secret string) *WebhookHandler {
	return &WebhookHandler{
		Store:    store,
		NatsConn: nc,
		Redis:    rdb,
		Secret:   []byte(secret),
	}
}

func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid HTTP Method", http.StatusMethodNotAllowed)
		return
	}

	clientIP := r.RemoteAddr
	if !h.allowRequestGlobal(clientIP) {
		http.Error(w, "Too many requests", http.StatusTooManyRequests)
		return
	}

	clientSig := r.Header.Get("X-Webhook-Signature")
	if clientSig == "" {
		http.Error(w, "Missing signature header", http.StatusUnauthorized)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil || len(bodyBytes) == 0 {
		http.Error(w, "Empty body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if !h.verifySignature(bodyBytes, clientSig) {
		http.Error(w, "Invalid cryptographic signature", http.StatusForbidden)
		return
	}

	eventID := r.Header.Get("X-Aegis-Event-ID") // AI agents can pass their own unique ID
	sourceID := r.Header.Get("X-Source-ID")
	if eventID == "" {
		eventID = "EVT-" + hex.EncodeToString(bodyBytes)[:12] // Fallback ID generation
	}

	// In test mode, h.Store will be nil. We bypass DB logic for purely testing the HTTP/Security layer.
	if h.Store != nil {
		err = h.Store.InsertEvent(eventID, sourceID, "RECEIVED")
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(models.WebhookResponse{
				Status:  "duplicate",
				EventID: eventID,
				Message: "Event already processed or queued (Idempotency Guard)",
			})
			return
		}
	}

	payload := models.WebhookPayload{
		EventID:   eventID,
		SourceID:  sourceID,
		Timestamp: time.Now().UnixNano(),
		Payload:   bodyBytes,
	}
	bytePayload, _ := json.Marshal(payload)

	// Publish to NATS JetStream for guaranteed persistence
	if h.NatsConn != nil && h.NatsConn.IsConnected() {
		js, jsErr := h.NatsConn.JetStream()
		if jsErr == nil {
			_, err = js.Publish("WEBHOOK.events", bytePayload)
			if err != nil && h.Store != nil {
				// Fixed: Cast bytePayload to string
				h.Store.InsertDLQ(eventID, sourceID, string(bytePayload), "Failed to publish to JetStream")
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(models.WebhookResponse{
		Status:  "accepted",
		EventID: eventID,
		Message: "Payload secured in Aegis Mesh",
	})
}

func (h *WebhookHandler) verifySignature(body []byte, clientSig string) bool {
	mac := hmac.New(sha256.New, h.Secret)
	mac.Write(body)
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	return subtle.ConstantTimeCompare([]byte(clientSig), []byte(expectedSig)) == 1
}

// Global Distributed Rate Limiter using Redis (50 req/sec per IP)
func (h *WebhookHandler) allowRequestGlobal(ip string) bool {
	if h.Redis == nil {
		return true // Fallback if Redis is down or in Test Mode
	}
	key := "rate_limit:" + ip

	// Increment request count
	count, err := h.Redis.Incr(ctx, key).Result()
	if err != nil {
		return true
	}

	// Set expiry window on first request
	if count == 1 {
		h.Redis.Expire(ctx, key, time.Second)
	}

	return count <= 50
}