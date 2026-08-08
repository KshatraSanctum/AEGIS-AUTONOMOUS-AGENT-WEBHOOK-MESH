package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"zerops-webhook-mesh/internal/storage"

	"github.com/nats-io/nats.go"
)

type ReplayHandler struct {
	Store    *storage.PostgresStore
	NatsConn *nats.Conn
}

func NewReplayHandler(store *storage.PostgresStore, natsConn *nats.Conn) *ReplayHandler {
	return &ReplayHandler{Store: store, NatsConn: natsConn}
}

func (h *ReplayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	eventID := r.URL.Query().Get("event_id")
	if eventID == "" {
		http.Error(w, "Missing event_id parameter", http.StatusBadRequest)
		return
	}

	// 1. Fetch the payload from DLQ
	var payload string
	var sourceID string
	err := h.Store.DB.QueryRow("SELECT source_id, payload FROM webhook_dlq WHERE event_id = $1", eventID).Scan(&sourceID, &payload)
	if err != nil {
		slog.Error("Failed to find event in DLQ for replay", "event_id", eventID, "error", err)
		http.Error(w, "Event not found in DLQ", http.StatusNotFound)
		return
	}

	// 2. Re-publish to NATS message broker if connected
	if h.NatsConn != nil {
		err = h.NatsConn.Publish("webhook.incoming", []byte(payload))
		if err != nil {
			slog.Error("Failed to re-publish payload to NATS", "error", err)
			http.Error(w, "Failed to re-queue message", http.StatusInternalServerError)
			return
		}
	}

	// 3. Remove from DLQ after successful re-queue
	_, _ = h.Store.DB.Exec("DELETE FROM webhook_dlq WHERE event_id = $1", eventID)

	slog.Info("Successfully replayed failed webhook event", "event_id", eventID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "success",
		"message":  "Webhook re-queued successfully",
		"event_id": eventID,
	})
}