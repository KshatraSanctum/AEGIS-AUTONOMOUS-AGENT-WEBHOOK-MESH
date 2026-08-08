package models

import "encoding/json"

type WebhookPayload struct {
	EventID   string          `json:"event_id"`
	SourceID  string          `json:"source_id"`
	Timestamp int64           `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type WebhookResponse struct {
	Status     string `json:"status"`
	EventID    string `json:"event_id"`
	Message    string `json:"message,omitempty"`
	LatencyMs  int64  `json:"latency_ms,omitempty"`
}