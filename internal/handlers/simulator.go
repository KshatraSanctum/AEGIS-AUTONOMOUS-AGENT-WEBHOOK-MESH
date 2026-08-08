package handlers

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

type SimulatorHandler struct {
	Secret string
	Port   string
}

func NewSimulatorHandler(secret, port string) *SimulatorHandler {
	return &SimulatorHandler{Secret: secret, Port: port}
}

func (h *SimulatorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Run the Doomsday Swarm simulation in the background
	go func() {
		targetURL := fmt.Sprintf("http://127.0.0.1:%s/v1/webhook", h.Port)
		agents := []string{"CrewAI-Alpha", "AutoGPT-Omega", "Devin-Beta"}
		downstreamAPIs := []string{
			"https://httpbin.org/post",       // Healthy Endpoint
			"https://httpbin.org/status/503", // Fatal Endpoint (Trips Circuit Breaker)
		}

		for i := 1; i <= 30; i++ {
			agent := agents[rand.Intn(len(agents))]
			targetAPI := downstreamAPIs[rand.Intn(len(downstreamAPIs))]

			payload := []byte(fmt.Sprintf(`{"agent_id":"%s","task":"Simulation","target_url":"%s","swarm_id":%d,"timestamp":%d}`,
				agent, targetAPI, i, time.Now().UnixNano()))

			mac := hmac.New(sha256.New, []byte(h.Secret))
			mac.Write(payload)
			signature := hex.EncodeToString(mac.Sum(nil))

			req, _ := http.NewRequest("POST", targetURL, bytes.NewBuffer(payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Webhook-Signature", signature)
			req.Header.Set("X-Source-ID", agent)

			client := &http.Client{Timeout: 3 * time.Second}
			client.Do(req) 
			time.Sleep(20 * time.Millisecond) // Slight delay to bypass rate limits gracefully
		}
	}()

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "Simulation Swarm Initiated"}`))
}