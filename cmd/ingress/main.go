package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"zerops-webhook-mesh/internal/handlers"
	"zerops-webhook-mesh/internal/storage"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

func main() {
	log.Println("🚀 INITIATING AEGIS INGRESS GATEWAY (JETSTREAM ENABLED)...")

	// 1. Database Connection (Strict Mode)
	dbURL := os.Getenv("DATABASE_URL")
	store, err := storage.NewPostgresStore(dbURL)
	if err != nil {
		log.Fatalf("❌ DB connection failed. Awaiting Zerops restart... %v", err)
	}
	defer store.DB.Close()

	// 2. NATS JetStream Connection (Strict Mode)
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("❌ NATS connection failed. Awaiting Zerops restart... %v", err)
	}
	defer nc.Close()

	// Initialize JetStream Stream
	js, _ := nc.JetStream()
	js.AddStream(&nats.StreamConfig{
		Name:     "WEBHOOKS",
		Subjects: []string{"WEBHOOK.>"},
		Storage:  nats.FileStorage,
	})

	// 3. Redis Connection (For Global Rate Limiting)
	valkeyURL := os.Getenv("REDIS_URL")
	if valkeyURL == "" {
		valkeyURL = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: valkeyURL})

	secret := os.Getenv("HMAC_SECRET_KEY")
	if secret == "" {
		secret = "default_dev_secret_key_32_bytes"
	}
	adminToken := os.Getenv("ADMIN_TOKEN")
	if adminToken == "" {
		adminToken = "super-secret-admin-123"
	}

	// Fetch port here so it can be passed to SimulatorHandler securely
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// 4. Initialize Handlers
	webhookHandler := handlers.NewWebhookHandler(store, nc, rdb, secret)
	dashboardHandler := handlers.NewDashboardHandler(store)
	replayHandler := handlers.NewReplayHandler(store, nc)
	simulatorHandler := handlers.NewSimulatorHandler(secret, port)

	mux := http.NewServeMux()
	
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	
	mux.Handle("/v1/webhook", webhookHandler)

	// Add the login handler for the Secure Cookie session
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			r.ParseForm()
			token := r.FormValue("token")
			http.SetCookie(w, &http.Cookie{
				Name:     "aegis_session",
				Value:    token,
				Path:     "/",
				HttpOnly: true,  // Prevents XSS attacks
				Secure:   false, // Set to true if deploying with HTTPS
				MaxAge:   3600,
			})
			http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		}
	})

	// Apply Zero-Trust Security to Control Plane
	mux.Handle("/dashboard", adminAuth(dashboardHandler, adminToken))
	mux.Handle("/v1/replay", adminAuth(replayHandler, adminToken))
	mux.Handle("/v1/simulate", adminAuth(simulatorHandler, adminToken))

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("🛡️ Aegis Gateway Listening on Port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("🛑 Gracefully shutting down...")
}

// adminAuth enforces Zero-Trust security on the Control Plane via Secure Cookies
func adminAuth(next http.Handler, validToken string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("aegis_session")
		if err != nil || cookie.Value != validToken {
			// If unauthorized, show a professional 401/Login prompt instead of raw text
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`
				<body style="background:#09090b;color:#fafafa;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;">
					<div style="border:1px solid #27272a;padding:2rem;border-radius:8px;text-align:center;">
						<h2 style="margin-top:0;">Secure Control Plane</h2>
						<p style="color:#a1a1aa;font-size:14px;">Enter Enterprise Admin Token</p>
						<form method="POST" action="/login">
							<input type="password" name="token" style="padding:8px;border-radius:4px;border:1px solid #3f3f46;background:#18181b;color:white;width:100%;margin-bottom:1rem;" autofocus>
							<button type="submit" style="background:#fafafa;color:#09090b;padding:8px;width:100%;border:none;border-radius:4px;font-weight:bold;cursor:pointer;">Authenticate</button>
						</form>
					</div>
				</body>`))
			return
		}
		next.ServeHTTP(w, r)
	})
}