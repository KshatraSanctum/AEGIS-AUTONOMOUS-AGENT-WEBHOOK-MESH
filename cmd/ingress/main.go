package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"zerops-webhook-mesh/internal/handlers"
	"zerops-webhook-mesh/internal/storage"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

func getEnvFallback(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" && !strings.Contains(v, "{") {
			return v
		}
	}
	return ""
}

func main() {
	log.Println("🚀 INITIATING AEGIS INGRESS GATEWAY (JETSTREAM ENABLED)...")

	dbURL := getEnvFallback("DATABASE_URL", "db_connectionString")
	if dbURL != "" && !strings.Contains(dbURL, "sslmode=") {
		if strings.Contains(dbURL, "?") {
			dbURL += "&sslmode=disable"
		} else {
			dbURL += "?sslmode=disable"
		}
	}

	store, err := storage.NewPostgresStore(dbURL)
	if err != nil {
		log.Fatalf("❌ DB connection failed: %v", err)
	}
	defer store.DB.Close()

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
		log.Printf("⚠️ NATS connection deferred (%v). Attempting background reconnect...", err)
	} else {
		defer nc.Close()
		if js, err := nc.JetStream(); err == nil {
			js.AddStream(&nats.StreamConfig{
				Name:     "WEBHOOKS",
				Subjects: []string{"WEBHOOK.>"},
				Storage:  nats.FileStorage,
			})
		}
	}

	redisHost := getEnvFallback("REDIS_HOST", "cache_hostname")
	if redisHost == "" {
		redisHost = "cache"
	}
	redisPort := getEnvFallback("REDIS_PORT", "cache_port")
	if redisPort == "" {
		redisPort = "6379"
	}

	rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", redisHost, redisPort)})

	secret := os.Getenv("HMAC_SECRET_KEY")
	if secret == "" {
		secret = "default_dev_secret_key_32_bytes"
	}
	adminToken := os.Getenv("ADMIN_TOKEN")
	if adminToken == "" {
		adminToken = "super-secret-admin-123"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

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

	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			r.ParseForm()
			token := r.FormValue("token")
			http.SetCookie(w, &http.Cookie{
				Name:     "aegis_session",
				Value:    token,
				Path:     "/",
				HttpOnly: true,
				Secure:   false,
				MaxAge:   3600,
			})
			http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		}
	})

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

func adminAuth(next http.Handler, validToken string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("aegis_session")
		if err != nil || cookie.Value != validToken {
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