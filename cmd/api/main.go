package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nguyenanh/real-time-event-streaming/internal/cache"
	"github.com/nguyenanh/real-time-event-streaming/internal/config"
	"github.com/nguyenanh/real-time-event-streaming/internal/db"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := db.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}

	redis, err := cache.New(ctx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("connect redis: %v", err)
	}
	defer redis.Close()

	server := &apiServer{store: store, redis: redis}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.health)
	mux.HandleFunc("GET /stats", server.stats)
	mux.HandleFunc("GET /alerts/recent", server.recentAlerts)
	mux.HandleFunc("GET /users/", server.userRisk)
	mux.HandleFunc("GET /leaderboard/risk", server.riskLeaderboard)

	httpServer := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("api listening addr=%s", cfg.APIAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("api server: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("api shutdown: %v", err)
	}
}

type apiServer struct {
	store *db.Store
	redis *cache.Client
}

func (s *apiServer) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *apiServer) stats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *apiServer) recentAlerts(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 20)
	alerts, err := s.store.RecentAlerts(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, alerts)
}

func (s *apiServer) userRisk(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/users/") || !strings.HasSuffix(r.URL.Path, "/risk") {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "expected /users/{user_id}/risk"})
		return
	}
	userID := strings.TrimPrefix(r.URL.Path, "/users/")
	userID = strings.TrimSuffix(userID, "/risk")
	userID = strings.Trim(userID, "/")
	if userID == "" || strings.Contains(userID, "/") {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "expected /users/{user_id}/risk"})
		return
	}

	dbRisk, err := s.store.UserRisk(r.Context(), userID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if errors.Is(err, pgx.ErrNoRows) {
		dbRisk.UserID = userID
	}

	redisRisk, err := s.redis.RiskScore(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"user_id":          userID,
		"postgres_risk":    dbRisk.RiskScore,
		"redis_risk":       redisRisk,
		"last_alert_at":    dbRisk.LastAlertAt,
		"postgres_updated": dbRisk.UpdatedAt,
	})
}

func (s *apiServer) riskLeaderboard(w http.ResponseWriter, r *http.Request) {
	limit := int64(queryInt(r, "limit", 10))
	entries, err := s.redis.TopRiskScores(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func queryInt(r *http.Request, key string, fallback int) int {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}
