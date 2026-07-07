package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nguyenanh/real-time-event-streaming/internal/cache"
	"github.com/nguyenanh/real-time-event-streaming/internal/config"
	"github.com/nguyenanh/real-time-event-streaming/internal/db"
)

func main() {
	cfg := config.Load()
	iterations := flag.Int("iterations", 1000, "number of lookup iterations")
	userID := flag.String("user", "user_hot_failures", "user id to benchmark")
	flag.Parse()

	ctx := context.Background()
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

	postgresDuration := measure(*iterations, func() error {
		_, err := store.UserRisk(ctx, *userID)
		if err == nil || err == pgx.ErrNoRows {
			return nil
		}
		return err
	})

	redisDuration := measure(*iterations, func() error {
		_, err := redis.RiskScore(ctx, *userID)
		return err
	})

	fmt.Printf("user=%s iterations=%d\n", *userID, *iterations)
	fmt.Printf("postgres_total=%s postgres_avg=%s\n", postgresDuration, postgresDuration/time.Duration(*iterations))
	fmt.Printf("redis_total=%s redis_avg=%s\n", redisDuration, redisDuration/time.Duration(*iterations))
	if redisDuration > 0 {
		improvement := (1 - float64(redisDuration)/float64(postgresDuration)) * 100
		fmt.Printf("redis_vs_postgres_improvement=%.2f%%\n", improvement)
	}
}

func measure(iterations int, fn func() error) time.Duration {
	if iterations <= 0 {
		iterations = 1
	}
	start := time.Now()
	for i := 0; i < iterations; i++ {
		if err := fn(); err != nil {
			log.Fatalf("benchmark failed: %v", err)
		}
	}
	return time.Since(start)
}
