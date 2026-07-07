package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nguyenanh/real-time-event-streaming/internal/cache"
	"github.com/nguyenanh/real-time-event-streaming/internal/config"
	"github.com/nguyenanh/real-time-event-streaming/internal/db"
	"github.com/nguyenanh/real-time-event-streaming/internal/detection"
	internalkafka "github.com/nguyenanh/real-time-event-streaming/internal/kafka"
	"github.com/nguyenanh/real-time-event-streaming/internal/models"
	kafkago "github.com/segmentio/kafka-go"
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

	if err := internalkafka.EnsureTopic(ctx, cfg.KafkaBrokers, cfg.KafkaTopic, 3); err != nil {
		log.Printf("topic setup skipped: %v", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.KafkaGroupID,
		MinBytes:       1e3,
		MaxBytes:       10e6,
		MaxWait:        250 * time.Millisecond,
		CommitInterval: 0,
	})
	defer reader.Close()

	engine := detection.NewEngine(detection.NewDefaultRules(redis, cfg.Rules)...)

	var processed atomic.Int64
	var alertCount atomic.Int64
	done := make(chan struct{})
	go reportProgress(done, &processed, &alertCount)
	defer close(done)

	log.Printf("detector consuming brokers=%v topic=%s group=%s", cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				log.Printf("detector stopped processed=%d alerts=%d", processed.Load(), alertCount.Load())
				return
			}
			log.Printf("fetch message: %v", err)
			continue
		}

		alerts, err := processMessage(ctx, store, redis, engine, message)
		if err != nil {
			log.Printf("process message offset=%d: %v", message.Offset, err)
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("commit offset=%d: %v", message.Offset, err)
			continue
		}
		processed.Add(1)
		alertCount.Add(int64(alerts))
	}
}

func processMessage(ctx context.Context, store *db.Store, redis *cache.Client, engine *detection.Engine, message kafkago.Message) (int, error) {
	var event models.Event
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return 0, err
	}
	if err := event.Validate(); err != nil {
		return 0, err
	}
	if err := store.InsertRawEvent(ctx, event); err != nil {
		return 0, err
	}

	alerts, err := engine.Evaluate(ctx, event)
	if err != nil {
		return 0, err
	}
	for _, alert := range alerts {
		if err := store.InsertAlert(ctx, alert); err != nil {
			return 0, err
		}
		if _, err := store.AddRisk(ctx, alert.UserID, alert.RiskPoints, alert.CreatedAt); err != nil {
			return 0, err
		}
		if err := redis.RecordAlert(ctx, alert); err != nil {
			return 0, err
		}
	}
	return len(alerts), nil
}

func reportProgress(done <-chan struct{}, processed, alerts *atomic.Int64) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastProcessed int64
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			current := processed.Load()
			log.Printf("detector throughput=%d events/sec total=%d alerts=%d", (current-lastProcessed)/5, current, alerts.Load())
			lastProcessed = current
		}
	}
}
