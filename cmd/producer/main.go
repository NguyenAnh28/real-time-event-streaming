package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nguyenanh/real-time-event-streaming/internal/config"
	"github.com/nguyenanh/real-time-event-streaming/internal/generator"
	internalkafka "github.com/nguyenanh/real-time-event-streaming/internal/kafka"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load()

	brokers := flag.String("brokers", strings.Join(cfg.KafkaBrokers, ","), "comma-separated Kafka brokers")
	topic := flag.String("topic", cfg.KafkaTopic, "Kafka topic")
	rate := flag.Int("rate", 1000, "events per second")
	duration := flag.Duration("duration", 30*time.Second, "how long to produce events; use 0 for forever")
	batchSize := flag.Int("batch", 100, "messages per Kafka write")
	users := flag.Int("users", 10000, "number of synthetic users")
	cards := flag.Int("cards", 5000, "number of synthetic cards")
	ips := flag.Int("ips", 2000, "number of synthetic IP pools")
	merchants := flag.Int("merchants", 100, "number of synthetic merchants")
	fraudRate := flag.Float64("fraud-rate", 0.05, "fraction of generated events that should look suspicious")
	seed := flag.Int64("seed", 0, "random seed; defaults to current time")
	flag.Parse()

	ctx := context.Background()
	brokerList := splitCSV(*brokers)
	if err := internalkafka.EnsureTopic(ctx, brokerList, *topic, 3); err != nil {
		log.Printf("topic setup skipped: %v", err)
	}

	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokerList...),
		Topic:                  *topic,
		Balancer:               &kafkago.Hash{},
		BatchSize:              *batchSize,
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           kafkago.RequireOne,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	gen := generator.New(generator.Options{
		Users:     *users,
		Cards:     *cards,
		IPs:       *ips,
		Merchants: *merchants,
		FraudRate: *fraudRate,
		Seed:      *seed,
	})

	runCtx := ctx
	var cancel context.CancelFunc
	if *duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	if *batchSize <= 0 {
		*batchSize = 1
	}
	if *rate > 0 && *batchSize > *rate {
		*batchSize = *rate
	}

	var sent atomic.Int64
	done := make(chan struct{})
	go reportProgress(done, &sent)

	log.Printf("producing events brokers=%s topic=%s rate=%d/s duration=%s", strings.Join(brokerList, ","), *topic, *rate, duration.String())

	interval := time.Duration(0)
	if *rate > 0 {
		interval = time.Second * time.Duration(*batchSize) / time.Duration(*rate)
		if interval <= 0 {
			interval = time.Millisecond
		}
	}

	for {
		select {
		case <-runCtx.Done():
			close(done)
			log.Printf("producer stopped sent=%d", sent.Load())
			return
		default:
		}

		messages := make([]kafkago.Message, 0, *batchSize)
		for i := 0; i < *batchSize; i++ {
			event := gen.Next()
			payload, err := json.Marshal(event)
			if err != nil {
				log.Fatalf("marshal event: %v", err)
			}
			messages = append(messages, kafkago.Message{
				Key:   []byte(event.UserID),
				Value: payload,
				Time:  event.Timestamp,
			})
		}

		if err := writer.WriteMessages(runCtx, messages...); err != nil {
			if runCtx.Err() != nil {
				continue
			}
			log.Fatalf("write messages: %v", err)
		}
		sent.Add(int64(len(messages)))

		if interval > 0 {
			select {
			case <-runCtx.Done():
			case <-time.After(interval):
			}
		}
	}
}

func reportProgress(done <-chan struct{}, sent *atomic.Int64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var last int64
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			current := sent.Load()
			log.Printf("producer throughput=%d events/sec total=%d", current-last, current)
			last = current
		}
	}
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			items = append(items, part)
		}
	}
	return items
}
