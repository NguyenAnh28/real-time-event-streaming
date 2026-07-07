package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type App struct {
	APIAddr       string
	KafkaBrokers  []string
	KafkaTopic    string
	KafkaGroupID  string
	DatabaseURL   string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	Rules         Rules
}

type Rules struct {
	PaymentFailuresThreshold int
	PaymentFailuresWindow    time.Duration
	PurchasesThreshold       int
	PurchasesWindow          time.Duration
	CardUsersThreshold       int
	CardUsersWindow          time.Duration
	IPUsersThreshold         int
	IPUsersWindow            time.Duration
	HighPurchaseAmount       float64
}

func Load() App {
	return App{
		APIAddr:       getenv("API_ADDR", ":8080"),
		KafkaBrokers:  splitCSV(getenv("KAFKA_BROKERS", "localhost:9094")),
		KafkaTopic:    getenv("KAFKA_TOPIC", "transactions"),
		KafkaGroupID:  getenv("KAFKA_GROUP_ID", "fraud-detectors"),
		DatabaseURL:   getenv("DATABASE_URL", "postgres://fraud:fraud@localhost:5432/fraud?sslmode=disable"),
		RedisAddr:     getenv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getenv("REDIS_PASSWORD", ""),
		RedisDB:       getenvInt("REDIS_DB", 0),
		Rules: Rules{
			PaymentFailuresThreshold: getenvInt("PAYMENT_FAILURES_THRESHOLD", 5),
			PaymentFailuresWindow:    getenvDuration("PAYMENT_FAILURES_WINDOW", 5*time.Minute),
			PurchasesThreshold:       getenvInt("PURCHASES_THRESHOLD", 10),
			PurchasesWindow:          getenvDuration("PURCHASES_WINDOW", time.Minute),
			CardUsersThreshold:       getenvInt("CARD_USERS_THRESHOLD", 3),
			CardUsersWindow:          getenvDuration("CARD_USERS_WINDOW", time.Hour),
			IPUsersThreshold:         getenvInt("IP_USERS_THRESHOLD", 5),
			IPUsersWindow:            getenvDuration("IP_USERS_WINDOW", 10*time.Minute),
			HighPurchaseAmount:       getenvFloat("HIGH_PURCHASE_AMOUNT", 1000),
		},
	}
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
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
