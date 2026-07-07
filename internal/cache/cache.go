package cache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nguyenanh/real-time-event-streaming/internal/models"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	redis *redis.Client
}

type RiskEntry struct {
	UserID string  `json:"user_id"`
	Score  float64 `json:"score"`
}

func New(ctx context.Context, addr, password string, db int) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Client{redis: client}, nil
}

func (c *Client) Close() error {
	return c.redis.Close()
}

func (c *Client) IncrementCounter(ctx context.Context, key string, window time.Duration) (int64, error) {
	count, err := c.redis.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if count == 1 {
		if err := c.redis.Expire(ctx, key, window).Err(); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (c *Client) AddToSet(ctx context.Context, key, member string, window time.Duration) (int64, error) {
	if err := c.redis.SAdd(ctx, key, member).Err(); err != nil {
		return 0, err
	}
	if err := c.redis.Expire(ctx, key, window).Err(); err != nil {
		return 0, err
	}
	return c.redis.SCard(ctx, key).Result()
}

func (c *Client) RecordAlert(ctx context.Context, alert models.Alert) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return err
	}
	pipe := c.redis.Pipeline()
	pipe.LPush(ctx, "fraud:alerts:recent", payload)
	pipe.LTrim(ctx, "fraud:alerts:recent", 0, 99)
	if alert.UserID != "" && alert.RiskPoints > 0 {
		pipe.ZIncrBy(ctx, "fraud:risk_scores", float64(alert.RiskPoints), alert.UserID)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (c *Client) RiskScore(ctx context.Context, userID string) (float64, error) {
	score, err := c.redis.ZScore(ctx, "fraud:risk_scores", userID).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return score, err
}

func (c *Client) TopRiskScores(ctx context.Context, limit int64) ([]RiskEntry, error) {
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	values, err := c.redis.ZRevRangeWithScores(ctx, "fraud:risk_scores", 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]RiskEntry, 0, len(values))
	for _, value := range values {
		userID, ok := value.Member.(string)
		if !ok {
			continue
		}
		entries = append(entries, RiskEntry{UserID: userID, Score: value.Score})
	}
	return entries, nil
}
