package detection

import (
	"context"
	"fmt"
	"time"

	"github.com/nguyenanh/real-time-event-streaming/internal/cache"
	"github.com/nguyenanh/real-time-event-streaming/internal/config"
	"github.com/nguyenanh/real-time-event-streaming/internal/models"
)

func NewDefaultRules(redis *cache.Client, cfg config.Rules) []Rule {
	return []Rule{
		&TooManyPaymentFailuresRule{redis: redis, threshold: cfg.PaymentFailuresThreshold, window: cfg.PaymentFailuresWindow},
		&TooManyPurchasesRule{redis: redis, threshold: cfg.PurchasesThreshold, window: cfg.PurchasesWindow},
		&SameCardManyUsersRule{redis: redis, threshold: cfg.CardUsersThreshold, window: cfg.CardUsersWindow},
		&SameIPManyUsersRule{redis: redis, threshold: cfg.IPUsersThreshold, window: cfg.IPUsersWindow},
		&HighPurchaseAmountRule{threshold: cfg.HighPurchaseAmount},
	}
}

type TooManyPaymentFailuresRule struct {
	redis     *cache.Client
	threshold int
	window    time.Duration
}

func (r *TooManyPaymentFailuresRule) Name() string {
	return "too_many_payment_failures"
}

func (r *TooManyPaymentFailuresRule) Evaluate(ctx context.Context, event models.Event) (*models.Alert, error) {
	if event.EventType != models.EventPaymentFailed {
		return nil, nil
	}
	key := fmt.Sprintf("fraud:user:%s:payment_failures", event.UserID)
	count, err := r.redis.IncrementCounter(ctx, key, r.window)
	if err != nil {
		return nil, err
	}
	if count < int64(r.threshold) {
		return nil, nil
	}
	return newAlert(event, r.Name(), "medium", "too many payment failures from one user", 20, map[string]any{
		"count":     count,
		"threshold": r.threshold,
		"window":    r.window.String(),
	}), nil
}

type TooManyPurchasesRule struct {
	redis     *cache.Client
	threshold int
	window    time.Duration
}

func (r *TooManyPurchasesRule) Name() string {
	return "too_many_purchases"
}

func (r *TooManyPurchasesRule) Evaluate(ctx context.Context, event models.Event) (*models.Alert, error) {
	if event.EventType != models.EventPurchaseCompleted {
		return nil, nil
	}
	key := fmt.Sprintf("fraud:user:%s:purchases", event.UserID)
	count, err := r.redis.IncrementCounter(ctx, key, r.window)
	if err != nil {
		return nil, err
	}
	if count < int64(r.threshold) {
		return nil, nil
	}
	return newAlert(event, r.Name(), "medium", "too many purchases from one user", 15, map[string]any{
		"count":     count,
		"threshold": r.threshold,
		"window":    r.window.String(),
	}), nil
}

type SameCardManyUsersRule struct {
	redis     *cache.Client
	threshold int
	window    time.Duration
}

func (r *SameCardManyUsersRule) Name() string {
	return "same_card_many_users"
}

func (r *SameCardManyUsersRule) Evaluate(ctx context.Context, event models.Event) (*models.Alert, error) {
	if event.CardHash == "" {
		return nil, nil
	}
	switch event.EventType {
	case models.EventPaymentAttempt, models.EventPaymentFailed, models.EventPurchaseCompleted:
	default:
		return nil, nil
	}
	key := fmt.Sprintf("fraud:card:%s:users", event.CardHash)
	count, err := r.redis.AddToSet(ctx, key, event.UserID, r.window)
	if err != nil {
		return nil, err
	}
	if count < int64(r.threshold) {
		return nil, nil
	}
	return newAlert(event, r.Name(), "high", "same card used by many users", 35, map[string]any{
		"card_hash":  event.CardHash,
		"user_count": count,
		"threshold":  r.threshold,
		"window":     r.window.String(),
	}), nil
}

type SameIPManyUsersRule struct {
	redis     *cache.Client
	threshold int
	window    time.Duration
}

func (r *SameIPManyUsersRule) Name() string {
	return "same_ip_many_users"
}

func (r *SameIPManyUsersRule) Evaluate(ctx context.Context, event models.Event) (*models.Alert, error) {
	if event.IPAddress == "" || event.UserID == "" {
		return nil, nil
	}
	key := fmt.Sprintf("fraud:ip:%s:users", event.IPAddress)
	count, err := r.redis.AddToSet(ctx, key, event.UserID, r.window)
	if err != nil {
		return nil, err
	}
	if count < int64(r.threshold) {
		return nil, nil
	}
	return newAlert(event, r.Name(), "high", "same IP used by many accounts", 30, map[string]any{
		"ip_address": event.IPAddress,
		"user_count": count,
		"threshold":  r.threshold,
		"window":     r.window.String(),
	}), nil
}

type HighPurchaseAmountRule struct {
	threshold float64
}

func (r *HighPurchaseAmountRule) Name() string {
	return "high_purchase_amount"
}

func (r *HighPurchaseAmountRule) Evaluate(_ context.Context, event models.Event) (*models.Alert, error) {
	if event.EventType != models.EventPurchaseCompleted && event.EventType != models.EventPaymentAttempt {
		return nil, nil
	}
	if event.Amount < r.threshold {
		return nil, nil
	}
	return newAlert(event, r.Name(), "high", "purchase amount is unusually high", 25, map[string]any{
		"amount":    event.Amount,
		"threshold": r.threshold,
		"currency":  event.Currency,
	}), nil
}

func newAlert(event models.Event, ruleName, severity, reason string, riskPoints int, metadata map[string]any) *models.Alert {
	alert := models.NewAlert(event, ruleName, severity, reason, riskPoints, metadata)
	return &alert
}
