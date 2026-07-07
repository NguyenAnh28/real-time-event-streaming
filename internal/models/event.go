package models

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	EventLoginAttempt      = "login_attempt"
	EventCheckoutStarted   = "checkout_started"
	EventPaymentAttempt    = "payment_attempt"
	EventPaymentFailed     = "payment_failed"
	EventPurchaseCompleted = "purchase_completed"
)

type Event struct {
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	UserID     string    `json:"user_id"`
	IPAddress  string    `json:"ip_address"`
	CardHash   string    `json:"card_hash,omitempty"`
	Amount     float64   `json:"amount,omitempty"`
	Currency   string    `json:"currency"`
	Country    string    `json:"country"`
	MerchantID string    `json:"merchant_id"`
	Timestamp  time.Time `json:"timestamp"`
}

func NewEventID() string {
	return uuid.NewString()
}

func (e Event) Validate() error {
	if e.EventID == "" {
		return errors.New("event_id is required")
	}
	if e.EventType == "" {
		return errors.New("event_type is required")
	}
	if e.UserID == "" {
		return errors.New("user_id is required")
	}
	if e.IPAddress == "" {
		return errors.New("ip_address is required")
	}
	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	switch e.EventType {
	case EventLoginAttempt, EventCheckoutStarted, EventPaymentAttempt, EventPaymentFailed, EventPurchaseCompleted:
		return nil
	default:
		return errors.New("unknown event_type")
	}
}
