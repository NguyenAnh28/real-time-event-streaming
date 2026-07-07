package models

import (
	"time"

	"github.com/google/uuid"
)

type Alert struct {
	AlertID    string         `json:"alert_id"`
	EventID    string         `json:"event_id"`
	RuleName   string         `json:"rule_name"`
	UserID     string         `json:"user_id"`
	Severity   string         `json:"severity"`
	Reason     string         `json:"reason"`
	RiskPoints int            `json:"risk_points"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
}

type UserRisk struct {
	UserID      string    `json:"user_id"`
	RiskScore   int       `json:"risk_score"`
	LastAlertAt time.Time `json:"last_alert_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

type Stats struct {
	TotalEvents   int64     `json:"total_events"`
	TotalAlerts   int64     `json:"total_alerts"`
	EventsLastMin int64     `json:"events_last_minute"`
	AlertsLastMin int64     `json:"alerts_last_minute"`
	LastEventAt   time.Time `json:"last_event_at,omitempty"`
	LastAlertAt   time.Time `json:"last_alert_at,omitempty"`
}

func NewAlert(event Event, ruleName, severity, reason string, riskPoints int, metadata map[string]any) Alert {
	return Alert{
		AlertID:    uuid.NewString(),
		EventID:    event.EventID,
		RuleName:   ruleName,
		UserID:     event.UserID,
		Severity:   severity,
		Reason:     reason,
		RiskPoints: riskPoints,
		Metadata:   metadata,
		CreatedAt:  time.Now().UTC(),
	}
}
