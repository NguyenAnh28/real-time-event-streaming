package db

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nguyenanh/real-time-event-streaming/internal/models"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

func (s *Store) InsertRawEvent(ctx context.Context, event models.Event) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO raw_events (
			event_id, event_type, user_id, ip_address, card_hash, amount,
			currency, country, merchant_id, event_timestamp
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (event_id) DO NOTHING
	`,
		event.EventID,
		event.EventType,
		event.UserID,
		event.IPAddress,
		nullableString(event.CardHash),
		event.Amount,
		event.Currency,
		event.Country,
		event.MerchantID,
		event.Timestamp,
	)
	return err
}

func (s *Store) InsertAlert(ctx context.Context, alert models.Alert) error {
	metadata, err := json.Marshal(alert.Metadata)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO fraud_alerts (
			alert_id, event_id, rule_name, user_id, severity, reason,
			risk_points, metadata, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (alert_id) DO NOTHING
	`,
		alert.AlertID,
		alert.EventID,
		alert.RuleName,
		alert.UserID,
		alert.Severity,
		alert.Reason,
		alert.RiskPoints,
		metadata,
		alert.CreatedAt,
	)
	return err
}

func (s *Store) AddRisk(ctx context.Context, userID string, points int, alertTime time.Time) (models.UserRisk, error) {
	var risk models.UserRisk
	err := s.pool.QueryRow(ctx, `
		INSERT INTO user_risk_scores (user_id, risk_score, last_alert_at, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (user_id) DO UPDATE
		SET risk_score = user_risk_scores.risk_score + EXCLUDED.risk_score,
			last_alert_at = EXCLUDED.last_alert_at,
			updated_at = now()
		RETURNING user_id, risk_score, last_alert_at, updated_at
	`, userID, points, alertTime).Scan(&risk.UserID, &risk.RiskScore, &risk.LastAlertAt, &risk.UpdatedAt)
	return risk, err
}

func (s *Store) RecentAlerts(ctx context.Context, limit int) ([]models.Alert, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT alert_id, event_id, rule_name, user_id, severity, reason, risk_points, metadata, created_at
		FROM fraud_alerts
		ORDER BY created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	alerts := make([]models.Alert, 0, limit)
	for rows.Next() {
		var alert models.Alert
		var metadata []byte
		if err := rows.Scan(
			&alert.AlertID,
			&alert.EventID,
			&alert.RuleName,
			&alert.UserID,
			&alert.Severity,
			&alert.Reason,
			&alert.RiskPoints,
			&metadata,
			&alert.CreatedAt,
		); err != nil {
			return nil, err
		}
		if len(metadata) > 0 {
			_ = json.Unmarshal(metadata, &alert.Metadata)
		}
		alerts = append(alerts, alert)
	}
	return alerts, rows.Err()
}

func (s *Store) UserRisk(ctx context.Context, userID string) (models.UserRisk, error) {
	var risk models.UserRisk
	err := s.pool.QueryRow(ctx, `
		SELECT user_id, risk_score, last_alert_at, updated_at
		FROM user_risk_scores
		WHERE user_id = $1
	`, userID).Scan(&risk.UserID, &risk.RiskScore, &risk.LastAlertAt, &risk.UpdatedAt)
	return risk, err
}

func (s *Store) Stats(ctx context.Context) (models.Stats, error) {
	var stats models.Stats
	err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM raw_events),
			(SELECT count(*) FROM fraud_alerts),
			(SELECT count(*) FROM raw_events WHERE created_at >= now() - interval '1 minute'),
			(SELECT count(*) FROM fraud_alerts WHERE created_at >= now() - interval '1 minute'),
			COALESCE((SELECT max(created_at) FROM raw_events), '1970-01-01'::timestamptz),
			COALESCE((SELECT max(created_at) FROM fraud_alerts), '1970-01-01'::timestamptz)
	`).Scan(
		&stats.TotalEvents,
		&stats.TotalAlerts,
		&stats.EventsLastMin,
		&stats.AlertsLastMin,
		&stats.LastEventAt,
		&stats.LastAlertAt,
	)
	return stats, err
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
