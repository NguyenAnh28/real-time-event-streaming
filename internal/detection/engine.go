package detection

import (
	"context"

	"github.com/nguyenanh/real-time-event-streaming/internal/models"
)

type Rule interface {
	Name() string
	Evaluate(ctx context.Context, event models.Event) (*models.Alert, error)
}

type Engine struct {
	rules []Rule
}

func NewEngine(rules ...Rule) *Engine {
	return &Engine{rules: rules}
}

func (e *Engine) Evaluate(ctx context.Context, event models.Event) ([]models.Alert, error) {
	alerts := make([]models.Alert, 0)
	for _, rule := range e.rules {
		alert, err := rule.Evaluate(ctx, event)
		if err != nil {
			return nil, err
		}
		if alert != nil {
			alerts = append(alerts, *alert)
		}
	}
	return alerts, nil
}
