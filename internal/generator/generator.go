package generator

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/nguyenanh/real-time-event-streaming/internal/models"
)

type Generator struct {
	rand      *rand.Rand
	users     int
	cards     int
	ips       int
	merchants int
	fraudRate float64
}

type Options struct {
	Users     int
	Cards     int
	IPs       int
	Merchants int
	FraudRate float64
	Seed      int64
}

func New(options Options) *Generator {
	if options.Users <= 0 {
		options.Users = 10000
	}
	if options.Cards <= 0 {
		options.Cards = 5000
	}
	if options.IPs <= 0 {
		options.IPs = 2000
	}
	if options.Merchants <= 0 {
		options.Merchants = 100
	}
	if options.FraudRate < 0 {
		options.FraudRate = 0
	}
	if options.FraudRate > 1 {
		options.FraudRate = 1
	}
	if options.Seed == 0 {
		options.Seed = time.Now().UnixNano()
	}
	return &Generator{
		rand:      rand.New(rand.NewSource(options.Seed)),
		users:     options.Users,
		cards:     options.Cards,
		ips:       options.IPs,
		merchants: options.Merchants,
		fraudRate: options.FraudRate,
	}
}

func (g *Generator) Next() models.Event {
	if g.rand.Float64() < g.fraudRate {
		return g.suspiciousEvent()
	}
	return g.normalEvent()
}

func (g *Generator) normalEvent() models.Event {
	eventType := weightedEventType(g.rand)
	amount := 0.0
	if eventType == models.EventPaymentAttempt || eventType == models.EventPaymentFailed || eventType == models.EventPurchaseCompleted {
		amount = roundMoney(5 + g.rand.Float64()*245)
	}
	return models.Event{
		EventID:    models.NewEventID(),
		EventType:  eventType,
		UserID:     fmt.Sprintf("user_%d", 1+g.rand.Intn(g.users)),
		IPAddress:  fmt.Sprintf("203.0.%d.%d", g.rand.Intn(255), 1+g.rand.Intn(254)),
		CardHash:   fmt.Sprintf("card_%d", 1+g.rand.Intn(g.cards)),
		Amount:     amount,
		Currency:   "USD",
		Country:    randomCountry(g.rand),
		MerchantID: fmt.Sprintf("merchant_%d", 1+g.rand.Intn(g.merchants)),
		Timestamp:  time.Now().UTC(),
	}
}

func (g *Generator) suspiciousEvent() models.Event {
	now := time.Now().UTC()
	switch g.rand.Intn(5) {
	case 0:
		return models.Event{
			EventID:    models.NewEventID(),
			EventType:  models.EventPaymentFailed,
			UserID:     "user_hot_failures",
			IPAddress:  "198.51.100.10",
			CardHash:   "card_hot_failures",
			Amount:     roundMoney(25 + g.rand.Float64()*300),
			Currency:   "USD",
			Country:    "US",
			MerchantID: "merchant_1",
			Timestamp:  now,
		}
	case 1:
		return models.Event{
			EventID:    models.NewEventID(),
			EventType:  models.EventPurchaseCompleted,
			UserID:     "user_fast_buyer",
			IPAddress:  "198.51.100.11",
			CardHash:   "card_fast_buyer",
			Amount:     roundMoney(20 + g.rand.Float64()*80),
			Currency:   "USD",
			Country:    "US",
			MerchantID: "merchant_2",
			Timestamp:  now,
		}
	case 2:
		return models.Event{
			EventID:    models.NewEventID(),
			EventType:  models.EventPaymentAttempt,
			UserID:     fmt.Sprintf("user_shared_card_%d", 1+g.rand.Intn(20)),
			IPAddress:  fmt.Sprintf("198.51.100.%d", 20+g.rand.Intn(20)),
			CardHash:   "card_shared_many_users",
			Amount:     roundMoney(30 + g.rand.Float64()*250),
			Currency:   "USD",
			Country:    "US",
			MerchantID: "merchant_3",
			Timestamp:  now,
		}
	case 3:
		return models.Event{
			EventID:    models.NewEventID(),
			EventType:  models.EventLoginAttempt,
			UserID:     fmt.Sprintf("user_shared_ip_%d", 1+g.rand.Intn(30)),
			IPAddress:  "198.51.100.99",
			CardHash:   "",
			Amount:     0,
			Currency:   "USD",
			Country:    randomCountry(g.rand),
			MerchantID: "merchant_4",
			Timestamp:  now,
		}
	default:
		return models.Event{
			EventID:    models.NewEventID(),
			EventType:  models.EventPurchaseCompleted,
			UserID:     fmt.Sprintf("user_big_spender_%d", 1+g.rand.Intn(5)),
			IPAddress:  "198.51.100.50",
			CardHash:   fmt.Sprintf("card_big_%d", 1+g.rand.Intn(5)),
			Amount:     roundMoney(1000 + g.rand.Float64()*4000),
			Currency:   "USD",
			Country:    "US",
			MerchantID: "merchant_5",
			Timestamp:  now,
		}
	}
}

func weightedEventType(r *rand.Rand) string {
	n := r.Intn(100)
	switch {
	case n < 20:
		return models.EventLoginAttempt
	case n < 35:
		return models.EventCheckoutStarted
	case n < 65:
		return models.EventPaymentAttempt
	case n < 85:
		return models.EventPaymentFailed
	default:
		return models.EventPurchaseCompleted
	}
}

func randomCountry(r *rand.Rand) string {
	countries := []string{"US", "CA", "GB", "VN", "DE", "FR", "JP", "BR"}
	return countries[r.Intn(len(countries))]
}

func roundMoney(value float64) float64 {
	return float64(int(value*100)) / 100
}
