# Real-Time Fraud Detection Pipeline Plan

## Project goal

Build a simple, production-style event streaming system that detects suspicious payment and account activity in real time.

The project should stay backend-focused. No frontend is required. The main goal is to learn how Kafka, Go, Redis, PostgreSQL, Docker, and Kubernetes fit together in a real event-driven system.

## Problem being solved

Payment and e-commerce systems produce a large number of events:

- login attempts
- payment attempts
- failed payments
- completed purchases
- checkout activity

Fraud teams need to catch suspicious behavior while it is happening, not hours later in a batch report. This project simulates that problem by generating synthetic transaction events, streaming them through Kafka, processing them with Go, using Redis for fast rolling-window checks, and storing durable records in PostgreSQL.

## Resume-facing outcome

By the end of the project, you should be able to describe yourself as having built:

- a real-time fraud detection pipeline using Go and Apache Kafka
- a rule-based detection engine for high-volume transaction events
- a Redis-backed rolling-window state layer for fast fraud checks
- PostgreSQL persistence for raw events, alerts, and risk history
- a Docker Compose local environment and Kubernetes deployment manifests

Example resume bullet:

> Built a real-time fraud detection pipeline in Go using Kafka, Redis, and PostgreSQL to process high-volume transaction events, maintain rolling-window risk signals, and emit alerts for suspicious payment and account activity.

## Core architecture

```text
producer -> Kafka -> detector consumer -> Redis
                                  |
                                  v
                              PostgreSQL
                                  |
                                  v
                                 API
```

### Core flow

1. A Go producer generates synthetic payment and account events.
2. The producer publishes those events to a Kafka topic.
3. A Go detector service consumes events from Kafka.
4. The detector runs each event through a small set of fraud rules.
5. Redis stores short-lived counters, sets, and risk scores used by the rules.
6. PostgreSQL stores raw events and generated fraud alerts.
7. A small API exposes recent alerts, user risk, and processing stats.

## What each component does

### Go

Go is the main application language.

It will power:

- the event producer
- the Kafka consumer
- the fraud detection rules
- the API service
- benchmark/load testing tools

This project should teach Go concepts such as interfaces, structs, concurrency, context cancellation, JSON handling, batching, and service shutdown.

### Kafka

Kafka is the durable event stream.

It receives high-volume events and lets the detector service process them reliably. Kafka is useful because it can buffer events, preserve ordering within partitions, support consumer groups, and allow replaying older events.

In this project:

- topic: `transactions`
- producer: writes generated events to Kafka
- consumer: reads events and runs fraud detection

Kafka is the "things that happened" log.

### Redis

Redis is the fast short-term memory layer.

It should not be treated as the main database. Its job is to answer questions that need to be checked very quickly while events are streaming in.

Examples:

- how many failed payments has this user had in the last 5 minutes?
- how many accounts used this IP recently?
- how many users used this card recently?
- which users currently have the highest risk score?
- what are the most recent alerts?

Useful Redis structures:

- counters with TTLs
- sets for unique users/cards/IPs
- sorted sets for risk score leaderboards
- lists for recent alerts

Redis is the "what is happening right now?" layer.

### PostgreSQL

PostgreSQL is the durable database.

It stores records that should survive restarts and be queryable later.

Tables can include:

- `raw_events`
- `fraud_alerts`
- `user_risk_scores`
- `event_stats`

PostgreSQL is the "what happened historically?" layer.

### Docker

Docker makes the project easy to run locally.

Docker Compose should start the local development environment:

- Kafka
- PostgreSQL
- Redis
- producer
- detector
- API

The goal is to make the project runnable with one command:

```bash
docker compose up
```

### Kubernetes

Kubernetes is for production-style deployment.

It is not required for the first working version. Add it after the Docker Compose version works.

Kubernetes manifests should show how the services would run in a cluster:

- deployments for Go services
- services for internal networking
- config maps and secrets for configuration
- readiness and liveness probes

Kubernetes is mostly resume polish for this project. The core learning should come from Kafka, Go, Redis, and PostgreSQL.

## Event model

Use synthetic e-commerce/payment events.

### Event types

Start with these:

- `login_attempt`
- `checkout_started`
- `payment_attempt`
- `payment_failed`
- `purchase_completed`

### Event schema

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "payment_failed",
  "user_id": "user_123",
  "ip_address": "203.0.113.10",
  "card_hash": "card_abc123",
  "amount": 149.99,
  "currency": "USD",
  "country": "US",
  "merchant_id": "merchant_42",
  "timestamp": "2026-07-07T12:00:00Z"
}
```

The data is generated, but it should be realistic enough to test the system under controlled load.

## Detection rules

The first version should use rule-based detection. No machine learning is needed.

Each rule should be implemented as a Go function or struct with one job:

> given an event, update/check state, and maybe return a fraud alert.

Suggested interface:

```go
type Rule interface {
    Name() string
    Evaluate(ctx context.Context, event Event) (*Alert, error)
}
```

Suggested MVP rules:

- too many payment failures from one user in 5 minutes
- too many purchases from one user in 1 minute
- same card used by many users in 1 hour
- same IP used by many accounts in 10 minutes
- unusually high purchase amount

Keep the rule logic simple, but make thresholds configurable.

Example:

```text
TOO_MANY_PAYMENT_FAILURES_THRESHOLD=5
TOO_MANY_PAYMENT_FAILURES_WINDOW=5m
HIGH_PURCHASE_AMOUNT_THRESHOLD=1000
```

## MVP scope

The MVP should be small and demoable.

### Must-have features

- synthetic event producer
- Kafka topic for transaction events
- Go detector consumer
- rule-based fraud detection engine
- Redis rolling-window state
- PostgreSQL storage for events and alerts
- API endpoints for:
  - health
  - processing stats
  - recent alerts
  - user risk
- Docker Compose for local development
- benchmark script or command

### Nice-to-have features

- dead-letter topic for invalid events
- event schema validation
- batch PostgreSQL writes
- multiple detector consumers in one consumer group
- Prometheus metrics
- Grafana dashboard
- Kubernetes manifests

## API endpoints

Keep the API tiny.

Suggested endpoints:

```text
GET /health
GET /stats
GET /alerts/recent
GET /users/{user_id}/risk
```

Optional endpoints:

```text
GET /rules
GET /events/recent
GET /leaderboard/risk
```

## Suggested database tables

### `raw_events`

Stores received transaction/account events.

Important columns:

- `event_id`
- `event_type`
- `user_id`
- `ip_address`
- `card_hash`
- `amount`
- `country`
- `timestamp`
- `created_at`

### `fraud_alerts`

Stores alerts generated by detection rules.

Important columns:

- `alert_id`
- `event_id`
- `rule_name`
- `user_id`
- `severity`
- `reason`
- `metadata`
- `created_at`

### `user_risk_scores`

Stores latest durable risk score by user.

Important columns:

- `user_id`
- `risk_score`
- `last_alert_at`
- `updated_at`

## Suggested Redis keys

```text
fraud:user:{user_id}:payment_failures
fraud:user:{user_id}:purchases
fraud:card:{card_hash}:users
fraud:ip:{ip_address}:users
fraud:risk_scores
fraud:alerts:recent
```

## Suggested repo structure

```text
cmd/
  producer/
  detector/
  api/
  benchmark/
internal/
  config/
  kafka/
  db/
  cache/
  models/
  detection/
    engine.go
    rules.go
    alerts.go
configs/
  docker-compose.yml
  k8s/
docs/
  plan.md
```

## Implementation phases

### Phase 1: Local infrastructure

Goal: get the local stack running.

Tasks:

- initialize Go module
- create Docker Compose for Kafka, PostgreSQL, and Redis
- define event and alert models
- create database migrations

Deliverable:

- local infrastructure starts successfully

### Phase 2: Event producer and Kafka

Goal: generate events and publish them to Kafka.

Tasks:

- build a Go producer
- support configurable event rate and duration
- generate realistic synthetic transaction events
- publish events to Kafka

Deliverable:

- producer can send events to the `transactions` Kafka topic

### Phase 3: Detector consumer

Goal: consume Kafka events and run rule-based detection.

Tasks:

- build a Go Kafka consumer
- parse and validate events
- implement the detection engine interface
- implement the first two fraud rules
- store alerts in PostgreSQL

Deliverable:

- suspicious events generate durable fraud alerts

### Phase 4: Redis rolling-window state

Goal: use Redis for fast real-time checks.

Tasks:

- add Redis counters, sets, and TTLs
- implement card/IP/user rolling-window rules
- maintain recent alerts and risk scores in Redis

Deliverable:

- rules can detect suspicious behavior using fast Redis state

### Phase 5: API and benchmarks

Goal: make the system easy to inspect and defend in interviews.

Tasks:

- add API endpoints for stats, alerts, and user risk
- add benchmark/load command
- measure events processed per second
- measure Redis-backed lookup latency

Deliverable:

- demoable API and measured performance numbers

### Phase 6: Deployment polish

Goal: make the project look production-ready.

Tasks:

- containerize Go services
- add Kubernetes manifests
- add health checks and readiness probes
- optionally add Prometheus metrics

Deliverable:

- production-style deployment artifacts

## Suggested weekly build plan

### Week 1

- create Docker Compose stack
- implement producer
- publish generated events to Kafka

### Week 2

- implement detector consumer
- persist raw events and alerts in PostgreSQL
- add first detection rules

### Week 3

- add Redis rolling-window state
- add risk score leaderboard and recent alerts
- benchmark detection throughput

### Week 4

- add API endpoints
- add Kubernetes manifests
- polish README and demo flow

## Demo story for interviews

You can present the project like this:

> I built a backend-only real-time fraud detection pipeline that simulates transaction traffic, streams events through Kafka, processes them with a Go consumer, uses Redis for rolling-window fraud signals, and stores durable alerts in PostgreSQL. I benchmarked the system under controlled load and used the results to reason about throughput and latency.

## Recommended first milestone

Start with this exact milestone:

- Docker Compose stack with Kafka, Redis, and PostgreSQL
- Go producer sending generated transaction events to Kafka
- Go detector consuming events from Kafka
- one fraud rule: too many failed payments from one user
- alerts written to PostgreSQL

That is enough to prove the architecture before adding more rules.
