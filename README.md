# Real-Time Fraud Detection Pipeline

Backend-only event streaming project built with Go, Apache Kafka, Redis, PostgreSQL, Docker, and Kubernetes.

The project simulates high-volume payment and account events, streams them through Kafka, runs rule-based fraud detection in Go, uses Redis for fast rolling-window checks, and stores durable events and alerts in PostgreSQL.

## Why this exists

The goal is to build a simple but realistic systems project without a frontend. It focuses on the backend pieces commonly used in real-time fraud, payments, e-commerce, gaming, ad tech, and telemetry systems.

## Core flow

```text
producer -> Kafka -> detector consumer -> Redis/PostgreSQL -> API
```

## What is implemented

- Synthetic transaction event producer
- Kafka detector consumer
- Rule-based fraud detection engine
- Redis counters, sets, recent alerts, and risk leaderboard
- PostgreSQL raw event, alert, and risk score storage
- HTTP API for stats, alerts, user risk, and risk leaderboard
- Docker Compose local stack
- Kubernetes app manifests

## Main components

- Go: producer, detector, API, and benchmark tooling
- Kafka: durable event stream for transaction events
- Redis: fast rolling-window state for fraud rules
- PostgreSQL: durable storage for raw events and fraud alerts
- Docker Compose: local development environment
- Kubernetes: production-style deployment manifests

## First milestone

- Start Kafka, Redis, and PostgreSQL with Docker Compose
- Generate synthetic transaction events
- Publish events to Kafka
- Consume events with a Go detector
- Detect too many failed payments from one user
- Store generated fraud alerts in PostgreSQL

## Run locally

Start Kafka, Redis, PostgreSQL, the detector, and the API:

```bash
docker compose up --build
```

In another terminal, generate synthetic events:

```bash
docker compose run --rm producer
```

Query the API:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/stats
curl http://localhost:8080/alerts/recent
curl http://localhost:8080/users/user_hot_failures/risk
curl http://localhost:8080/leaderboard/risk
```

Run the Redis vs PostgreSQL lookup benchmark:

```bash
docker compose run --rm benchmark
```

Stop the stack:

```bash
docker compose down -v
```

## Local Go commands

The normal path is Docker Compose because Redis and PostgreSQL are kept internal to avoid port conflicts. If you want to run the Go commands directly, point `DATABASE_URL` and `REDIS_ADDR` at your own local services.

```bash
go run ./cmd/producer -rate=1000 -duration=30s
DATABASE_URL="postgres://fraud:fraud@localhost:5432/fraud?sslmode=disable" REDIS_ADDR="localhost:6379" go run ./cmd/detector
DATABASE_URL="postgres://fraud:fraud@localhost:5432/fraud?sslmode=disable" REDIS_ADDR="localhost:6379" go run ./cmd/api
DATABASE_URL="postgres://fraud:fraud@localhost:5432/fraud?sslmode=disable" REDIS_ADDR="localhost:6379" go run ./cmd/benchmark
```

## Project plan

See [docs/plan.md](docs/plan.md) for the full implementation plan.

See [docs/implementation.md](docs/implementation.md) for a file-by-file explanation of how the project was implemented.
