# Real-Time Fraud Detection Pipeline

Backend-only event streaming project built with Go, Apache Kafka, Redis, PostgreSQL, Docker, and Kubernetes.

The project simulates high-volume payment and account events, streams them through Kafka, runs rule-based fraud detection in Go, uses Redis for fast rolling-window checks, and stores durable events and alerts in PostgreSQL.

## Why this exists

The goal is to build a simple but realistic systems project without a frontend. It focuses on the backend pieces commonly used in real-time fraud, payments, e-commerce, gaming, ad tech, and telemetry systems.

## Core flow

```text
producer -> Kafka -> detector consumer -> Redis/PostgreSQL -> API
```

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

## Project plan

See [docs/plan.md](docs/plan.md) for the full implementation plan.
