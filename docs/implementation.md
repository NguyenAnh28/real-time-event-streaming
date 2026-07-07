# Implementation Guide

This document explains the order the project was implemented, why that order made sense, what each file does, and how the files connect to form the full fraud detection pipeline.

## Big Picture

The system has one main flow:

```text
producer -> Kafka -> detector -> Redis + PostgreSQL -> API
```

Each part has a specific job:

- The producer generates fake payment/account events.
- Kafka stores those events as a stream.
- The detector consumes events from Kafka and runs fraud rules.
- Redis stores fast rolling-window state for rules.
- PostgreSQL stores durable events, alerts, and risk scores.
- The API exposes results so you can inspect the system.

## Implementation Order

### 1. Project Foundation

The first step was creating the basic Go project and runtime shape.

Files:

- `go.mod`
- `go.sum`
- `.gitignore`
- `Makefile`

Why first:

Every Go service needs the same module, dependencies, formatting, build, and test commands. Creating this foundation first makes the rest of the project easier to build and verify.

What they do:

- `go.mod` defines the Go module name and dependency roots.
- `go.sum` locks dependency checksums.
- `.gitignore` keeps local build outputs and environment files out of git.
- `Makefile` gives short commands for formatting, building, testing, and running Docker Compose.

### 2. Shared Configuration

File:

- `internal/config/config.go`

Why next:

All services need the same environment settings: Kafka brokers, topic name, database URL, Redis address, and fraud rule thresholds. Centralizing config avoids hardcoding values across commands.

What it does:

- Reads environment variables.
- Provides defaults for local development.
- Groups fraud rule thresholds and time windows into one `Rules` config struct.

Used by:

- `cmd/producer/main.go`
- `cmd/detector/main.go`
- `cmd/api/main.go`
- `cmd/benchmark/main.go`

### 3. Shared Data Models

Files:

- `internal/models/event.go`
- `internal/models/alert.go`

Why next:

Before services can talk to each other, they need to agree on the shape of the data. These models define that contract.

What they do:

- `event.go` defines transaction/account events such as `payment_failed` and `purchase_completed`.
- `event.go` validates required fields before processing.
- `alert.go` defines fraud alerts, user risk records, and API stats.
- `alert.go` creates new alert IDs and timestamps.

Used by:

- Producer when creating events.
- Detector when parsing Kafka messages.
- Fraud rules when creating alerts.
- Database and Redis layers when storing results.
- API when returning JSON responses.

### 4. Synthetic Event Generator

File:

- `internal/generator/generator.go`

Why here:

The project needs event traffic before Kafka and fraud detection can be tested. The generator gives the system realistic input without building a real e-commerce app.

What it does:

- Generates normal events.
- Generates suspicious event patterns.
- Supports fake users, cards, IPs, merchants, countries, and amounts.
- Produces repeated suspicious identities like `user_hot_failures`, `user_fast_buyer`, and `card_shared_many_users`.

Used by:

- `cmd/producer/main.go`

### 5. Kafka Helper

File:

- `internal/kafka/topics.go`

Why here:

The producer and detector both need the `transactions` topic to exist. This helper keeps topic setup out of the command code.

What it does:

- Connects to Kafka.
- Finds the Kafka controller.
- Creates the configured topic with a default partition count.

Used by:

- `cmd/producer/main.go`
- `cmd/detector/main.go`

### 6. PostgreSQL Layer

Files:

- `internal/db/schema.go`
- `internal/db/store.go`
- `configs/migrations/001_init.sql`

Why before the detector:

The detector needs somewhere durable to store raw events, fraud alerts, and risk scores. The database layer was built before the detector so processing code could call clean storage methods.

What they do:

- `schema.go` contains the SQL schema the app can create automatically.
- `store.go` connects to PostgreSQL and exposes methods like:
  - `InsertRawEvent`
  - `InsertAlert`
  - `AddRisk`
  - `RecentAlerts`
  - `UserRisk`
  - `Stats`
- `001_init.sql` mirrors the schema as a real migration artifact.

Used by:

- `cmd/detector/main.go`
- `cmd/api/main.go`
- `cmd/benchmark/main.go`

### 7. Redis Layer

File:

- `internal/cache/cache.go`

Why before rules:

The fraud rules need fast short-term memory. Redis is where rolling counters, unique sets, recent alerts, and risk leaderboard data live.

What it does:

- Connects to Redis.
- Increments counters with TTLs.
- Adds values to sets with TTLs.
- Stores recent alerts.
- Updates a sorted-set risk leaderboard.
- Reads risk scores from Redis.

Used by:

- `internal/detection/rules.go`
- `cmd/detector/main.go`
- `cmd/api/main.go`
- `cmd/benchmark/main.go`

### 8. Fraud Detection Engine

Files:

- `internal/detection/engine.go`
- `internal/detection/rules.go`

Why after Redis and models:

The rule engine depends on shared event/alert models and Redis state helpers. Once those existed, the fraud logic could stay clean and focused.

What they do:

- `engine.go` defines the `Rule` interface.
- `engine.go` runs every rule against each event.
- `rules.go` implements the MVP rules:
  - too many payment failures from one user
  - too many purchases from one user
  - same card used by many users
  - same IP used by many accounts
  - unusually high purchase amount

How a rule works:

```text
event comes in
-> rule checks if it cares about that event type
-> rule updates/reads Redis state
-> rule returns an alert if the threshold is crossed
```

Used by:

- `cmd/detector/main.go`

### 9. Producer Service

File:

- `cmd/producer/main.go`

Why first command:

The producer creates the input stream. Without it, there is nothing for Kafka or the detector to process.

What it does:

- Loads config and CLI flags.
- Creates the Kafka topic if needed.
- Uses the generator to create synthetic events.
- Publishes JSON events to Kafka.
- Supports rate, duration, batch size, and fraud-rate flags.

Connects to:

- `internal/config`
- `internal/generator`
- `internal/kafka`
- Kafka

### 10. Detector Service

File:

- `cmd/detector/main.go`

Why after producer and rules:

The detector is the core service. It needed Kafka input, rule logic, Redis state, and PostgreSQL storage to exist first.

What it does:

- Connects to PostgreSQL.
- Connects to Redis.
- Ensures the Kafka topic exists.
- Creates a Kafka consumer group.
- Reads messages from Kafka.
- Parses each message into an `Event`.
- Stores the raw event in PostgreSQL.
- Runs the fraud detection engine.
- Stores generated alerts in PostgreSQL.
- Updates durable user risk in PostgreSQL.
- Updates recent alerts and risk leaderboard in Redis.
- Commits Kafka offsets after processing.

Connects to:

- Kafka for incoming events.
- Redis for rolling-window rule state.
- PostgreSQL for durable storage.
- `internal/detection` for fraud rules.

### 11. API Service

File:

- `cmd/api/main.go`

Why after detector:

The API is useful only after there is stored data to inspect. It sits on top of Redis and PostgreSQL.

What it does:

- Starts an HTTP server.
- Exposes:
  - `GET /health`
  - `GET /stats`
  - `GET /alerts/recent`
  - `GET /users/{user_id}/risk`
  - `GET /leaderboard/risk`
- Reads durable data from PostgreSQL.
- Reads fast leaderboard/risk data from Redis.

Connects to:

- PostgreSQL through `internal/db`
- Redis through `internal/cache`

### 12. Benchmark Command

File:

- `cmd/benchmark/main.go`

Why after API/storage:

The project needs a measurable performance story. The benchmark exists to compare Redis risk lookups against PostgreSQL risk lookups.

What it does:

- Looks up one user's risk score many times in PostgreSQL.
- Looks up the same user's risk score many times in Redis.
- Prints total time, average lookup time, and percentage improvement.

Connects to:

- PostgreSQL through `internal/db`
- Redis through `internal/cache`

### 13. Local Runtime

Files:

- `docker-compose.yml`
- `Dockerfile`

Why after the services:

Once the Go commands existed, Docker made the full system runnable with one command.

What they do:

- `Dockerfile` builds all four Go commands into one container image.
- `docker-compose.yml` runs:
  - Kafka
  - Redis
  - PostgreSQL
  - detector
  - API
  - optional producer
  - optional benchmark

Important detail:

Redis and PostgreSQL are not published to host ports in Compose. They are internal services, which avoids conflicts if your machine already has Redis or PostgreSQL running. The API is published on port `8080`, and Kafka is published on port `9094` for local producer access.

### 14. Kubernetes Manifests

File:

- `configs/k8s/app.yaml`

Why last:

Kubernetes is deployment polish, not the core MVP. It was added after the local Docker Compose version worked.

What it does:

- Defines a namespace.
- Defines config and secret objects.
- Defines deployments for the detector and API.
- Defines an API service.
- Adds basic readiness and liveness probes.

This file shows how the app services would run in a production-style cluster, but it does not replace the local Docker Compose workflow.

### 15. Documentation

Files:

- `README.md`
- `docs/plan.md`
- `docs/implementation.md`

What they do:

- `README.md` explains how to run the project.
- `docs/plan.md` explains the project goal and development phases.
- `docs/implementation.md` explains how the implemented files fit together.

## End-to-End Data Flow

### Step 1: Generate Events

`cmd/producer/main.go` calls `internal/generator/generator.go`.

The generator returns a `models.Event`.

The producer JSON-encodes that event and writes it to Kafka.

```text
producer -> generator -> Event -> JSON -> Kafka transactions topic
```

### Step 2: Consume Events

`cmd/detector/main.go` reads messages from the Kafka `transactions` topic.

Each message is decoded into `models.Event` and validated.

```text
Kafka message -> detector -> models.Event -> Validate()
```

### Step 3: Store Raw Events

The detector calls `store.InsertRawEvent`.

PostgreSQL stores the original event in `raw_events`.

```text
detector -> internal/db/store.go -> raw_events
```

### Step 4: Run Fraud Rules

The detector sends the event to the detection engine.

The engine loops over all rules.

Each rule decides whether it cares about the event.

```text
detector -> Engine.Evaluate -> Rule.Evaluate
```

### Step 5: Use Redis for Fast Rule State

Rules call Redis helpers for fast state.

Examples:

```text
payment_failed -> IncrementCounter("fraud:user:{id}:payment_failures")
payment_attempt -> AddToSet("fraud:card:{card}:users")
login_attempt -> AddToSet("fraud:ip:{ip}:users")
```

If a threshold is crossed, the rule returns an alert.

### Step 6: Store Alerts and Risk

For each alert, the detector:

- stores the alert in PostgreSQL
- updates durable user risk in PostgreSQL
- stores recent alert/risk leaderboard data in Redis

```text
Alert -> fraud_alerts
Alert risk points -> user_risk_scores
Alert risk points -> Redis sorted set fraud:risk_scores
```

### Step 7: Read Results Through API

`cmd/api/main.go` exposes HTTP endpoints.

Some endpoints read PostgreSQL:

```text
/stats
/alerts/recent
/users/{user_id}/risk
```

Some endpoints read Redis:

```text
/leaderboard/risk
Redis side of /users/{user_id}/risk
```

## Dependency Map

```text
cmd/producer
  -> internal/config
  -> internal/generator
  -> internal/kafka
  -> internal/models
  -> Kafka

cmd/detector
  -> internal/config
  -> internal/db
  -> internal/cache
  -> internal/detection
  -> internal/kafka
  -> internal/models
  -> Kafka
  -> Redis
  -> PostgreSQL

cmd/api
  -> internal/config
  -> internal/db
  -> internal/cache
  -> PostgreSQL
  -> Redis

cmd/benchmark
  -> internal/config
  -> internal/db
  -> internal/cache
  -> PostgreSQL
  -> Redis
```

## Why This Order Works

The implementation starts with stable shared pieces, then builds outward:

1. Config and models define the common language.
2. Generator creates realistic input.
3. Kafka moves the input as a stream.
4. Database and cache packages provide storage/state.
5. Detection rules define the actual business logic.
6. Commands wire everything into runnable services.
7. Docker Compose runs the whole system locally.
8. Kubernetes manifests show production-style deployment.

That order keeps the project understandable. Each layer depends on the layer below it, so you can learn and debug the system one piece at a time.
