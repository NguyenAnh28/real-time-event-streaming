.PHONY: fmt build test compose-up compose-down produce benchmark

fmt:
	go fmt ./...

build:
	go build ./...

test:
	go test ./...

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v

produce:
	docker compose run --rm producer

benchmark:
	docker compose run --rm benchmark

