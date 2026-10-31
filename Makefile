TEST_DATABASE_URL ?= postgres://ledgerflow:ledgerflow@localhost:5432/ledgerflow?sslmode=disable
TEST_REDIS_URL    ?= redis://localhost:6379/1

.PHONY: up down logs test test-unit vet build loadtest

up:            ## start the full stack (postgres, redis, api, 2 workers, demo receiver)
	docker compose up --build -d
	@echo "Dashboard: http://localhost:8080  (API key: lf_demo_key)"

down:
	docker compose down -v

logs:
	docker compose logs -f api worker webhook-receiver

build:
	go build -o bin/ ./cmd/...

vet:
	go vet ./...

test-unit:     ## tests that need no database
	go test ./...

test:          ## all tests, including integration tests against real Postgres and Redis
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" TEST_REDIS_URL="$(TEST_REDIS_URL)" go test -race -count=1 ./...

loadtest:
	go run ./cmd/loadtest -url http://localhost:8080 -key lf_demo_key -accounts 20 -workers 32 -duration 15s
