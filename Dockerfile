# syntax=docker/dockerfile:1
# Multi-stage build: compile with the full Go toolchain, ship only static binaries
# in a minimal image (smaller, faster to pull, fewer CVEs, no shell for an attacker).
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/api ./cmd/worker ./cmd/webhook-receiver ./cmd/loadtest

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/api"]
