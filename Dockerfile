FROM golang:1.27-alpine AS builder

WORKDIR /app


COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/worker ./cmd/worker

FROM alpine:3.20
WORKDIR /app


RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /bin/server /usr/local/bin/server
COPY --from=builder /bin/worker /usr/local/bin/worker

COPY --from=builder /app/migrations ./migrations

USER appuser

EXPOSE 8080

ENTRYPOINT [ "server" ]