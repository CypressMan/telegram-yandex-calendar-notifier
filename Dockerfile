FROM golang:1.21-alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bot ./main.go

FROM alpine:3.21
WORKDIR /app


COPY --from=builder /app/bot /app/bot
COPY --from=builder /app/users.json /app/data/users.json
COPY --from=builder /app/help.png /app/help.png

RUN adduser -D appuser && mkdir -p /app/data && chown -R appuser:appuser /app

USER appuser
ENV USERS_FILE=/app/data/users.json
ENV LOG_DIR=/app/data/logs
ENV LOG_ROTATE_TIME=00:05
ENV LOG_RETENTION_DAYS=14
ENV HELP_IMAGE_PATH=/app/help.png

CMD ["/app/bot"]


