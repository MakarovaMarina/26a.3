FROM golang:1.24.1 AS builder
WORKDIR /app
COPY . .
RUN go build -o pipeline


FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/pipeline .
ENTRYPOINT ["./pipeline"]