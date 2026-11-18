FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/streamforge ./cmd/streamforge && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/streamforge-cli ./cmd/streamforge-cli

FROM alpine:3.21
RUN adduser -D -u 10001 streamforge && mkdir -p /data && chown streamforge:streamforge /data
USER streamforge
WORKDIR /data
COPY --from=builder /out/streamforge /usr/local/bin/streamforge
COPY --from=builder /out/streamforge-cli /usr/local/bin/streamforge-cli
EXPOSE 9092 9093
HEALTHCHECK --interval=30s --timeout=5s CMD wget -qO- http://127.0.0.1:9093/api/v1/health || exit 1
ENTRYPOINT ["/usr/local/bin/streamforge"]
CMD ["-data-dir", "/data/log", "-wire-addr", "0.0.0.0:9092", "-http-addr", "0.0.0.0:9093"]
