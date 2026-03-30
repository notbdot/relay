### Stage 1: build the binary
FROM golang:1.22-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /relay .


### Stage 2: runtime image with ffmpeg
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /data
COPY --from=builder /relay /usr/local/bin/relay

# HTTP viewer/admin port
EXPOSE 2935
# SRT ingest port (UDP)
EXPOSE 9999/udp

VOLUME ["/data"]

ENTRYPOINT ["relay"]
CMD ["serve"]
