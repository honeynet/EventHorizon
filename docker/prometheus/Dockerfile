FROM golang:1.23.9-bookworm AS go-builder

WORKDIR /app
ENV GOTOOLCHAIN=local \
    GOFLAGS=-mod=readonly
COPY prometheus/go.mod prometheus/go.sum ./
RUN sha256sum go.mod go.sum > /tmp/go-modules.sha256
COPY prometheus/ .
RUN sha256sum -c /tmp/go-modules.sha256 \
    && mkdir -p /out \
    && go build -mod=readonly -o /out/prometheus_exporter

# Final image
FROM debian:bookworm-slim

COPY --from=go-builder /out/prometheus_exporter /usr/local/bin/

WORKDIR /app
CMD ["prometheus_exporter"]
