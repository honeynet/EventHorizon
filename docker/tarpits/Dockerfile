FROM gcc:12 AS c-builder

WORKDIR /src
COPY servers/ ./servers/
COPY shared/ ./shared/
COPY makefile .

RUN make telnet_pit upnp_pit mqtt_pit coap_pit

# Final image
FROM debian:bookworm-slim

COPY --from=c-builder /src/bin/* /usr/local/bin/
COPY run.sh /usr/local/bin/
RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
RUN chmod +x /usr/local/bin/run.sh
RUN useradd -r -s /bin/false tarpit
USER tarpit

ENTRYPOINT ["/usr/local/bin/run.sh"]
