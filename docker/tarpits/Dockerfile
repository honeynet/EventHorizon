FROM gcc:12.5.0-bookworm AS c-builder

WORKDIR /src
COPY servers/ ./servers/
COPY shared/ ./shared/
COPY makefile .

RUN make telnet_pit upnp_pit mqtt_pit coap_pit

# Final image
FROM debian:bookworm-slim

COPY --from=c-builder /src/bin/* /usr/local/bin/
COPY run.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/run.sh

ENTRYPOINT ["/usr/local/bin/run.sh"]
