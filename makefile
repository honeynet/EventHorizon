CC = gcc
CFLAGS = -Wall -Wextra -g -pthread -DEVENTHORIZON_JSON_METRIC_EVENTS

STRUCTS = shared/structs.c shared/session_events.c shared/interaction_depth.c shared/metric_events.c

TELNET_TARGET = bin/telnet_pit
UPNP_TARGET = bin/upnp_pit
MQTT_TARGET = bin/mqtt_pit
COAP_TARGET = bin/coap_pit
BYTE_METRIC_TEST_TARGET = bin/byte_metric_test
INTERACTION_DEPTH_TEST_TARGET = bin/interaction_depth_test
METRIC_EVENT_EMITTER_TEST_TARGET = bin/metric_event_emitter_test

TELNET_SRC = servers/telnet_pit.c
UPNP_SRC = servers/upnp_pit.c
MQTT_SRC = servers/mqtt_pit.c
COAP_SRC = servers/coap_pit.c

GO_DIR = prometheus
GO_TARGET = bin/prometheus_exporter
GO_SRCS := $(wildcard prometheus/*.go)

BIN_DIR = bin

# Default Rule
all: $(TELNET_TARGET) $(UPNP_TARGET) $(MQTT_TARGET) $(COAP_TARGET) $(GO_TARGET)

$(TELNET_TARGET): $(TELNET_SRC) $(STRUCTS) | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^ 

$(UPNP_TARGET): $(UPNP_SRC) $(STRUCTS) | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^ 

$(MQTT_TARGET): $(MQTT_SRC) $(STRUCTS) | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^ 

$(COAP_TARGET): $(COAP_SRC) $(STRUCTS) | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^ 

$(BYTE_METRIC_TEST_TARGET): tests/byte_metric_test.c $(STRUCTS) | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^

$(INTERACTION_DEPTH_TEST_TARGET): tests/interaction_depth_test.c shared/interaction_depth.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^

$(METRIC_EVENT_EMITTER_TEST_TARGET): tests/metric_event_emitter_test.c shared/metric_events.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^

$(GO_TARGET): $(GO_SRCS) | $(BIN_DIR)
	cd $(GO_DIR) && go build -o ../$(GO_TARGET)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

# Aliases
telnet_pit: $(TELNET_TARGET)
upnp_pit:   $(UPNP_TARGET)
mqtt_pit:   $(MQTT_TARGET)
coap_pit:	$(COAP_TARGET)
prometheus: $(GO_TARGET)
test-byte-metrics: $(BYTE_METRIC_TEST_TARGET)
	./$(BYTE_METRIC_TEST_TARGET)
test-interaction-depth: $(INTERACTION_DEPTH_TEST_TARGET)
	./$(INTERACTION_DEPTH_TEST_TARGET)
test-metric-event-emitter: $(METRIC_EVENT_EMITTER_TEST_TARGET)
	./$(METRIC_EVENT_EMITTER_TEST_TARGET)
test: test-byte-metrics test-interaction-depth test-metric-event-emitter

test-go:
	cd $(GO_DIR) && GOTOOLCHAIN=local GOFLAGS=-mod=readonly go test ./...
	cd $(GO_DIR) && GOTOOLCHAIN=local GOFLAGS=-mod=readonly go vet ./...

check-dashboards:
	python3 scripts/check_dashboards.py

smoke:
	./scripts/smoke.sh

clean:
	rm -f $(TELNET_TARGET) $(UPNP_TARGET) $(MQTT_TARGET) $(GO_TARGET) $(BYTE_METRIC_TEST_TARGET) $(INTERACTION_DEPTH_TEST_TARGET) $(METRIC_EVENT_EMITTER_TEST_TARGET)

.PHONY: all clean test test-byte-metrics test-interaction-depth test-metric-event-emitter test-go check-dashboards smoke
