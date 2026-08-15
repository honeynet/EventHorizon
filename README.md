# EventHorizon
[![CI](https://github.com/honeynet/EventHorizon/actions/workflows/ci.yml/badge.svg)](https://github.com/honeynet/EventHorizon/actions/workflows/ci.yml)

**EventHorizon** is an open-source framework for deploying and analyzing multiprotocol IoT tarpits.  
It provides a modular, containerized environment where each protocol emulator runs inside its own Docker container, 
and all telemetry data is collected and visualized through a Prometheus + Grafana stack.

This project was presented at DIMVA 2026 in Chania, Greece. For more information about EventHorizon in DIMVA click [here](https://www.dimva.org/dimva2026/).

### 🌌 Why the name *EventHorizon*?
In astrophysics, the *event horizon* is the boundary around a black hole beyond which nothing can escape.  
Similarly, the **EventHorizon** framework acts as a boundary for malicious network activity:  
once an automated scanner crosses into it, the connection cannot progress or escape—it becomes trapped indefinitely.  

This captures the essence of what the framework does: slowing, containing, and observing automated attacks without letting them spread.

---

## 🚀 Quick start

```bash
git clone https://github.com/honeynet/EventHorizon.git
cd EventHorizon
docker compose up -d --build
```

Then check that everything works:

```bash
./scripts/smoke.sh
```

The smoke test starts the stack, opens one Telnet connection, and confirms that the exporter counted it. It prints `SMOKE PASSED` and leaves the stack running.

Stop everything with:
```bash
docker compose down
```

## Services and ports

| Service | Address or Port | Purpose |
| --- | --- | --- |
| Grafana | http://127.0.0.1:3000 | Protocol measurement dashboard |
| Prometheus | http://127.0.0.1:9090 | Metric storage and queries |
| Exporter | http://127.0.0.1:9101/metrics | Prometheus metrics from the tarpits |
| Telnet tarpit | TCP port 23 | Prolonged Telnet negotiation |
| MQTT tarpit | TCP port 1883 | Protocol-aware MQTT interaction |
| UPnP tarpit | UDP port 1900 and TCP port 8080 | SSDP discovery and device-description responses |
| CoAP tarpit | UDP port 5683 | CoAP request and response exchanges |
| SSH tarpit | configured `SSH_PORT`, default TCP port 22 | Vendored Endlessh SSH tarpit |

Ports and per-protocol limits are configured in [`.env`](.env). The defaults use common service ports, but mappings remain environment-specific. If your own SSH server listens on port 22, change `SSH_PORT` before starting EventHorizon.

## Dashboard

The EventHorizon Metrics dashboard presents:
- Current activity using protocol-specific observation units
- Finalized work and duration observations
- Timing and interaction-depth measurements
- Protocol outcomes and reliability evidence
- Telemetry health
- Optional container CPU and network evidence.

The optional Defender Cost row is collapsed by default and requires [cAdvisor](https://github.com/google/cadvisor), which is not needed for a normal EventHorizon deployment.

Enable the defender cost evidence with:

```bash
docker compose \
    -f docker-compose.yml \
    -f docker-compose.cost.yml \
    up -d
```

The dashboard refreshes every 30 seconds by default.

## Manual verification

The smoke test automates the following critical-path check:
```bash
# Confirm that the exporter is serving metrics
curl -s http://127.0.0.1:9101/metrics | grep total_connects

# Open a connection to the Telnet tarpit
# Press Ctrl-C to disconnect
nc 127.0.0.1 23

# Confirm that the connection was counted
curl -s http://127.0.0.1:9101/metrics | grep 'total_connects{server="Telnet"}'

# Confirm that Prometheus and Grafana are healthy
curl -s http://127.0.0.1:9090/-/healthy
curl -s http://127.0.0.1:3000/api/health
```
[METRICS.md](METRICS.md) documents the metric families, observation units, label schemas, and interpretation limits.

## Development

```bash
make all               # Build the tarpit binaries and the Go exporter
make test              # Run the C unit tests
make test-go           # Run the Go exporter tests and go vet
make check-dashboards  # Check Grafana panels against the metric surface
make smoke             # Run the focused Compose smoke test
```

CI contains three focused jobs: `build-and-test`, `shell-quality`, and `smoke`. The smoke test verifies one critical Telnet path. Broader protocol validation is provided by the real-producer integration tests.

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) before placing EventHorizon on a public host.

Only intended tarpit protocol ports should be exposed publicly. Grafana, Prometheus, the exporter, and optional cAdvisor endpoints should remain private.

Protocol mappings are environment-specific. For example, the SSH tarpit may use port 22 on a dedicated observation VPS while using another port on a host that already runs an SSH server.

## License

EventHorizon is released under the MIT License. Bundled third-party components retain their respective license terms. See [LICENSE](LICENSE) for details.
