# Deployment

EventHorizon deploys as a Docker Compose stack. Clone the repository on the target host, configure `.env`, select a Compose file set, inspect the rendered bindings, and start the services.

## Before you expose EventHorizon

EventHorizon is a multiprotocol tarpit framework and a specialized low-interaction honeypot. A public deployment will receive hostile automated traffic.

Deploy only on an authorized host that carries no unrelated production workload or sensitive data and can be rebuilt if necessary.

Keep the provider and host firewalls closed while starting and verifying the stack. Docker publishes configured tarpit ports on the host even while an external firewall denies inbound traffic. Open only the intended protocol ports after checking the rendered bindings and private health endpoints.

## Requirements

- Linux host with Docker Engine and the Docker Compose plugin
- x86-64 or arm64 architecture
- Git and curl
- Netcat if using `scripts/smoke.sh`
- Configured tarpit ports available on the host

## 1. Clone and configure

```bash
git clone https://github.com/honeynet/EventHorizon.git
cd EventHorizon
```

Contributors can substitute their fork URL when testing unmerged changes.

The `.env` file controls host port mappings and protocol limits.

| Variable | Default | Notes |
| --- | ---: | --- |
| `TELNET_PORT` | 23 | Telnet over TCP |
| `SSH_PORT` | 22 | Conflicts with a host SSH server using port 22 |
| `MQTT_PORT` | 1883 | MQTT over TCP |
| `COAP_PORT` | 5683 | CoAP over UDP |
| `UPNP_SSDP_PORT` | 1900 | SSDP over UDP |
| `UPNP_HTTP_PORT` | 8080 | Device description over TCP |

If the host SSH server listens on port 22, starting Endlessh with the default mapping will fail. Either assign the tarpit another port in `.env`:

```dotenv
SSH_PORT=2222
```

Or move the real SSH service first, verify a new management connection on its new port, and only then leave `SSH_PORT=22` for Endlessh. Moving the management service carelessly can lock you out.

Check existing listeners before startup:

```bash
ss -ltn
ss -lun
```

## 2. Select a deployment mode

Export one of the following `COMPOSE_FILE` values in each new shell. All later Compose commands and `scripts/smoke.sh` will then use the same file set.

Normal deployment:

```bash
export COMPOSE_FILE=docker-compose.yml
```

Long-running or public observation deployment:

```bash
export COMPOSE_FILE=docker-compose.yml:docker-compose.field.yml
```

The field overlay runs all five tarpits, restarts failed services, applies process health checks and resource limits, disables tarpit container logs, routes structured session logs to `/dev/null`, and keeps observability endpoints on loopback.

Each tarpit defaults to half of one CPU core and 512 processes. Set `FIELD_TARPIT_MEMORY_LIMIT` to a supported Compose value such as `256m` when the host supports container memory limits. The default `0` leaves memory uncapped.

Field deployment with optional defender-cost evidence:

```bash
export COMPOSE_FILE=docker-compose.yml:docker-compose.field.yml:docker-compose.cost.yml
```

The cost overlay adds privileged cAdvisor access and a Prometheus configuration that also scrapes container metrics. cAdvisor remains bound to `127.0.0.1:8081` and is unnecessary unless CPU and container network evidence is being collected.

## 3. Inspect the rendered configuration

```bash
docker compose config
docker compose config --services
```

Confirm that:

- only the intended tarpit ports bind to `0.0.0.0`;
- Grafana, Prometheus, and the exporter bind to `127.0.0.1`;
- all five tarpit services are present; and
- cAdvisor is absent unless the cost overlay was selected.

## 4. Start and verify

```bash
docker compose up -d --build
docker compose ps
./scripts/smoke.sh
```

The smoke test opens one Telnet connection, confirms that the exporter counted it, and checks Prometheus and Grafana health. It is a focused critical-path check, not an all-protocol runtime test.

Additional commands are documented in [README.md](README.md#manual-verification).

## 5. Open the firewall

Only after verification should the provider and host firewalls allow inbound traffic to the configured tarpit ports.

For the default dedicated-VPS mapping:

| Transport | Public ports |
| --- | --- |
| TCP | 22, 23, 1883, 8080 |
| UDP | 1900, 5683 |

Port 22 is appropriate for the SSH tarpit only when the real management SSH service has already moved elsewhere. Port mappings remain environment-specific.

Never expose these observability endpoints publicly:

| Service | Loopback port |
| --- | ---: |
| Grafana | 3000 |
| Prometheus | 9090 |
| Exporter | 9101 |
| Optional cAdvisor | 8081 |

Because Docker-published ports interact with host firewall rules, a host firewall rule alone isn't enough to control a published container port. See Docker's [packet filtering and firewall documentation](https://docs.docker.com/engine/network/packet-filtering-firewalls/) for details.

## Operations

Set the same `COMPOSE_FILE` value again whenever a new shell is opened.

### Access Grafana and Prometheus

Keep both services on loopback and tunnel through the real management SSH service. Replace port `2222` with its actual port:

```bash
ssh -p 2222 -N \
  -L 3000:127.0.0.1:3000 \
  -L 9090:127.0.0.1:9090 \
  user@your-host
```

Then open `http://127.0.0.1:3000` or `http://127.0.0.1:9090` locally.

### Update

Record the current implementation commit before changing a measurement deployment. Then update and recreate the selected services:

```bash
git pull --ff-only
docker compose up -d --build
```

### Stop

Close the public firewall ports first if the host will remain online. Stop services while preserving Prometheus and Grafana data:

```bash
docker compose down
```

### Delete stored data

The following command permanently deletes the named Prometheus and Grafana volumes in addition to stopping services:

```bash
docker compose down --volumes
```

Do not use `--volumes` when observation data must be retained.
