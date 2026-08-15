#!/usr/bin/env bash
# EventHorizon smoke test: start the stack and prove one connection is counted.
#
# Starts the Compose stack, waits for the exporter and Prometheus, opens a single Telnet connection, and checks that the exporter counted it. Also checks that Grafana is up. Leaves the stack running so you can open the dashboards.
#
# Usage: scripts/smoke.sh
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

telnet_port="${TELNET_PORT:-$(grep -E '^TELNET_PORT=' .env | cut -d= -f2)}"
exporter_url="http://127.0.0.1:9101/metrics"
prometheus_url="http://127.0.0.1:9090/-/healthy"
grafana_url="http://127.0.0.1:3000/api/health"

step() { printf '\n== %s\n' "$1"; }
ok() { printf '[ok]   %s\n' "$1"; }
fail() {
    printf '[FAIL] %s\n' "$1" >&2
    printf '\nSMOKE FAILED. Container logs: docker compose logs\n' >&2
    exit 1
}

# Poll a URL until it answers, up to 120 seconds.
wait_for() {
    local label="$1" url="$2" deadline=$((SECONDS + 120))
    until curl --fail --silent --output /dev/null "$url"; do
        ((SECONDS < deadline)) || fail "$label did not become ready at $url"
        sleep 3
    done
    ok "$label is ready"
}

# Current value of total_connects{server="Telnet"}, or 0 if not yet exported.
telnet_connects() {
    curl --fail --silent "$exporter_url" \
        | awk '/^total_connects\{server="Telnet"\}/ { print $2; found = 1 }
               END { if (!found) print 0 }'
}

command -v nc >/dev/null || fail "nc (netcat) is required by this smoke test"

step "Starting the stack (docker compose up -d --build)"
docker compose up -d --build
ok "containers started"

step "Waiting for services"
wait_for exporter "$exporter_url"
wait_for Prometheus "$prometheus_url"
wait_for Grafana "$grafana_url"

step "Opening one Telnet connection on port $telnet_port"
before="$(telnet_connects)"
timeout 3 nc 127.0.0.1 "$telnet_port" </dev/null >/dev/null 2>&1 || true
ok "connection opened and closed"

step "Checking that the exporter counted it"
deadline=$((SECONDS + 30))
while :; do
    after="$(telnet_connects)"
    if awk "BEGIN { exit !($after > $before) }"; then
        ok "total_connects{server=\"Telnet\"} rose from $before to $after"
        break
    fi
    ((SECONDS < deadline)) || fail "total_connects{server=\"Telnet\"} stayed at $before"
    sleep 2
done

step "Checking Grafana"
curl --fail --silent "$grafana_url" | grep -q '"database": *"ok"' \
    || fail "Grafana is up but did not report a healthy database at $grafana_url"
ok "Grafana is healthy"

cat <<EOF

SMOKE PASSED.

The stack is still running:
  Grafana     http://127.0.0.1:3000
  Prometheus  http://127.0.0.1:9090
  Exporter    http://127.0.0.1:9101/metrics

Stop it with: docker compose down
EOF
