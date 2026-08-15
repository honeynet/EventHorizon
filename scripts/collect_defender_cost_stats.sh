#!/usr/bin/env bash
set -euo pipefail

mapfile -t container_ids < <(docker compose ps -q telnet_pit mqtt_pit)
if ((${#container_ids[@]} == 0)); then
    echo "Telnet and MQTT containers are not running" >&2
    exit 1
fi

printf 'NAME\tCPU\tMEMORY USAGE / LIMIT\tNETWORK I/O\n'
docker stats --no-stream \
    --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}' \
    "${container_ids[@]}"

printf '\nNAME\tRESTARTS\tOOM KILLED\tHEALTH\n'
for container_id in "${container_ids[@]}"; do
    docker inspect --format \
        '{{.Name}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}not-configured{{end}}' \
        "$container_id" | sed 's#^/##' | tr '|' '\t'
done
