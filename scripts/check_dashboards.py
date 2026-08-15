#!/usr/bin/env python3
"""Check Grafana dashboard PromQL against the current metric contract.

Every metric family referenced by a dashboard panel must exist in the exporter's
current exposition. This catches the drift class that the typed-contract cutover
produced: panels left querying removed families, or removed label values.

The allowlist below is a literal transcription of the metric-family summary in
METRICS.md (repo root). When a family is added or removed there, update LIVE.

Usage:  python3 scripts/check_dashboards.py [--json]
Exit:   0 = no findings, 1 = findings.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO / "grafana" / "dashboards"

# --- Metric families the exporter registers. Source: METRICS.md, "Metric-family
# --- summary". Histogram _bucket/_sum/_count suffixes are stripped before match.
LIVE = {
    "total_connects",
    "current_connected_clients",
    "eventhorizon_protocol_actions_total",
    "eventhorizon_read_errors_total",
    "eventhorizon_write_errors_total",
    "eventhorizon_exporter_malformed_messages_total",
    "eventhorizon_bytes_received_total",
    "eventhorizon_bytes_sent_total",
    "eventhorizon_completed_sessions_total",
    "eventhorizon_session_duration_ms",
    "eventhorizon_session_interaction_depth_total",
    "eventhorizon_telnet_first_write_delay_ms",
    "eventhorizon_telnet_inter_write_interval_ms",
    "eventhorizon_ssh_tracked_client_lifetime_ms",
    "eventhorizon_mqtt_network_connection_finalizations_total",
    "eventhorizon_mqtt_network_connection_duration_ms",
    "eventhorizon_mqtt_network_connection_interaction_depth_total",
    "eventhorizon_mqtt_connect_to_connack_duration_ms",
    "eventhorizon_coap_active_request_exchanges",
    "eventhorizon_coap_request_exchange_duration_ms",
    "eventhorizon_coap_active_con_response_exchanges",
    "eventhorizon_coap_con_response_exchange_duration_ms",
    "eventhorizon_upnp_active_description_responses",
    "eventhorizon_upnp_description_stream_duration_ms",
}

# --- Closed label schemas. Source: METRICS.md, "Closed label schemas". Only the
# --- listed values are ever initialized; anything else is drift. `None` means the
# --- family has no explicit labels.
IO_REASONS = {"timeout", "reset", "closed", "other"}
DEPTH = {"0", "1", "2", "3"}

LABELS: dict[str, dict[str, set[str]]] = {
    "total_connects": {"server": {"Telnet", "MQTT", "SSH"}},
    "current_connected_clients": {"server": {"Telnet", "MQTT"}},
    "eventhorizon_protocol_actions_total": {
        "protocol": {"upnp", "coap", "mqtt"},
        "action": {
            # upnp
            "ssdp_msearch_received", "ssdp_discovery_response_sent",
            "description_get_received", "description_response_started",
            "description_response_completed", "description_response_terminated",
            # coap
            "get_request_received", "get_response_sent", "get_request_terminated",
            "con_response_sent", "con_response_retransmitted",
            "con_response_ack_received", "con_response_rst_received",
            "con_response_retry_exhausted",
            # mqtt
            "connect_accepted", "connack_sent", "publish_received",
            "subscribe_received", "unsubscribe_received", "suback_sent",
            "unsuback_sent", "puback_sent", "pubrec_sent", "pubrel_received",
            "pubcomp_sent", "subscription_publish_sent",
        },
    },
    "eventhorizon_read_errors_total": {"protocol": {"telnet", "mqtt"}, "reason": IO_REASONS},
    "eventhorizon_write_errors_total": {
        "protocol": {"upnp", "coap", "telnet", "mqtt"}, "reason": IO_REASONS},
    "eventhorizon_exporter_malformed_messages_total": {"reason": {
        "empty_message", "missing_fields", "unknown_format", "unknown_server",
        "invalid_number", "unsupported_event"}},
    "eventhorizon_bytes_received_total": {"protocol": {"telnet", "mqtt"}},
    "eventhorizon_bytes_sent_total": {"protocol": {"telnet", "mqtt"}},
    "eventhorizon_completed_sessions_total": {
        "protocol": {"telnet"},
        "disconnect_reason": {"peer_closed", "read_error", "write_error",
                              "server_shutdown", "bounded_policy"}},
    "eventhorizon_session_duration_ms": {"protocol": {"telnet"}},
    "eventhorizon_session_interaction_depth_total": {
        "protocol": {"telnet"}, "depth_level": DEPTH},
    "eventhorizon_telnet_first_write_delay_ms": {},
    "eventhorizon_telnet_inter_write_interval_ms": {},
    "eventhorizon_mqtt_network_connection_finalizations_total": {"finalization_reason": {
        "peer_closed", "disconnect_received", "connect_refused", "operation_refused",
        "protocol_error", "keep_alive_timeout", "read_error", "write_error",
        "server_shutdown", "bounded_policy"}},
    "eventhorizon_mqtt_network_connection_duration_ms": {},
    "eventhorizon_mqtt_network_connection_interaction_depth_total": {"depth_level": DEPTH},
    "eventhorizon_mqtt_connect_to_connack_duration_ms": {},
    "eventhorizon_coap_active_request_exchanges": {},
    "eventhorizon_coap_request_exchange_duration_ms": {
        "outcome": {"response_sent", "terminated"}},
    "eventhorizon_coap_active_con_response_exchanges": {},
    "eventhorizon_coap_con_response_exchange_duration_ms": {
        "outcome": {"ack_received", "rst_received", "retry_exhausted"}},
    "eventhorizon_upnp_active_description_responses": {},
    "eventhorizon_upnp_description_stream_duration_ms": {
        "outcome": {"completed", "terminated"}},
}

# Always-present labels added by Prometheus itself, plus the histogram bucket label.
UNIVERSAL_LABELS = {"job", "instance", "le"}

# Families scraped from other jobs (cadvisor), not produced by the exporter.
# Present only when a cost/field compose overlay is running.
EXTERNAL_PREFIXES = ("container_", "machine_", "cadvisor_", "process_", "go_", "up")

PROMQL_KEYWORDS = {
    "by", "without", "on", "ignoring", "group_left", "group_right", "offset",
    "bool", "and", "or", "unless", "le", "inf", "nan", "start", "end", "atan2",
}
PROMQL_FUNCS = {
    "abs", "absent", "absent_over_time", "avg", "avg_over_time", "ceil", "changes",
    "clamp", "clamp_max", "clamp_min", "count", "count_over_time", "count_values",
    "day_of_month", "day_of_week", "day_of_year", "days_in_month", "delta", "deriv",
    "exp", "floor", "group", "histogram_quantile", "histogram_count", "histogram_sum",
    "holt_winters", "hour", "idelta", "increase", "irate", "label_join",
    "label_replace", "last_over_time", "ln", "log10", "log2", "max", "max_over_time",
    "min", "min_over_time", "minute", "month", "predict_linear", "present_over_time",
    "quantile", "quantile_over_time", "rate", "resets", "round", "scalar", "sgn",
    "sort", "sort_desc", "sqrt", "stddev", "stddev_over_time", "stdvar",
    "stdvar_over_time", "sum", "sum_over_time", "time", "timestamp", "topk",
    "bottomk", "vector", "year", "pi", "sin", "cos", "tan", "rad", "deg",
}

IDENT = re.compile(r"[A-Za-z_:][A-Za-z0-9_:]*")
STRING = re.compile(r"'[^']*'|\"[^\"]*\"|`[^`]*`")
LABEL_KEY = re.compile(r"[{,]\s*[A-Za-z_][A-Za-z0-9_]*\s*(?==|!=|=~|!~)")
# `sum by (protocol, action) (...)` -- the parenthesised list holds label names,
# not metric names. Same for without/on/ignoring/group_left/group_right.
GROUPING = re.compile(
    r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)", re.IGNORECASE
)
GRAFANA_VAR = re.compile(r"\$__?[A-Za-z_]+|\$\{[^}]*\}")
DURATION = re.compile(r"\b\d+[smhdwy]\b")


def families_in(expr: str) -> set[str]:
    """Extract candidate metric-family names from a PromQL expression."""
    e = GRAFANA_VAR.sub(" ", expr)
    e = STRING.sub(" ", e)
    e = GROUPING.sub(" ", e)
    e = LABEL_KEY.sub(" ", e)
    e = DURATION.sub(" ", e)
    out = set()
    for m in IDENT.finditer(e):
        name = m.group(0)
        if name in PROMQL_KEYWORDS or name in PROMQL_FUNCS:
            continue
        # skip an identifier immediately followed by '(' -- a function call
        if e[m.end():m.end() + 1] == "(":
            continue
        out.add(name)
    return out


def base_family(name: str) -> str:
    for suffix in ("_bucket", "_sum", "_count"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


SELECTOR = re.compile(r"([A-Za-z_:][A-Za-z0-9_:]*)\s*\{([^}]*)\}")
MATCHER = re.compile(
    r'([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"'
)
# Only alternation is decomposable; anything else is a real regex we won't judge.
PLAIN_ALTERNATION = re.compile(r"^[A-Za-z0-9_|.-]*$")


def label_findings(expr: str):
    """Yield (family, label, value, kind) for label names/values outside the
    closed schema. Negative matchers are checked for label name only."""
    e = GRAFANA_VAR.sub(" ", expr)
    for sel in SELECTOR.finditer(e):
        fam = base_family(sel.group(1))
        if fam.startswith(EXTERNAL_PREFIXES) or fam not in LABELS:
            continue  # unknown families are already reported by the family check
        schema = LABELS[fam]
        for lm in MATCHER.finditer(sel.group(2)):
            label, op, value = lm.group(1), lm.group(2), lm.group(3)
            if label in UNIVERSAL_LABELS:
                continue
            if label not in schema:
                yield fam, label, value, "label"
                continue
            if op in ("!=", "!~"):
                continue  # excluding a value need not mean it exists
            allowed = schema[label]
            if op == "=":
                if value not in allowed:
                    yield fam, label, value, "value"
            elif PLAIN_ALTERNATION.match(value):
                for alt in value.split("|"):
                    if alt and alt not in allowed:
                        yield fam, label, alt, "value"


def walk(panels, acc):
    for p in panels:
        acc.append(p)
        if p.get("panels"):
            walk(p["panels"], acc)


def check(path: Path):
    """Return (findings, referenced_families) for one dashboard file."""
    data = json.loads(path.read_text())
    flat: list = []
    walk(data.get("panels", []), flat)

    findings = []
    referenced: set[str] = set()
    for panel in flat:
        for target in panel.get("targets") or []:
            expr = target.get("expr")
            if not expr:
                continue
            for raw in sorted(families_in(expr)):
                fam = base_family(raw)
                if fam.startswith(EXTERNAL_PREFIXES):
                    continue
                referenced.add(fam)
                if fam not in LIVE:
                    findings.append({
                        "kind": "family",
                        "panel": panel.get("title") or f"<{panel.get('type')} id={panel.get('id')}>",
                        "detail": f"'{raw}' is not a live metric family",
                        "expr": " ".join(expr.split())[:140],
                    })
            for fam, label, value, kind in label_findings(expr):
                what = ("label '%s' does not exist on %s" % (label, fam) if kind == "label"
                        else "%s{%s=\"%s\"} is not an initialized label value" % (fam, label, value))
                findings.append({
                    "kind": kind,
                    "panel": panel.get("title") or f"<{panel.get('type')} id={panel.get('id')}>",
                    "detail": what,
                    "expr": " ".join(expr.split())[:140],
                })
    return data, findings, referenced


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", action="store_true", help="emit machine-readable output")
    args = ap.parse_args()

    files = sorted(DASHBOARD_DIR.glob("*.json"))
    if not files:
        print(f"no dashboards found under {DASHBOARD_DIR}", file=sys.stderr)
        return 1

    report = []
    all_referenced: set[str] = set()
    fatal = 0

    for path in files:
        data, findings, referenced = check(path)
        all_referenced |= referenced
        report.append({
            "file": path.name, "title": data.get("title"),
            "findings": findings,
        })
        if findings:
            fatal += len(findings)

    uncovered = sorted(LIVE - all_referenced)

    if args.json:
        print(json.dumps({
            "dashboards": report,
            "uncovered_families": uncovered,
            "fatal": fatal,
        }, indent=2))
        return 1 if fatal else 0

    for entry in report:
        status = "OK" if not entry["findings"] else "FAIL"
        print(f"[{status:4}] {entry['file']}  (CURRENT)")
        print(f"         {entry['title']}")
        for f in entry["findings"]:
            print(f"         FAIL[{f['kind']}]: {f['detail']}")
            print(f"               panel: {f['panel']}")
            print(f"               expr:  {f['expr']}")
        print()

    print(f"Live metric families (METRICS.md): {len(LIVE)}")
    print(f"Referenced by current dashboards:  {len(LIVE) - len(uncovered)}")
    if uncovered:
        print("Families with no panel in any current dashboard:")
        for f in uncovered:
            print(f"  - {f}")
    else:
        print("Every live metric family is covered by at least one current-dashboard panel.")
    print()

    if fatal:
        print(f"RESULT: FAIL - {fatal} finding(s) in current dashboards")
        return 1
    print("RESULT: PASS - no findings in current dashboards")
    return 0


if __name__ == "__main__":
    sys.exit(main())
