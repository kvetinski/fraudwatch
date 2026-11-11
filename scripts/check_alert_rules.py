#!/usr/bin/env python3
"""Print FraudWatch alert rules evaluated by the running local Prometheus."""
import json
import sys
from urllib.error import URLError
from urllib.request import urlopen

BASE = "http://127.0.0.1:9090"


def fetch(path):
    with urlopen(BASE + path, timeout=5) as resp:
        result = json.load(resp)
    if result.get("status") != "success":
        raise RuntimeError(f"Prometheus API failed: {path}")
    return result["data"]


def main():
    groups = fetch("/api/v1/rules?type=alert")["groups"]
    rules = [r for g in groups for r in g.get("rules", [])
             if g.get("name") == "fraudwatch-operational"]
    print(f"FraudWatch alert rules loaded: {len(rules)}")
    for rule in rules:
        print(f"  {rule['name']:<40} {rule.get('state', 'unknown')}")
    if len(rules) != 10:
        raise RuntimeError("Expected 10 FraudWatch alert rules; check configuration")
    firing = fetch("/api/v1/alerts")["alerts"]
    ours = [a for a in firing if a.get("labels", {}).get("service") == "fraudwatch"]
    print(f"\nActive FraudWatch alerts: {len(ours)}")
    for a in ours:
        print(f"  {a['labels'].get('alertname')}: {a['state']}")
    print("\nNote: without Alertmanager, these alerts are evaluated but no notifications are delivered.")


if __name__ == "__main__":
    try:
        main()
    except (URLError, KeyError, RuntimeError, TimeoutError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
