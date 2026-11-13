"""Primo livello della pipeline: extractor Prometheus e regole, senza agente e senza token.

Prova sugli scenari gia' misurati:
    python monitor.py --scenarios normale,cpu,lock,connessioni,misto,primi_segnali

Prova togliendo una regola, per creare un caso sconosciuto:
    python monitor.py --scenarios cpu --disable cpu_saturation

Ascolto continuo, una lettura al minuto sugli ultimi 5 minuti:
    python monitor.py --every 60

Ogni analisi viene aggiunta a monitor_events.jsonl.
"""

import argparse
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from rules import active_rules, evaluate, load_learned

PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://35.84.236.220:9090")
DB_WORKER = os.getenv("DB_WORKER", "worker-4")

SCENARIOS = {
    "normale": ("20261104_120007", "step_05", "nessuna"),
    "cpu": ("20261104_193045", "step_06", "cpu"),
    "lock": ("20261104_182411", "step_04", "lock"),
    "connessioni": ("20261104_190132", "step_03", "connessioni"),
    "misto": ("20261105_164653", "step_02", "cpu"),
    "primi_segnali": ("20261104_193045", "step_03", "cpu"),
}


def metric_queries(window):
    w = window
    learned = {name: promql.replace("{w}", w) for name, promql in load_learned()["metrics"].items()}
    return {
        **learned,
        "cpu_pct": f'100 * (1 - avg by (name) (rate(node_cpu_seconds_total{{mode="idle"}}[{w}])))',
        "memory_pct": (
            f"100 * (1 - avg by (name) (avg_over_time(node_memory_MemAvailable_bytes[{w}]))"
            f" / avg by (name) (avg_over_time(node_memory_MemTotal_bytes[{w}])))"
        ),
        "cpu_cores": 'count by (name) (node_cpu_seconds_total{mode="idle"})',
        "memory_total_gb": "avg by (name) (node_memory_MemTotal_bytes) / 1024 / 1024 / 1024",
        "qps": f"sum by (name) (rate(mysql_global_status_queries[{w}]))",
        "threads_running": f"avg by (name) (avg_over_time(mysql_global_status_threads_running[{w}]))",
        "connections": f"avg by (name) (avg_over_time(mysql_global_status_threads_connected[{w}]))",
        "max_connections": "avg by (name) (mysql_global_variables_max_connections)",
        "lock_ms_s": f"sum by (name) (rate(mysql_global_status_innodb_row_lock_time[{w}]))",
        "slow_queries_s": f"sum by (name) (rate(mysql_global_status_slow_queries[{w}]))",
    }


def query_prometheus(promql, eval_time=None):
    params = {"query": promql}
    if eval_time is not None:
        params["time"] = eval_time
    url = f"{PROMETHEUS_URL}/api/v1/query?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as response:
        result = json.load(response)
    if result.get("status") != "success":
        raise RuntimeError(result.get("error", "Errore Prometheus"))
    return result["data"]["result"]


def extract(window, eval_time=None):
    """Legge tutte le metriche e le organizza per macchina."""
    machines = {}
    errors = []
    for metric, promql in metric_queries(window).items():
        try:
            rows = query_prometheus(promql, eval_time)
        except Exception as error:  # Un dato mancante deve arrivare alle regole come mancante.
            errors.append(f"{metric}: {error}")
            continue
        for row in rows:
            value = float(row["value"][1])
            if math.isfinite(value):
                name = row["metric"].get("name", "?")
                machines.setdefault(name, {})[metric] = round(value, 2)
    return machines, errors


def analyze(window, eval_time, disabled):
    machines, errors = extract(window, eval_time)
    metrics = machines.get(DB_WORKER, {})
    diagnosis = evaluate(metrics, disabled)
    return {
        "time": eval_time or time.time(), "window": window, "worker": DB_WORKER,
        "known_pattern": diagnosis is not None, "diagnosis": diagnosis,
        "next_step": "fine" if diagnosis else "agente",
        "metrics": metrics, "machines": machines, "errors": errors,
    }


def scenario_time(run, phase):
    path = PROJECT_DIR / "runs" / run / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    for item in manifest["phases"]:
        if item["phase"] == phase:
            return item["end_epoch"] - 5
    raise ValueError(f"Fase {phase} non trovata in {path}")


def save(event):
    with (PROJECT_DIR / "monitor_events.jsonl").open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, ensure_ascii=False) + "\n")


def describe(event):
    if event["known_pattern"]:
        d = event["diagnosis"]
        return f"{d['state']} | causa={d['cause']} | regola={d['rule']}"
    return "PATTERN SCONOSCIUTO -> agente"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--scenarios", help="scenari separati da virgola")
    parser.add_argument("--window", default="5m", help="periodo su cui calcolare le medie")
    parser.add_argument("--disable", default="", help="regole da togliere, separate da virgola")
    parser.add_argument("--every", type=int, default=0, help="secondi tra due letture")
    args = parser.parse_args()

    disabled = {name.strip() for name in args.disable.split(",") if name.strip()}
    unknown = disabled - {rule.__name__ for rule in active_rules()}
    if unknown:
        raise SystemExit(f"Regole inesistenti: {sorted(unknown)}")

    if args.scenarios:
        correct = 0
        names = [name.strip() for name in args.scenarios.split(",")]
        for name in names:
            run, phase, expected = SCENARIOS[name]
            event = analyze(args.window, scenario_time(run, phase), disabled)
            event["scenario"] = name
            save(event)
            cause = event["diagnosis"]["cause"] if event["diagnosis"] else None
            match = cause == expected
            correct += match
            print(f"{name:<14} {describe(event):<58} atteso={expected} "
                  f"{'OK' if match else 'DIVERSO'}")
            print(f"{'':<14} {json.dumps(event['metrics'])}")
            if event["errors"]:
                print(f"{'':<14} errori: {event['errors']}")
        print(f"\nCause riconosciute dalle regole: {correct} su {len(names)}")
        return

    while True:
        event = analyze(args.window, None, disabled)
        save(event)
        print(f"{datetime.now():%H:%M:%S} {describe(event)} | {json.dumps(event['metrics'])}")
        if not args.every:
            return
        time.sleep(args.every)


if __name__ == "__main__":
    main()