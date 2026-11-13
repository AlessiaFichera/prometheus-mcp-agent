"""Scarica le metriche Prometheus di un test e individua il gradino limite.

    python analyze_limit.py runs/<run_id>

Crea prometheus.csv (serie temporali), phases_metrics.csv (medie per fase) e
limit_report.md. Le metriche sono scaricate a fine test con query_range, quindi
coprono esattamente l'intervallo del test e possono essere riscaricate.
"""

import argparse
import csv
import json
import math
import os
import urllib.parse
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://35.84.236.220:9090")
LABEL = "name"
RATE_WINDOW = "2m"  # Con uno scrape di circa 30 s, 1m contiene troppi pochi punti.
RATE_WINDOW_S = 120
QUERY_STEP_S = 15

W = RATE_WINDOW
METRICS = {
    "qps": f"sum by ({LABEL}) (rate(mysql_global_status_queries[{W}]))",
    "threads_running": f"sum by ({LABEL}) (mysql_global_status_threads_running)",
    "threads_connected": f"sum by ({LABEL}) (mysql_global_status_threads_connected)",
    "connection_usage_pct": (
        f"100 * sum by ({LABEL}) (mysql_global_status_threads_connected)"
        f" / sum by ({LABEL}) (mysql_global_variables_max_connections)"
    ),
    "buffer_pool_hit_ratio_pct": (
        f"100 * (1 - sum by ({LABEL}) (rate(mysql_global_status_innodb_buffer_pool_reads[{W}]))"
        f" / sum by ({LABEL}) (rate(mysql_global_status_innodb_buffer_pool_read_requests[{W}])))"
    ),
    "row_lock_time_ms_s": f"sum by ({LABEL}) (rate(mysql_global_status_innodb_row_lock_time[{W}]))",
    "row_lock_waits_s": f"sum by ({LABEL}) (rate(mysql_global_status_innodb_row_lock_waits[{W}]))",
    "slow_queries_s": f"sum by ({LABEL}) (rate(mysql_global_status_slow_queries[{W}]))",
    "aborted_connects_s": f"sum by ({LABEL}) (rate(mysql_global_status_aborted_connects[{W}]))",
    "selects_s": f'sum by ({LABEL}) (rate(mysql_global_status_commands_total{{command="select"}}[{W}]))',
    "inserts_s": f'sum by ({LABEL}) (rate(mysql_global_status_commands_total{{command="insert"}}[{W}]))',
    "updates_s": f'sum by ({LABEL}) (rate(mysql_global_status_commands_total{{command="update"}}[{W}]))',
    "cpu_busy_pct": (
        f'100 * (1 - avg by ({LABEL}) (rate(node_cpu_seconds_total{{mode="idle"}}[{W}])))'
    ),
}

# Le metriche "conteggio" si sommano tra i worker; le altre si mediano.
AVERAGED = {"connection_usage_pct", "buffer_pool_hit_ratio_pct", "cpu_busy_pct"}


def query_range(promql, start, end):
    params = urllib.parse.urlencode({
        "query": promql, "start": start, "end": end, "step": QUERY_STEP_S,
    })
    request = urllib.request.Request(
        f"{PROMETHEUS_URL}/api/v1/query_range?{params}",
        headers={"Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        result = json.load(response)
    if result.get("status") != "success":
        raise RuntimeError(result.get("error", "Errore Prometheus"))
    return result["data"]["result"]


def download(run_dir, phases):
    start = min(p["begin_epoch"] for p in phases)
    end = max(p["end_epoch"] for p in phases) + RATE_WINDOW_S
    samples, missing = [], []
    for metric, promql in METRICS.items():
        try:
            series = query_range(promql, start, end)
        except Exception as error:
            print(f"  {metric}: errore ({error})")
            series = []
        if not series:
            missing.append(metric)
        for item in series:
            worker = item["metric"].get(LABEL, "?")
            for timestamp, value in item["values"]:
                number = float(value)
                if math.isfinite(number):
                    samples.append((float(timestamp), metric, worker, number))
    with (run_dir / "prometheus.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["epoch", "metric", "worker", "value"])
        writer.writerows(samples)
    return samples, missing


def phase_metrics(phases, samples):
    """Media per fase, escludendo la parte iniziale ancora "sporcata" dalla fase
    precedente a causa della finestra di rate."""
    rows = []
    for phase in phases:
        begin, end = phase["begin_epoch"], phase["end_epoch"]
        settled = begin + RATE_WINDOW_S
        if settled >= end:  # Fase piu' corta della finestra: dato solo indicativo.
            settled = begin
        row = {"phase": phase["phase"], "concurrency": phase["concurrency"]}
        for metric in METRICS:
            selected = [s for s in samples
                        if s[1] == metric and settled <= s[0] <= end]
            by_time = {}
            for timestamp, _, _, value in selected:
                by_time.setdefault(timestamp, []).append(value)
            if not by_time:
                row[metric] = ""
                continue
            totals = [sum(v) / len(v) if metric in AVERAGED else sum(v)
                      for v in by_time.values()]
            row[metric] = round(sum(totals) / len(totals), 4)
        rows.append(row)
    return rows


def find_limit(steps):
    """Regole descrittive: indicano un gradino candidato, non una causa."""
    if not steps:
        return None, ["Nessun gradino di carico completato."]
    base_p95 = steps[0]["p95_s"]
    previous = None
    for step in steps:
        reasons = []
        if step["error_pct"] > 1:
            reasons.append(f"errori {step['error_pct']}% (> 1%)")
        if base_p95 and step["p95_s"] and step["p95_s"] > 2 * base_p95:
            reasons.append(
                f"p95 {step['p95_s']:.3f} s (> 2 volte il primo gradino, {base_p95:.3f} s)"
            )
        if previous and previous["throughput_qps"] > 0:
            gain = step["throughput_qps"] / previous["throughput_qps"] - 1
            more = step["concurrency"] / previous["concurrency"] - 1
            if more > 0 and gain < 0.25 * more:
                reasons.append(
                    f"throughput {100 * gain:+.0f}% con {100 * more:+.0f}% di connessioni"
                )
        if reasons:
            return step, reasons
        previous = step
    return None, ["Nessun limite raggiunto: il test non ha saturato il sistema."]


def fmt(value, digits=3):
    return "n/d" if value in (None, "") else f"{value:.{digits}f}"


def worker_lines(steps, samples):
    """Connessioni e CPU per worker nell'ultimo gradino: il totale puo' nascondere un nodo saturo."""
    if not steps:
        return []
    last = steps[-1]
    begin = last["begin_epoch"] + RATE_WINDOW_S
    if begin >= last["end_epoch"]:
        begin = last["begin_epoch"]
    lines = ["", f"## Worker nell'ultimo gradino ({last['concurrency']} connessioni)", "",
             "| Worker | Connessioni | Thread running | CPU % | Lock ms/s |", "|---|---|---|---|---|"]
    mean = {}
    for epoch, metric, worker, value in samples:
        if begin <= epoch <= last["end_epoch"]:
            mean.setdefault((worker, metric), []).append(value)
    for worker in sorted({key[0] for key in mean}):
        cells = []
        for metric in ("threads_connected", "threads_running", "cpu_busy_pct", "row_lock_time_ms_s"):
            values = mean.get((worker, metric))
            cells.append(fmt(sum(values) / len(values), 1) if values else "n/d")
        lines.append(f"| {worker} | " + " | ".join(cells) + " |")
    return lines


def build_report(manifest, steps, limit, reasons, rows, missing, samples):
    by_phase = {row["phase"]: row for row in rows}
    lines = [
        f"# Limite del database - run {manifest['run_id']}", "",
        f"Motivo di fine test: `{manifest['stop_reason']}`  ",
        f"Mix: `{manifest['mix']}`", "",
        "| Conn. | QPS | p50 s | p95 s | p99 s | Errori % | SELECT 1 p50 s | CPU client % "
        "| Thread running | Lock ms/s | CPU DB % |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for step in steps:
        row = by_phase.get(step["phase"], {})
        lines.append(
            f"| {step['concurrency']} | {step['throughput_qps']:.1f} | {fmt(step['p50_s'])} "
            f"| {fmt(step['p95_s'])} | {fmt(step['p99_s'])} | {step['error_pct']:.2f} "
            f"| {fmt(step['probe_p50_s'])} | {step['client_cpu_pct']:.0f} "
            f"| {fmt(row.get('threads_running'), 1)} "
            f"| {fmt(row.get('row_lock_time_ms_s'), 1)} | {fmt(row.get('cpu_busy_pct'), 1)} |"
        )
    lines += ["", "## Gradino candidato", ""]
    if limit:
        lines.append(f"**{limit['concurrency']} connessioni.** Motivi:")
    lines += [f"- {reason}" for reason in reasons]
    busy = [s for s in steps if s["client_cpu_pct"] >= 70]
    if busy:
        lines += ["", "**Attenzione:** CPU del client >= 70% con "
                  f"{[s['concurrency'] for s in busy]} connessioni: il limite potrebbe "
                  "essere del generatore, non del database."]
    if limit and limit.get("probe_p50_s") and steps[0].get("probe_p50_s"):
        if limit["probe_p50_s"] > 2 * steps[0]["probe_p50_s"]:
            lines += ["", "Anche `SELECT 1` rallenta: il limite riguarda server o rete, "
                      "non solo le query pesanti."]
    if missing:
        lines += ["", f"Metriche non disponibili in Prometheus: {', '.join(missing)}."]
    if limit:
        lines += ["", "Il gradino e' un candidato: confermalo ripetendo il test almeno 3 volte "
                  "e controlla che il recupero torni ai valori della baseline."]
    lines += worker_lines(steps, samples) + [""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args()

    manifest = json.loads((args.run_dir / "manifest.json").read_text(encoding="utf-8"))
    phases = manifest["phases"]
    steps = [p for p in phases if p["phase"].startswith("step_")]

    samples, missing = download(args.run_dir, phases)
    rows = phase_metrics(phases, samples)
    with (args.run_dir / "phases_metrics.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["phase", "concurrency", *METRICS])
        writer.writeheader()
        writer.writerows(rows)

    limit, reasons = find_limit(steps)
    report = build_report(manifest, steps, limit, reasons, rows, missing, samples)
    (args.run_dir / "limit_report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()