"""Secondo livello della pipeline: primo agente per i pattern che le regole non conoscono.

Il sistema raccoglie i dati e li passa all'agente, che ragiona senza strumenti e senza MCP.

    python first_agent.py --scenarios connessioni
    python first_agent.py --scenarios cpu --disable cpu_saturation

Le prove vengono aggiunte a agent_events.jsonl.
"""

import argparse
import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic

from monitor import DB_WORKER, SCENARIOS, analyze, scenario_time

PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

MODEL = os.getenv("FIRST_AGENT_MODEL", "claude-haiku-5-5")

INSTRUCTIONS = """Sei un analista di database. Le regole automatiche non hanno riconosciuto questa situazione.
Analizza soltanto i dati forniti: non hai strumenti e non puoi chiederne altri.

Ambiente
- Prometheus monitora piu' macchine. MySQL gira solo su {worker}: e' normale che le altre macchine abbiano carico e query diversi, quindi i confronti tra macchine non indicano un problema.
- I dati sono medie o ritmi sulla finestra indicata: cpu_pct e memory_pct in percentuale, qps in query al secondo, threads_running e connections come valori medi, lock_ms_s in millisecondi di attesa al secondo, slow_queries_s in query al secondo.
- Una metrica assente significa che quel dato non e' disponibile: non trattarla come zero.

Rispondi solo con un oggetto JSON valido:
{{
  "sotto_stress": true,
  "problema": "descrizione breve",
  "confidenza": "bassa | media | alta",
  "prove": [{{"metrica": "nome", "macchina": "{worker}", "valore": 0.0}}],
  "dati_mancanti": ["nome metrica"],
  "azione_consigliata": "una frase",
  "spiegazione": "massimo tre frasi"
}}
In "prove" riporta solo valori numerici presenti nei dati, senza cambiarli. I dati assenti vanno in "dati_mancanti"."""


def build_messages(event):
    data = {"finestra": event["window"], "macchine": event["machines"]}
    question = (
        "Dati raccolti:\n" + json.dumps(data, ensure_ascii=False, indent=2)
        + "\n\nIl database e' sotto stress? Qual e' il problema?"
    )
    return [("system", INSTRUCTIONS.format(worker=DB_WORKER)), ("human", question)]


def response_text(content):
    if isinstance(content, str):
        return content
    return "\n".join(item.get("text", "") for item in content
                     if isinstance(item, dict) and item.get("type") == "text")


def parse_report(text):
    begin, end = text.find("{"), text.rfind("}")
    if begin < 0 or end <= begin:
        return None
    try:
        report = json.loads(text[begin:end + 1])
    except ValueError:
        return None
    return report if isinstance(report, dict) else None


def check_proofs(report, event):
    """Ogni prova deve corrispondere a un valore che il sistema ha davvero passato."""
    verified, wrong = 0, []
    for proof in report.get("prove") or []:
        try:
            machine = proof["macchina"]
            metric = proof["metrica"]
            value = float(proof["valore"])
            real = event["machines"][machine][metric]
        except (KeyError, TypeError, ValueError):
            wrong.append(proof)
            continue
        if abs(real - value) <= 0.0051:
            verified += 1
        else:
            wrong.append(proof)
    return verified, wrong


def run_agent(event, model=None):
    # Questo modello non accetta il parametro temperature.
    client = model or ChatAnthropic(model=MODEL, max_tokens=3000)
    started = time.perf_counter()
    response = client.invoke(build_messages(event))
    seconds = round(time.perf_counter() - started, 2)
    text = response_text(response.content)
    report = parse_report(text)
    verified, wrong = check_proofs(report, event) if report else (0, [])
    return {
        "model": MODEL, "seconds": seconds, "usage": getattr(response, "usage_metadata", None),
        "json_valid": report is not None, "report": report, "raw_text": text,
        "proofs_verified": verified, "proofs_wrong": wrong,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--scenarios", required=True)
    parser.add_argument("--window", default="5m")
    parser.add_argument("--disable", default="", help="regole da togliere, separate da virgola")
    args = parser.parse_args()
    disabled = {name.strip() for name in args.disable.split(",") if name.strip()}

    for name in [item.strip() for item in args.scenarios.split(",")]:
        run, phase, expected = SCENARIOS[name]
        event = analyze(args.window, scenario_time(run, phase), disabled)
        if event["known_pattern"]:
            print(f"{name}: riconosciuto dalla regola {event['diagnosis']['rule']}, agente non chiamato")
            continue
        result = run_agent(event)
        with (PROJECT_DIR / "agent_events.jsonl").open("a", encoding="utf-8") as file:
            file.write(json.dumps({"scenario": name, "expected_cause": expected, "disabled": sorted(disabled),
                                   "event": event, **result}, ensure_ascii=False, default=str) + "\n")
        usage = result["usage"] or {}
        print(f"\n{name}: {result['seconds']} s | {usage.get('total_tokens', '?')} token | "
              f"prove corrette {result['proofs_verified']}, sbagliate {len(result['proofs_wrong'])}")
        print(json.dumps(result["report"], ensure_ascii=False, indent=2) if result["report"] else result["raw_text"])


if __name__ == "__main__":
    main()