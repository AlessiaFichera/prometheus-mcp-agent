"""Terzo livello della pipeline: agente di esplorazione, chiamato quando la persona sceglie Esplora.

Cambia l'harness: l'agente riceve la prima ipotesi, che non ha convinto, e usa il server
MCP per cercare da solo su Prometheus. Il numero di passi e' limitato.
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent

from first_agent import parse_report, response_text
from monitor import DB_WORKER

PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

MODEL = os.getenv("EXPLORE_AGENT_MODEL", "claude-sonnet-5-5")
RECURSION_LIMIT = 21  # Circa 10 giri di ragionamento e strumento.
TIMEOUT_SECONDS = 300

INSTRUCTIONS = """Sei un analista esperto di database. Un primo agente ha analizzato un problema su MySQL, ma la persona non e' convinta della sua risposta.
Usa lo strumento query_prometheus per cercare da solo le informazioni. Puoi interrogare metriche MySQL (mysql_*) e della macchina (node_*).
MySQL serve le richieste su {worker}. Le macchine sono distinte dalla label `name`.

Regole
- Non fidarti della prima risposta: verificala con i dati.
- I contatori crescono nel tempo: per il ritmo attuale usa rate(...[2m]).
- Se una metrica o una macchina manca dai risultati, dichiaralo: non considerarla zero.
- Non inventare valori. Se i dati non bastano, dillo.
- Fai poche query mirate e chiedi solo le serie necessarie.

Alla fine rispondi solo con un oggetto JSON valido:
{{
  "trovato": true,
  "problema": "descrizione breve",
  "conferma_prima_ipotesi": "si | no | in_parte",
  "confidenza": "bassa | media | alta",
  "prove": [{{"metrica": "nome o query", "macchina": "{worker}", "valore": 0.0}}],
  "dati_mancanti": ["nome metrica"],
  "azione_consigliata": "una frase",
  "regola_proposta": "se ... allora ..., oppure null",
  "spiegazione": "massimo tre frasi"
}}
In "prove" riporta solo valori numerici restituiti dagli strumenti. Se non trovi una soluzione, usa "trovato": false."""


def build_question(event, first_result):
    first = first_result.get("report") or first_result.get("raw_text")
    return (
        "La risposta del primo agente non mi convince:\n"
        + json.dumps(first, ensure_ascii=False, indent=2)
        + "\n\nQuesti sono i dati gia' raccolti:\n"
        + json.dumps({"finestra": event["window"], "macchine": event["machines"]},
                     ensure_ascii=False, indent=2)
        + "\n\nCerca le informazioni che servono e dammi una nuova interpretazione."
    )


def collect_steps(messages):
    stats = {"model_calls": 0, "tool_calls": 0, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    steps, answer = [], ""
    for message in messages:
        kind = getattr(message, "type", "")
        if kind == "ai":
            stats["model_calls"] += 1
            usage = getattr(message, "usage_metadata", None) or {}
            for key in ("input_tokens", "output_tokens", "total_tokens"):
                stats[key] += usage.get(key, 0) or 0
            calls = getattr(message, "tool_calls", None) or []
            stats["tool_calls"] += len(calls)
            text = response_text(message.content)
            if text and not calls:
                answer = text
            steps.append({"type": "model", "text": text, "usage": dict(usage),
                          "tool_calls": [{"name": c.get("name"), "args": c.get("args")} for c in calls]})
        elif kind == "tool":
            steps.append({"type": "tool", "failed": getattr(message, "status", "") == "error",
                          "content": response_text(message.content)})
    return stats, steps, answer


def known_values(event, steps):
    """Valori effettivamente visti dall'agente: dati passati e risposte di Prometheus."""
    values = []
    for machine, metrics in event["machines"].items():
        values += [(machine, float(value)) for value in metrics.values()]
    for step in steps:
        if step["type"] != "tool":
            continue
        try:
            rows = json.loads(step["content"])["data"]["result"]
        except (TypeError, ValueError, KeyError):
            continue
        for row in rows:
            try:
                values.append((row["metric"].get("name"), float(row["value"][1])))
            except (KeyError, IndexError, TypeError, ValueError):
                continue
    return values


def check_proofs(report, event, steps):
    values = known_values(event, steps)
    verified, wrong = 0, []
    for proof in report.get("prove") or []:
        try:
            machine, value = proof["macchina"], float(proof["valore"])
        except (KeyError, TypeError, ValueError):
            wrong.append(proof)
            continue
        if any(worker == machine and abs(real - value) <= 0.0051 for worker, real in values):
            verified += 1
        else:
            wrong.append(proof)
    return verified, wrong


async def explore(event, first_result, eval_time=None):
    env = dict(os.environ)
    if eval_time is not None:
        env["PROMETHEUS_EVAL_TIME"] = str(eval_time)
    client = MultiServerMCPClient({"prometheus": {
        "transport": "stdio", "command": sys.executable,
        "args": [str(PROJECT_DIR / "prometheus_mcp_server.py")], "env": env,
    }})
    messages = [
        {"role": "system", "content": INSTRUCTIONS.format(worker=DB_WORKER)},
        {"role": "user", "content": build_question(event, first_result)},
    ]
    started = time.perf_counter()
    last_state = None
    status, error = "OK", ""

    async def consume(agent):
        nonlocal last_state
        async for state in agent.astream({"messages": messages}, stream_mode="values",
                                          config={"recursion_limit": RECURSION_LIMIT}):
            last_state = state

    try:
        tools = await client.get_tools()
        model = ChatAnthropic(model=MODEL, max_tokens=4000)
        await asyncio.wait_for(consume(create_react_agent(model, tools)), TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        status, error = "TIMEOUT", f"Oltre {TIMEOUT_SECONDS} s"
    except Exception as exc:  # Il fallimento va registrato e comunicato alla persona.
        status, error = type(exc).__name__, str(exc)[:500]

    stats, steps, answer = collect_steps(last_state["messages"] if last_state else [])
    report = parse_report(answer)
    verified, wrong = check_proofs(report, event, steps) if report else (0, [])
    if status == "OK" and not report:
        status = "NO_REPORT"
    return {"model": MODEL, "status": status, "error": error,
            "seconds": round(time.perf_counter() - started, 2), "report": report, "raw_text": answer,
            "proofs_verified": verified, "proofs_wrong": wrong, "steps": steps, **stats}
