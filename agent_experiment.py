"""Esperimenti sull'agente: stessa domanda e stessi scenari, harness diverso.

V0: solo il tool Prometheus e una domanda aperta, nessuna istruzione.
V2: come V0, piu' un contesto generale su ambiente e metriche.
V3: come V2, piu' soglie di gravita', report JSON e controllo automatico dei numeri.
Il server MCP risponde come se fossimo al momento dello scenario: ogni prova vede gli
stessi dati, quindi le versioni sono confrontabili.

Una prova su due scenari:
    python agent_experiment.py --version V2 --scenarios normale,cpu

Esperimento completo (20 esecuzioni):
    python agent_experiment.py --version V2 --repeats 5

Risultati in agent_runs/<id>/: results.csv e traces.jsonl
"""

import argparse
import asyncio
import csv
import json
import os
import statistics
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_groq import ChatGroq
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent

PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

PROVIDER = os.getenv("LLM_PROVIDER", "gemini").lower()
MODELS = {"gemini": "gemini-2.5-flash", "groq": "llama-3.3-70b-versatile"}
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://35.84.236.220:9090")

# Nessun suggerimento: e' il punto di partenza dell'esperimento.
PROMPT = (
    "Hai accesso a Prometheus tramite uno strumento. "
    "Il database MySQL e' sotto stress? Perche'? Produci un report."
)

# Conoscenza generale: vale per ogni scenario e non dice quale caso c'e' davanti.
# Non contiene soglie, che arriveranno con una versione successiva.
CONTEXT_V2 = """Contesto dell'ambiente
- Prometheus monitora quattro macchine, distinte dalla label `name`: worker-1, worker-2, worker-3, worker-4.
- MySQL serve le richieste su worker-4: valuta lo stress soprattutto su questa macchina.
- Le metriche mysql_* vengono dal MySQL exporter; quelle node_* dalla macchina.

Come leggere le metriche
- I contatori crescono da quando il server e' partito, quindi un totale non descrive la situazione attuale. Usa rate(...[2m]) per il ritmo attuale.
- I valori istantanei, come thread attivi e connessioni, si leggono direttamente.
- Se una macchina manca dai risultati di una query, segnalalo come dato mancante: non significa che vada tutto bene. In particolare controlla se manca worker-4.

Metriche utili (query PromQL)
- CPU %: 100 * (1 - avg by (name) (rate(node_cpu_seconds_total{mode="idle"}[2m])))
- Query al secondo: sum by (name) (rate(mysql_global_status_queries[2m]))
- Thread attivi: mysql_global_status_threads_running
- Connessioni aperte: mysql_global_status_threads_connected
- Limite connessioni: mysql_global_variables_max_connections
- Attesa sui lock, ms/s: sum by (name) (rate(mysql_global_status_innodb_row_lock_time[2m]))
- Query lente al secondo: sum by (name) (rate(mysql_global_status_slow_queries[2m]))

Come ragionare
- Valuta le metriche insieme: una sola non basta. Anche molte connessioni possono essere normali.
- CPU molto alta con molti thread attivi indica saturazione della CPU.
- CPU bassa con lunghe attese sui lock indica contesa sui lock.
- Connessioni vicine al limite, o dati mancanti su quella macchina, indicano un limite di connessioni.
- Se nessun segnale e' presente, dichiara che il database non e' sotto stress.
"""

# Soglie iniziali, ricavate dai test di stress: vanno verificate su scenari nuovi.
CONTEXT_V3 = CONTEXT_V2 + """
Livelli di gravita' su worker-4
- CPU: sotto 75% normale; da 75% a 90% attenzione; oltre 90% critico.
- Lock: dividi i ms/s per 1000 per ottenere le connessioni mediamente in attesa. Sotto 1 trascurabile; da 1 a 5 attenzione; oltre 5 critico.
- Connessioni: sotto l'80% del limite normale; dall'80% al 95% attenzione; oltre il 95% critico.
- Lo stato complessivo e' il livello piu' grave tra quelli osservati.
- Un valore trascurabile o normale non e' una causa e non va indicato tra le cause.
- Se le metriche MySQL di worker-4 mancano, usa stato e causa "dati_mancanti". Nella spiegazione indica come ipotesi sia un limite di connessioni che blocca l'exporter sia un guasto dell'exporter.

Formato della risposta
Rispondi solo con un oggetto JSON valido, senza altro testo:
{
  "stato": "normale | attenzione | critico | dati_mancanti",
  "causa_principale": "nessuna | cpu | lock | connessioni | dati_mancanti",
  "cause_secondarie": ["cpu | lock | connessioni"],
  "prove": [{"metrica": "nome", "macchina": "worker-4", "valore": 0.0}],
  "spiegazione": "massimo tre frasi"
}
In "prove" riporta i valori cosi' come restituiti dagli strumenti, arrotondati al massimo a due decimali.
"""

SYSTEM_PROMPTS = {"V0": None, "V2": CONTEXT_V2, "V3": CONTEXT_V3}

# Etichette per la valutazione automatica dei report strutturati.
# Non vengono mai passate all'agente.
EXPECTED_LABELS = {
    "normale": ("normale", "nessuna"),
    "cpu": ("critico", "cpu"),
    "lock": ("critico", "lock"),
    "connessioni": ("dati_mancanti", "dati_mancanti"),
    "misto": ("critico", "cpu"),
    "primi_segnali": ("attenzione", "cpu"),
}

# La risposta attesa serve solo a valutare: non viene mai passata all'agente.
SCENARIOS = {
    "normale": {
        "run": "20261104_120007", "phase": "step_05",
        "expected": "Nessuno stress: CPU bassa e nessuna coda (16 connessioni, query leggere).",
    },
    "cpu": {
        "run": "20261104_193045", "phase": "step_06",
        "expected": "CPU di worker-4 satura (circa 100%) con coda di thread attivi.",
    },
    "lock": {
        "run": "20261104_182411", "phase": "step_04",
        "expected": "Contesa sui row lock (circa 46.900 ms/s), con CPU bassa e molti thread in attesa.",
    },
    "connessioni": {
        "run": "20261104_190132", "phase": "step_03",
        "expected": "Limite di connessioni raggiunto su worker-4 (circa 152, max 151), con CPU bassa.",
    },
    # Scenari nuovi: il contesto di V2 non e' stato scritto guardando questi due casi.
    "misto": {
        "run": "20261105_164653", "phase": "step_02",
        "expected": "CPU di worker-4 satura (circa 100%) con coda di thread; lock presenti ma secondari (circa 116 ms/s).",
    },
    "primi_segnali": {
        "run": "20261104_193045", "phase": "step_03",
        "expected": "Degrado iniziale: CPU di worker-4 circa 80%, non ancora satura, con prima crescita dei thread.",
    },
}

COLUMNS = [
    "run_id", "version", "provider", "model", "scenario", "repeat", "status", "seconds",
    "model_calls", "tool_calls", "tool_errors", "tool_empty", "input_tokens",
    "output_tokens", "total_tokens", "expected", "answer",
    "json_valido", "stato", "causa_principale", "cause_secondarie", "stato_corretto",
    "causa_corretta", "prove_totali", "prove_verificate", "prove_non_trovate",
    "diagnosi_corretta", "valori_inventati", "note",
]


def create_model():
    if PROVIDER == "gemini":
        return ChatGoogleGenerativeAI(model=MODELS[PROVIDER], temperature=0)
    if PROVIDER == "groq":
        return ChatGroq(model=MODELS[PROVIDER], temperature=0)
    raise ValueError(f"Provider non supportato: {PROVIDER}")


def scenario_time(name):
    """Momento di valutazione: fine del gradino, con la finestra di rate tutta dentro."""
    scenario = SCENARIOS[name]
    path = PROJECT_DIR / "runs" / scenario["run"] / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    for phase in manifest["phases"]:
        if phase["phase"] == scenario["phase"]:
            return phase["end_epoch"] - 5
    raise ValueError(f"Fase {scenario['phase']} non trovata in {path}")


def check_data(eval_time):
    """Controlla, fuori dall'agente, che Prometheus conservi ancora quei dati."""
    params = urllib.parse.urlencode({
        "query": "mysql_global_status_threads_connected", "time": eval_time,
    })
    with urllib.request.urlopen(f"{PROMETHEUS_URL}/api/v1/query?{params}", timeout=30) as response:
        return len(json.load(response)["data"]["result"])


def text_of(content):
    if isinstance(content, str):
        return content
    parts = []
    for item in content or []:
        if isinstance(item, str):
            parts.append(item)
        elif isinstance(item, dict) and item.get("type") == "text":
            parts.append(item.get("text", ""))
    return "\n".join(parts)


def tool_result_empty(content):
    try:
        return json.loads(content)["data"]["result"] == []
    except (TypeError, ValueError, KeyError):
        return False


def analyze_messages(messages):
    """Ricava dalla conversazione conteggi, token e passi eseguiti."""
    stats = {"model_calls": 0, "tool_calls": 0, "tool_errors": 0, "tool_empty": 0,
             "input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    steps = []
    answer = ""
    for message in messages:
        kind = getattr(message, "type", "")
        if kind == "ai":
            stats["model_calls"] += 1
            usage = getattr(message, "usage_metadata", None) or {}
            for key in ("input_tokens", "output_tokens", "total_tokens"):
                stats[key] += usage.get(key, 0) or 0
            calls = getattr(message, "tool_calls", None) or []
            stats["tool_calls"] += len(calls)
            text = text_of(message.content)
            if text and not calls:
                answer = text
            steps.append({"type": "model", "text": text,
                          "tool_calls": [{"name": c.get("name"), "args": c.get("args")} for c in calls],
                          "usage": dict(usage)})
        elif kind == "tool":
            content = text_of(message.content)
            failed = getattr(message, "status", "") == "error"
            empty = tool_result_empty(content)
            stats["tool_errors"] += int(failed)
            stats["tool_empty"] += int(empty)
            steps.append({"type": "tool", "failed": failed, "empty": empty, "content": content})
    return stats, steps, answer


def parse_report(answer):
    """Estrae il report JSON, anche se il modello lo racchiude in un blocco di codice."""
    begin, end = answer.find("{"), answer.rfind("}")
    if begin < 0 or end <= begin:
        return None
    try:
        report = json.loads(answer[begin:end + 1])
    except ValueError:
        return None
    return report if isinstance(report, dict) else None


def tool_values(steps):
    """Raccoglie (macchina, valore) da tutte le risposte di Prometheus lette dall'agente."""
    values = []
    for step in steps:
        if step["type"] != "tool":
            continue
        try:
            results = json.loads(step["content"])["data"]["result"]
        except (TypeError, ValueError, KeyError):
            continue
        for item in results:
            try:
                values.append((item["metric"].get("name"), float(item["value"][1])))
            except (KeyError, IndexError, TypeError, ValueError):
                continue
    return values


def evaluate_report(name, answer, steps):
    """Confronta il report con la risposta attesa e cerca le prove nei dati letti."""
    empty = {"json_valido": False, "stato": "", "causa_principale": "", "cause_secondarie": "",
             "stato_corretto": "", "causa_corretta": "", "prove_totali": "",
             "prove_verificate": "", "prove_non_trovate": ""}
    report = parse_report(answer)
    if report is None:
        return empty
    expected_state, expected_cause = EXPECTED_LABELS[name]
    values = tool_values(steps)
    proofs = report.get("prove") if isinstance(report.get("prove"), list) else []
    verified, missing = 0, []
    for proof in proofs:
        try:
            value = float(proof.get("valore"))
            machine = proof.get("macchina")
        except (AttributeError, TypeError, ValueError):
            missing.append(str(proof))
            continue
        # Tollera solo l'arrotondamento a due decimali.
        if any(worker == machine and abs(real - value) <= 0.0051 for worker, real in values):
            verified += 1
        else:
            missing.append(f"{proof.get('metrica')}={value}@{machine}")
    return {
        "json_valido": True,
        "stato": report.get("stato", ""),
        "causa_principale": report.get("causa_principale", ""),
        "cause_secondarie": json.dumps(report.get("cause_secondarie", []), ensure_ascii=False),
        "stato_corretto": report.get("stato") == expected_state,
        "causa_corretta": report.get("causa_principale") == expected_cause,
        "prove_totali": len(proofs),
        "prove_verificate": verified,
        "prove_non_trovate": " | ".join(missing),
    }


async def run_once(model, version, name, eval_time, timeout_s):
    client = MultiServerMCPClient({
        "prometheus": {
            "transport": "stdio",
            "command": sys.executable,
            "args": [str(PROJECT_DIR / "prometheus_mcp_server.py")],
            "env": {**os.environ, "PROMETHEUS_EVAL_TIME": str(eval_time)},
        }
    })
    started = time.perf_counter()
    last_state = None
    status, error = "OK", ""

    system_prompt = SYSTEM_PROMPTS[version]
    first_messages = [{"role": "user", "content": PROMPT}]
    if system_prompt:
        first_messages.insert(0, {"role": "system", "content": system_prompt})

    async def consume(agent):
        # In streaming conserviamo i token gia' usati anche se la prova fallisce a meta'.
        nonlocal last_state
        async for state in agent.astream(
            {"messages": first_messages}, stream_mode="values"
        ):
            last_state = state

    try:
        tools = await client.get_tools()
        await asyncio.wait_for(consume(create_react_agent(model, tools)), timeout_s)
    except asyncio.TimeoutError:
        status, error = "TIMEOUT", f"Oltre {timeout_s} s"
    except Exception as exc:  # Un fallimento dell'agente e' un dato dell'esperimento.
        status, error = type(exc).__name__, str(exc)[:500]

    seconds = round(time.perf_counter() - started, 2)
    messages = last_state["messages"] if last_state else []
    stats, steps, answer = analyze_messages(messages)
    if status == "OK" and not answer:
        status = "NO_ANSWER"
    result = {"status": status, "error": error, "seconds": seconds, "answer": answer,
              "steps": steps, **stats}
    if version == "V3":
        result.update(evaluate_report(name, answer, steps))
    return result


def print_summary(rows):
    print("\nScenario     | Prove | OK | Tempo mediano | Token mediani | Chiamate tool mediane")
    for name in SCENARIOS:
        selected = [r for r in rows if r["scenario"] == name]
        if selected:
            ok = sum(r["status"] == "OK" for r in selected)
            print(f"{name:<12} | {len(selected):>5} | {ok:>2} | "
                  f"{statistics.median(r['seconds'] for r in selected):>11.1f} s | "
                  f"{statistics.median(r['total_tokens'] for r in selected):>13.0f} | "
                  f"{statistics.median(r['tool_calls'] for r in selected):>5.0f}")


async def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--version", choices=sorted(SYSTEM_PROMPTS), required=True)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--scenarios", default="normale,cpu,lock,connessioni")
    parser.add_argument("--pause-seconds", type=float, default=20,
                        help="pausa tra due prove, per i limiti del piano LLM")
    parser.add_argument("--timeout-seconds", type=int, default=600,
                        help="solo protezione del banco di prova, non una tecnica dell'agente")
    args = parser.parse_args()

    names = [n.strip() for n in args.scenarios.split(",")]
    times = {name: scenario_time(name) for name in names}
    for name, eval_time in times.items():
        found = check_data(eval_time)
        print(f"Dati per '{name}': {found} serie")
        if not found:
            raise SystemExit(f"Prometheus non ha piu' i dati dello scenario '{name}'.")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = PROJECT_DIR / "agent_runs" / f"{args.version}_{run_id}"
    out_dir.mkdir(parents=True)
    model = create_model()
    print(f"\n{args.version} | {PROVIDER} / {MODELS[PROVIDER]} | {len(names) * args.repeats} prove")

    rows = []
    with (out_dir / "results.csv").open("w", newline="", encoding="utf-8") as results_file, \
            (out_dir / "traces.jsonl").open("w", encoding="utf-8") as traces_file:
        writer = csv.DictWriter(results_file, fieldnames=COLUMNS)
        writer.writeheader()
        total = len(names) * args.repeats
        done = 0
        for repeat in range(1, args.repeats + 1):
            for name in names:
                result = await run_once(model, args.version, name, times[name], args.timeout_seconds)
                done += 1
                row = {key: result[key] for key in COLUMNS if key in result}
                row.update(run_id=run_id, version=args.version, provider=PROVIDER,
                           model=MODELS[PROVIDER], scenario=name, repeat=repeat,
                           expected=SCENARIOS[name]["expected"], note=result["error"])
                writer.writerow(row)
                results_file.flush()
                rows.append(row)
                traces_file.write(json.dumps({
                    "run_id": run_id, "version": args.version, "scenario": name, "repeat": repeat,
                    "prompt": PROMPT, "system_prompt": SYSTEM_PROMPTS[args.version], "eval_time": times[name], "provider": PROVIDER,
                    "model": MODELS[PROVIDER], **result,
                }, ensure_ascii=False, default=str) + "\n")
                traces_file.flush()
                print(f"[{done}/{total}] {name} #{repeat}: {result['status']} | "
                      f"{result['seconds']} s | {result['total_tokens']} token | "
                      f"{result['tool_calls']} chiamate tool")
                if args.version == "V3":
                    print(f"   stato={result.get('stato')} ({result.get('stato_corretto')}) | "
                          f"causa={result.get('causa_principale')} ({result.get('causa_corretta')}) | "
                          f"prove {result.get('prove_verificate')}/{result.get('prove_totali')}")
                if result["error"]:
                    print(f"   {result['error'][:200]}")
                if done < total:
                    await asyncio.sleep(args.pause_seconds)

    print_summary(rows)
    print(f"\nRisultati in {out_dir}")


if __name__ == "__main__":
    asyncio.run(main())