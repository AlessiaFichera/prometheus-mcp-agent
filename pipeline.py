"""Pipeline: regole -> primo agente -> Telegram -> agente di esplorazione -> proposta di regola.

    python pipeline.py --scenario connessioni
    python pipeline.py --scenario cpu --disable cpu_saturation

Ogni esecuzione viene aggiunta a pipeline_events.jsonl. Le ipotesi approvate vanno in
proposed_rules.jsonl, pronte per diventare nuove regole.
"""

import argparse
import asyncio
import json
import time
from pathlib import Path

from explorer_agent import explore
from first_agent import run_agent
from monitor import SCENARIOS, analyze, scenario_time
from telegram_bot import find_chat_id, next_offset, send_decision_request, wait_for_decision, call

PROJECT_DIR = Path(__file__).resolve().parent


def format_message(scenario, result):
    report = result["report"]
    if not report:
        return f"Problema mai visto ({scenario}), ma il report dell'agente non e' leggibile. Esplora?"
    missing = ", ".join(report.get("dati_mancanti") or []) or "nessuno"
    check = f"{result['proofs_verified']} corrette, {len(result['proofs_wrong'])} sbagliate"
    tokens = (result.get("usage") or {}).get("total_tokens", "?")
    return (
        f"Problema mai visto dalle regole ({scenario})\n\n"
        f"Ipotesi: {report.get('problema')}\n"
        f"Confidenza: {report.get('confidenza')}\n"
        f"Azione consigliata: {report.get('azione_consigliata')}\n"
        f"Dati mancanti: {missing}\n"
        f"Prove controllate: {check}\n"
        f"Costo: {tokens} token, {result.get('seconds')} s\n\n"
        "OK: l'ipotesi e' giusta\nEsplora: analizza piu' a fondo"
    )


def format_exploration(scenario, result):
    report = result["report"]
    if not report:
        return (f"Esplorazione non conclusa ({scenario}): {result['status']}. {result['error']}\n"
                f"Chiamate tool: {result['tool_calls']}, token: {result['total_tokens']}")
    found = "Trovato" if report.get("trovato") else "Soluzione non trovata"
    missing = ", ".join(report.get("dati_mancanti") or []) or "nessuno"
    return (
        f"Esplorazione ({scenario}): {found}\n\n"
        f"Problema: {report.get('problema')}\n"
        f"Conferma la prima ipotesi: {report.get('conferma_prima_ipotesi')}\n"
        f"Confidenza: {report.get('confidenza')}\n"
        f"Azione consigliata: {report.get('azione_consigliata')}\n"
        f"Regola proposta: {report.get('regola_proposta')}\n"
        f"Dati mancanti: {missing}\n"
        f"Prove controllate: {result['proofs_verified']} corrette, {len(result['proofs_wrong'])} sbagliate\n"
        f"Costo: {result['tool_calls']} query, {result['total_tokens']} token\n\n"
        "OK: approva questa analisi\nNon convince: chiudi senza nuove regole"
    )


def save_proposal(scenario, source, report, event, disabled):
    proposal = {"time": time.time(), "scenario": scenario, "source": source,
                "report": report, "metrics": event["metrics"], "disabled": sorted(disabled)}
    with (PROJECT_DIR / "proposed_rules.jsonl").open("a", encoding="utf-8") as file:
        file.write(json.dumps(proposal, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    parser.add_argument("--window", default="5m")
    parser.add_argument("--disable", default="", help="regole da togliere, separate da virgola")
    parser.add_argument("--wait-minutes", type=float, default=30)
    args = parser.parse_args()
    disabled = {name.strip() for name in args.disable.split(",") if name.strip()}

    run, phase, expected = SCENARIOS[args.scenario]
    eval_time = scenario_time(run, phase)
    event = analyze(args.window, eval_time, disabled)
    record = {"time": time.time(), "scenario": args.scenario, "expected_cause": expected,
              "disabled": sorted(disabled), "event": event}

    if event["known_pattern"]:
        record["outcome"] = "regola"
        print(f"Riconosciuto dalla regola {event['diagnosis']['rule']}: nessun agente e nessun messaggio.")
    else:
        result = run_agent(event)
        record["agent"] = result
        chat_id = find_chat_id()
        offset = next_offset()
        send_decision_request(chat_id, format_message(args.scenario, result))
        print("Ipotesi inviata su Telegram: aspetto la risposta...")
        decision = wait_for_decision(chat_id, args.wait_minutes, offset)
        record["outcome"] = decision
        print(f"Decisione: {decision}")
        if decision == "ok" and result["report"]:
            save_proposal(args.scenario, "primo_agente", result["report"], event, disabled)
            call("sendMessage", chat_id=chat_id, text="Ipotesi approvata: salvata come proposta di regola.")
        elif decision == "esplora":
            call("sendMessage", chat_id=chat_id, text="Avvio l'esplorazione: potrebbe richiedere qualche minuto.")
            print("Esplorazione in corso...")
            exploration = asyncio.run(explore(event, result, eval_time))
            record["exploration"] = exploration
            print(f"Esplorazione: {exploration['status']} | {exploration['seconds']} s | "
                  f"{exploration['tool_calls']} query | {exploration['total_tokens']} token | "
                  f"prove corrette {exploration['proofs_verified']}, sbagliate {len(exploration['proofs_wrong'])}")
            offset = next_offset()
            send_decision_request(chat_id, format_exploration(args.scenario, exploration), "Non convince")
            final = wait_for_decision(chat_id, args.wait_minutes, offset)
            record["exploration_decision"] = {"esplora": "non_convince"}.get(final, final)
            print(f"Decisione finale: {record['exploration_decision']}")
            if final == "ok" and exploration["report"]:
                save_proposal(args.scenario, "esplorazione", exploration["report"], event, disabled)
                call("sendMessage", chat_id=chat_id, text="Analisi approvata: salvata come proposta di regola.")
            else:
                call("sendMessage", chat_id=chat_id, text="Chiuso senza nuove regole.")
        elif decision == "nessuna_risposta":
            call("sendMessage", chat_id=chat_id, text="Tempo scaduto: nessuna decisione registrata.")

    with (PROJECT_DIR / "pipeline_events.jsonl").open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


if __name__ == "__main__":
    main()