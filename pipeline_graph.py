"""Pipeline completa come grafo LangGraph: dal monitor fino alla nuova regola.

    python pipeline_graph.py --scenario connessioni
    python pipeline_graph.py --scenario cpu --disable cpu_saturation
    python pipeline_graph.py --every 60    (dal vivo: un'analisi al minuto sugli ultimi 5 minuti)
    python pipeline_graph.py --draw        (stampa lo schema del grafo in Mermaid)

Se il pattern e' noto, il nodo azione invia la prescrizione su Telegram e registra l'azione
prevista in actions.jsonl. Per ora l'azione non viene eseguita sul sistema (modalita' dry-run).

Ogni nodo e' una fase del sistema e legge o aggiorna uno stato condiviso (PipelineState).
Gli archi condizionali decidono la fase successiva in base allo stato: regola nota, risposta
della persona, esito dei test. Ogni esecuzione viene aggiunta a graph_events.jsonl.
"""

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from explorer_agent import explore
from first_agent import run_agent
from monitor import SCENARIOS, analyze, scenario_time
from pipeline import format_exploration, format_message, save_proposal
from propose_rule import (MAX_ATTEMPTS, attempt_rule, make_feedback, print_tests, rule_message,
                          save_learned_rule)
from telegram_bot import call, find_chat_id, next_offset, send_decision_request, wait_for_decision

PROJECT_DIR = Path(__file__).resolve().parent

# Pattern riconosciuto -> soluzione. Le azioni sono solo proposte (dry-run): eseguirle davvero,
# per esempio scalare la macchina, e' un'evoluzione da concordare con il docente.
ACTIONS = {
    "cpu": ("scale_up", "Spostare MySQL su una macchina con piu' CPU, oppure limitare le query piu' costose."),
    "lock": ("riduci_scritture_concorrenti", "Accorciare le transazioni e distribuire le scritture sulle righe contese."),
    "connessioni": ("connection_pool", "Introdurre un connection pool; aumentare max_connections solo se la memoria lo consente."),
}


class PipelineState(TypedDict, total=False):
    """Lo stato condiviso: ogni nodo legge cio' che gli serve e restituisce solo cio' che cambia."""
    # Dati di partenza
    scenario: str
    silenced: list
    window: str
    disabled: list
    wait_minutes: float
    eval_time: float
    # Fase 1: regole e azione
    event: dict
    action: dict
    # Fase 2-3: primo agente e persona
    agent: dict
    decision: str
    # Fase 4: esplorazione e persona
    exploration: dict
    exploration_decision: str
    # Fase 5: nuova regola
    proposal: dict
    attempts: list
    feedback: Optional[str]
    rule_decision: str
    # Esito finale e percorso seguito
    outcome: str
    path: list


def visited(state, name):
    return state.get("path", []) + [name]


def ask_person(text, second_label, wait_minutes):
    """Invia il messaggio con due pulsanti e aspetta la risposta su Telegram."""
    chat_id = find_chat_id()
    offset = next_offset()
    send_decision_request(chat_id, text, second_label)
    return wait_for_decision(chat_id, wait_minutes, offset)


def notify(text):
    call("sendMessage", chat_id=find_chat_id(), text=text)


# ---------------------------------------------------------------- nodi

def monitor_node(state: PipelineState) -> dict:
    """Fase 1: extractor e regole deterministiche, zero token."""
    event = analyze(state["window"], state.get("eval_time"), set(state["disabled"]))
    path = visited(state, "regole")
    silenced = state.get("silenced", [])
    if event["known_pattern"]:
        diagnosis = event["diagnosis"]
        print(f"[regole] riconosciuto da {diagnosis['rule']}: {diagnosis['state']}, causa {diagnosis['cause']}.")
        if diagnosis["state"] == "normale":
            return {"event": event, "outcome": "normale", "path": path}
        if diagnosis["cause"] in silenced:
            return {"event": event, "outcome": "gia_segnalato", "path": path}
        return {"event": event, "path": path}
    if "sconosciuto" in silenced:
        print("[regole] pattern sconosciuto, ma gia' in analisi da poco: aspetto.")
        return {"event": event, "outcome": "in_attesa", "path": path}
    print("[regole] pattern sconosciuto: passo al primo agente.")
    return {"event": event, "path": path}


def action_node(state: PipelineState) -> dict:
    """Pattern noto: la soluzione. Invia la prescrizione e registra l'azione prevista (dry-run)."""
    diagnosis = state["event"]["diagnosis"]
    name, description = ACTIONS.get(diagnosis["cause"], ("nessuna_azione_automatica", diagnosis["recommendation"]))
    action = {"time": time.time(), "scenario": state["scenario"], "rule": diagnosis["rule"],
              "state": diagnosis["state"], "cause": diagnosis["cause"], "evidence": diagnosis["evidence"],
              "action": name, "description": description, "mode": "dry-run"}
    with (PROJECT_DIR / "actions.jsonl").open("a", encoding="utf-8") as file:
        file.write(json.dumps(action, ensure_ascii=False) + "\n")
    notify(f"Problema riconosciuto ({state['scenario']}): {diagnosis['state']}, causa {diagnosis['cause']}\n"
           f"Regola: {diagnosis['rule']}\nProve: {json.dumps(diagnosis['evidence'])}\n\n"
           f"Prescrizione: {diagnosis['recommendation']}\nAzione prevista: {name} (non eseguita)")
    print(f"[azione] {name}: {description} (dry-run, prescrizione inviata su Telegram)")
    return {"action": action, "outcome": "regola_nota", "path": visited(state, "azione")}


def first_agent_node(state: PipelineState) -> dict:
    """Fase 2: Haiku riceve i dati raccolti dal sistema, senza strumenti."""
    result = run_agent(state["event"])
    usage = result["usage"] or {}
    print(f"[primo agente] {result['seconds']} s | {usage.get('total_tokens', '?')} token | "
          f"prove corrette {result['proofs_verified']}, sbagliate {len(result['proofs_wrong'])}")
    return {"agent": result, "path": visited(state, "primo_agente")}


def review_hypothesis_node(state: PipelineState) -> dict:
    """Fase 3: la persona sceglie OK oppure Esplora."""
    print("[persona] ipotesi inviata su Telegram: aspetto la risposta...")
    decision = ask_person(format_message(state["scenario"], state["agent"]), "Esplora", state["wait_minutes"])
    print(f"[persona] decisione: {decision}")
    update = {"decision": decision, "path": visited(state, "revisione_ipotesi")}
    if decision == "ok" and not state["agent"]["report"]:
        update["outcome"] = "report_non_leggibile"
    elif decision == "nessuna_risposta":
        notify("Tempo scaduto: nessuna decisione registrata.")
        update["outcome"] = "nessuna_risposta"
    return update


def explore_node(state: PipelineState) -> dict:
    """Fase 4: cambia l'harness, Sonnet cerca da solo con MCP."""
    notify("Avvio l'esplorazione: potrebbe richiedere qualche minuto.")
    print("[esplorazione] in corso...")
    result = asyncio.run(explore(state["event"], state["agent"], state["eval_time"]))
    print(f"[esplorazione] {result['status']} | {result['seconds']} s | {result['tool_calls']} query | "
          f"{result['total_tokens']} token | prove corrette {result['proofs_verified']}, "
          f"sbagliate {len(result['proofs_wrong'])}")
    return {"exploration": result, "path": visited(state, "esplorazione")}


def review_exploration_node(state: PipelineState) -> dict:
    """La persona approva l'analisi dell'esplorazione oppure la chiude."""
    print("[persona] analisi inviata su Telegram: aspetto la risposta...")
    final = ask_person(format_exploration(state["scenario"], state["exploration"]), "Non convince",
                       state["wait_minutes"])
    final = {"esplora": "non_convince"}.get(final, final)
    print(f"[persona] decisione: {final}")
    update = {"exploration_decision": final, "path": visited(state, "revisione_esplorazione")}
    if final != "ok" or not state["exploration"]["report"]:
        notify("Chiuso senza nuove regole.")
        update["outcome"] = "analisi_non_approvata"
    return update


def save_proposal_node(state: PipelineState) -> dict:
    """L'ipotesi approvata diventa una proposta di regola."""
    if state.get("exploration_decision") == "ok":
        source, report = "esplorazione", state["exploration"]["report"]
    else:
        source, report = "primo_agente", state["agent"]["report"]
    save_proposal(state["scenario"], source, report, state["event"], set(state["disabled"]))
    proposal = {"scenario": state["scenario"], "source": source, "report": report,
                "metrics": state["event"]["metrics"], "disabled": state["disabled"],
                "eval_time": state["event"]["time"]}
    print(f"[proposta] salvata dall'analisi di: {source}")
    return {"proposal": proposal, "attempts": [], "feedback": None, "path": visited(state, "proposta")}


def write_rule_node(state: PipelineState) -> dict:
    """Fase 5: l'agente scrive la regola; controllo di sicurezza e test sui 6 scenari."""
    attempt = attempt_rule(state["proposal"], state.get("feedback"), set(state["disabled"]))
    attempts = state["attempts"] + [attempt]
    number = len(attempts)
    update = {"attempts": attempts, "path": visited(state, f"scrivi_regola_{number}")}
    if attempt.get("error"):
        print(f"[regola] tentativo {number}: {attempt['error']}")
        update["outcome"] = "regola_rifiutata"
        return update
    print(f"[regola] tentativo {number}: {attempt['seconds']} s | "
          f"{attempt['usage'].get('total_tokens', '?')} token")
    print(attempt["answer"]["regola"])
    print_tests(attempt["tests"], state["disabled"])
    if not attempt["passed"]:
        if number < MAX_ATTEMPTS:
            update["feedback"] = make_feedback(state["proposal"], attempt)
        else:
            print(f"[regola] dopo {MAX_ATTEMPTS} tentativi non supera i test: non viene aggiunta.")
            update["outcome"] = "test_falliti"
    return update


def review_rule_node(state: PipelineState) -> dict:
    """La persona approva la regola prima che entri nel sistema."""
    attempt = state["attempts"][-1]
    print("[persona] regola inviata su Telegram: aspetto la risposta...")
    decision = ask_person(rule_message(attempt["answer"], attempt["tests"], state["attempts"]), "Non convince",
                          state["wait_minutes"])
    print(f"[persona] decisione: {decision}")
    update = {"rule_decision": decision, "path": visited(state, "revisione_regola")}
    if decision != "ok":
        notify("Regola scartata.")
        update["outcome"] = "regola_scartata"
    return update


def save_rule_node(state: PipelineState) -> dict:
    """La regola approvata entra in learned_rules.json: la prossima volta costa zero token."""
    attempt = state["attempts"][-1]
    save_learned_rule(attempt["answer"], attempt["extra"], state["scenario"])
    notify(f"Regola aggiunta: {attempt['answer']['nome']}")
    print("[regola] aggiunta a learned_rules.json.")
    return {"outcome": "regola_aggiunta", "path": visited(state, "salva_regola")}


# ---------------------------------------------------------------- archi condizionali

def after_monitor(state: PipelineState) -> str:
    if state.get("outcome"):
        return "fine"
    return "azione" if state["event"]["known_pattern"] else "primo_agente"


def after_hypothesis(state: PipelineState) -> str:
    if state.get("outcome"):
        return "fine"
    return "esplorazione" if state["decision"] == "esplora" else "proposta"


def after_exploration_review(state: PipelineState) -> str:
    return "fine" if state.get("outcome") else "proposta"


def after_write(state: PipelineState) -> str:
    if state.get("outcome"):
        return "fine"
    return "revisione_regola" if state["attempts"][-1]["passed"] else "riprova"


def after_rule_review(state: PipelineState) -> str:
    return "fine" if state.get("outcome") else "salva_regola"


def build_graph():
    graph = StateGraph(PipelineState)
    graph.add_node("regole", monitor_node)
    graph.add_node("azione", action_node)
    graph.add_node("primo_agente", first_agent_node)
    graph.add_node("revisione_ipotesi", review_hypothesis_node)
    graph.add_node("esplorazione", explore_node)
    graph.add_node("revisione_esplorazione", review_exploration_node)
    graph.add_node("proposta", save_proposal_node)
    graph.add_node("scrivi_regola", write_rule_node)
    graph.add_node("revisione_regola", review_rule_node)
    graph.add_node("salva_regola", save_rule_node)

    graph.add_edge(START, "regole")
    graph.add_conditional_edges("regole", after_monitor,
                                {"azione": "azione", "primo_agente": "primo_agente", "fine": END})
    graph.add_edge("azione", END)
    graph.add_edge("primo_agente", "revisione_ipotesi")
    graph.add_conditional_edges("revisione_ipotesi", after_hypothesis,
                                {"esplorazione": "esplorazione", "proposta": "proposta", "fine": END})
    graph.add_edge("esplorazione", "revisione_esplorazione")
    graph.add_conditional_edges("revisione_esplorazione", after_exploration_review,
                                {"proposta": "proposta", "fine": END})
    graph.add_edge("proposta", "scrivi_regola")
    # Il ciclo: se i test falliscono, il grafo torna a scrivi_regola con il feedback.
    graph.add_conditional_edges("scrivi_regola", after_write,
                                {"revisione_regola": "revisione_regola", "riprova": "scrivi_regola",
                                 "fine": END})
    graph.add_conditional_edges("revisione_regola", after_rule_review,
                                {"salva_regola": "salva_regola", "fine": END})
    graph.add_edge("salva_regola", END)
    return graph.compile()


def summary(state: dict) -> dict[str, Any]:
    """Token per fase, per misurare il costo di ogni percorso."""
    tokens = {}
    if state.get("agent"):
        tokens["primo_agente"] = (state["agent"]["usage"] or {}).get("total_tokens")
    if state.get("exploration"):
        tokens["esplorazione"] = state["exploration"]["total_tokens"]
    if state.get("attempts"):
        tokens["scrivi_regola"] = sum((a["usage"] or {}).get("total_tokens") or 0 for a in state["attempts"])
    return tokens


def run_once(app, args, scenario, eval_time, disabled, silenced, expected=None):
    started = time.perf_counter()
    final = app.invoke({
        "scenario": scenario, "window": args.window, "disabled": disabled, "silenced": sorted(silenced),
        "wait_minutes": args.wait_minutes, "eval_time": eval_time, "path": [],
    })
    tokens = summary(final)
    print(f"Esito: {final.get('outcome')} | percorso: {' -> '.join(final.get('path', []))}")
    print(f"Token per fase: {tokens} | totale: {sum(v or 0 for v in tokens.values())}")
    record = {"time": time.time(), "scenario": scenario, "expected_cause": expected,
              "disabled": disabled, "outcome": final.get("outcome"), "path": final.get("path"),
              "tokens": tokens, "seconds": round(time.perf_counter() - started, 2), "state": final}
    with (PROJECT_DIR / "graph_events.jsonl").open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    return final


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--every", type=int, default=0, help="dal vivo: secondi tra due analisi")
    parser.add_argument("--cooldown-minutes", type=float, default=15,
                        help="dal vivo: non ripetere avvisi o agenti sulla stessa causa entro questo tempo")
    parser.add_argument("--window", default="5m")
    parser.add_argument("--disable", default="", help="regole da togliere, separate da virgola")
    parser.add_argument("--wait-minutes", type=float, default=30)
    parser.add_argument("--draw", action="store_true", help="stampa lo schema del grafo ed esce")
    args = parser.parse_args()

    app = build_graph()
    if args.draw:
        print(app.get_graph().draw_mermaid())
        return
    disabled = sorted(name.strip() for name in args.disable.split(",") if name.strip())

    if args.scenario:
        run, phase, expected = SCENARIOS[args.scenario]
        run_once(app, args, args.scenario, scenario_time(run, phase), disabled, set(), expected)
        return
    if not args.every:
        parser.error("serve --scenario, --every oppure --draw")

    # Ascolto continuo. Senza un periodo di attesa, lo stesso problema farebbe partire
    # un avviso o un agente a ogni lettura.
    last_seen = {}
    while True:
        now = time.time()
        silenced = {key for key, when in last_seen.items() if now - when < args.cooldown_minutes * 60}
        print(f"\n[{time.strftime('%H:%M:%S')}] analisi dal vivo")
        final = run_once(app, args, "dal_vivo", None, disabled, silenced)
        event = final.get("event") or {}
        if final.get("outcome") == "regola_nota":
            last_seen[event["diagnosis"]["cause"]] = now
        elif final.get("outcome") == "regola_aggiunta":
            last_seen.pop("sconosciuto", None)  # la nuova regola vale gia' dalla lettura successiva
        elif "primo_agente" in final.get("path", []):
            last_seen["sconosciuto"] = now
        time.sleep(args.every)


if __name__ == "__main__":
    main()