"""Passo 5: trasforma un'analisi approvata in una nuova regola, provata prima di entrare.

    python propose_rule.py

Legge l'ultima proposta in proposed_rules.jsonl, chiede a un agente la regola, la prova sui sei
scenari e chiede conferma su Telegram. Solo dopo l'approvazione la salva in learned_rules.json.
"""

import json
import os
import shutil
import time
from pathlib import Path

from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic

from first_agent import parse_report, response_text
from monitor import DB_WORKER, SCENARIOS, query_prometheus, scenario_time
from monitor import metric_queries
from rules import LEARNED_FILE, active_rules, build_rule, evaluate, load_learned
from telegram_bot import call, find_chat_id, next_offset, send_decision_request, wait_for_decision

PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

MODEL = os.getenv("RULE_AGENT_MODEL", "claude-sonnet-5-5")
WINDOW = "5m"
MAX_ATTEMPTS = 2

INSTRUCTIONS = """Trasforma un'analisi approvata da una persona in una regola Python per un monitor MySQL.
MySQL gira su una sola macchina: e' normale che le altre abbiano QPS quasi nullo, quindi non usare confronti tra macchine come prova di sbilanciamento.
Usa "critico" per un problema gia' grave e "attenzione" per i primi segnali, quando il valore e' sopra la norma ma non al limite.
La regola riceve solo il dizionario m con le metriche della macchina con MySQL. Le metriche possono mancare: leggile con m.get("nome") e, se servono e sono assenti, restituisci None.

Nel campo "causa" usa una di queste parole se descrive il problema: cpu, lock, connessioni. Inventane una nuova solo se nessuna va bene.

Metriche gia' disponibili: {metrics}
Puoi aggiungere metriche in "metriche_aggiunte", con nome e query PromQL. Nelle query usa {{w}} come finestra temporale. Una query deve restituire una serie per macchina con la label name. Usa una finestra lunga, come last_over_time(metrica[30m]), per i valori che possono mancare durante la saturazione.

La funzione deve avere questa forma e nient'altro:
def nome_regola(m):
    valore = m.get("metrica")
    if valore is None:
        return None
    if valore > 0:
        return diagnosis("nome_regola", "critico", "connessioni", {{"metrica": valore}}, "Azione consigliata.")
    return None

Puoi usare solo if, confronti, operazioni aritmetiche, m.get, diagnosis, round, abs, min e max.
Non usare import, altre funzioni, cicli o file. Se non puoi scrivere una regola valida, rispondi con "regola": null.

Rispondi solo con un oggetto JSON valido:
{{"nome": "nome_regola", "causa": "connessioni", "metriche_aggiunte": {{"nome": "query"}}, "regola": "codice Python", "motivazione": "una frase"}}"""


def latest_proposal():
    path = PROJECT_DIR / "proposed_rules.jsonl"
    if not path.exists():
        raise SystemExit("Nessuna proposta: prima approva un'analisi con pipeline.py.")
    return json.loads(path.read_text(encoding="utf-8").strip().splitlines()[-1])


def request_rule(proposal, feedback=None):
    model = ChatAnthropic(model=MODEL, max_tokens=2000)
    question = "Analisi approvata:\n" + json.dumps(
        {"problema": proposal["report"].get("problema"),
         "regola_proposta": proposal["report"].get("regola_proposta"),
         "metriche_osservate": proposal["metrics"]}, ensure_ascii=False, indent=2)
    if feedback:
        question += "\n\nIl tentativo precedente non ha superato la prova:\n" + feedback
    started = time.perf_counter()
    response = model.invoke([
        ("system", INSTRUCTIONS.format(metrics=", ".join(sorted(metric_queries(WINDOW))))),
        ("human", question),
    ])
    usage = getattr(response, "usage_metadata", None) or {}
    return parse_report(response_text(response.content)), usage, round(time.perf_counter() - started, 2)


def scenario_metrics(name, extra):
    """Le metriche che il monitor vedrebbe nello scenario, comprese quelle aggiunte."""
    run, phase, _ = SCENARIOS[name]
    return metrics_at(scenario_time(run, phase), extra)


def metrics_at(eval_time, extra):
    """Le metriche di worker-4 in un istante qualsiasi: serve anche per i casi visti dal vivo."""
    queries = metric_queries(WINDOW)
    queries.update({key: value.replace("{w}", WINDOW) for key, value in extra.items()})
    metrics = {}
    for key, promql in queries.items():
        try:
            rows = query_prometheus(promql, eval_time)
        except Exception:
            continue
        for row in rows:
            if row["metric"].get("name") == DB_WORKER:
                metrics[key] = round(float(row["value"][1]), 4)
    return metrics


def test_rule(rule, target_scenario, cause, extra, disabled=(), target_time=None):
    """La regola deve riconoscere il caso approvato e non cambiare le altre risposte.

    Con regole tolte (prova del docente), "prima" e' calcolato senza quelle regole e
    "originale" mostra cosa rispondeva il sistema completo, per confrontare.
    """
    results = []
    passed = True
    for name, (_, _, expected) in SCENARIOS.items():
        metrics = scenario_metrics(name, extra)
        before = evaluate(metrics, disabled)
        original = evaluate(metrics)
        fired = rule(metrics)
        if name == target_scenario:
            ok = bool(fired) and fired["cause"] == cause
        elif before:
            ok = fired is None or fired["cause"] == before["cause"]
        else:
            ok = fired is None or fired["cause"] == expected
        passed = passed and ok
        results.append({"scenario": name, "attesa": expected, "prima": before and before["cause"],
                        "originale": original and f"{original['cause']} ({original['state']})",
                        "regola": fired and f"{fired['cause']} ({fired['state']})" if fired else None,
                        "ok": ok})
    if target_scenario not in SCENARIOS and target_time is not None:
        # Caso visto dal vivo: la regola deve riconoscerlo, come uno scenario con risposta nota.
        fired = rule(metrics_at(target_time, extra))
        ok = bool(fired) and fired["cause"] == cause
        passed = passed and ok
        results.append({"scenario": target_scenario, "attesa": cause, "prima": None, "originale": None,
                        "regola": fired and f"{fired['cause']} ({fired['state']})" if fired else None,
                        "ok": ok})
    return passed, results


def attempt_rule(proposal, feedback=None, disabled=()):
    """Un tentativo: l'agente scrive la regola, poi controllo di sicurezza e test sui 6 scenari."""
    answer, usage, seconds = request_rule(proposal, feedback)
    base = {"answer": answer, "usage": usage, "seconds": seconds, "tests": [], "passed": False, "extra": {}}
    if not answer or not answer.get("regola"):
        return {**base, "error": "L'agente non ha prodotto una regola valida"}
    if answer.get("nome") in {rule.__name__ for rule in active_rules()}:
        return {**base, "error": f"Esiste gia' una regola con nome {answer.get('nome')}"}
    try:
        rule = build_rule(answer["regola"])
    except (SyntaxError, ValueError) as error:
        return {**base, "error": f"Regola rifiutata dal controllo di sicurezza: {error}"}
    extra = answer.get("metriche_aggiunte") or {}
    passed, results = test_rule(rule, proposal["scenario"], answer.get("causa"), extra, disabled,
                                proposal.get("eval_time"))
    return {**base, "tests": results, "passed": passed, "extra": extra}


def print_tests(results, disabled=()):
    for row in results:
        original = f" originale={row['originale']}" if disabled else ""
        print(f"  {row['scenario']:<14} prima={row['prima']} regola={row['regola']}{original} "
              f"{'OK' if row['ok'] else 'NON PASSA'}")


def make_feedback(proposal, attempt):
    """Il risultato dei test, restituito all'agente per il tentativo successivo."""
    answer = attempt["answer"]
    if proposal["scenario"] in SCENARIOS:
        seen = scenario_metrics(proposal["scenario"], attempt["extra"])
    else:
        seen = metrics_at(proposal["eval_time"], attempt["extra"])
    return (
        f"Regola provata:\n{answer['regola']}\n"
        f"Nello scenario {proposal['scenario']} la regola doveva indicare la causa {answer.get('causa')}, "
        f"ma non lo ha fatto.\nIn quello scenario il monitor vede queste metriche:\n{json.dumps(seen)}\n"
        "Una metrica assente da questo elenco non e' disponibile. Scrivi una regola che funzioni con questi dati, "
        "oppure aggiungi la metrica mancante con una query in metriche_aggiunte. "
        f"Risultati sugli scenari: {json.dumps(attempt['tests'])}"
    )


def rule_message(answer, results, attempts=None):
    summary = "\n".join(f"{r['scenario']}: {r['regola'] or '-'}" for r in results)
    cost = ""
    if attempts:
        tokens = sum((a.get("usage") or {}).get("total_tokens") or 0 for a in attempts)
        cost = f"\nCosto: {tokens} token, {len(attempts)} tentativ{'o' if len(attempts) == 1 else 'i'}\n"
    return (f"Nuova regola proposta: {answer['nome']}\n\n{answer.get('motivazione')}\n\n"
            f"{answer['regola']}\n\nProva sugli scenari:\n{summary}\n{cost}\nOK: aggiungila\nNon convince: scartala")


def save_learned_rule(answer, extra, source_scenario):
    """Salva la regola approvata, dopo una copia di sicurezza del file precedente."""
    learned = load_learned()
    if LEARNED_FILE.exists():
        shutil.copy(LEARNED_FILE, LEARNED_FILE.with_suffix(f".{int(time.time())}.bak"))
    learned["metrics"].update(extra)
    learned["rules"].append({"name": answer["nome"], "code": answer["regola"],
                             "source_scenario": source_scenario, "time": time.time()})
    LEARNED_FILE.write_text(json.dumps(learned, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    proposal = latest_proposal()
    disabled = set(proposal.get("disabled") or [])
    print(f"Proposta dallo scenario: {proposal['scenario']} ({proposal['source']})")
    if disabled:
        print(f"Prova con regole tolte: {sorted(disabled)}")
    feedback, attempts, attempt = None, [], None
    for number in range(1, MAX_ATTEMPTS + 1):
        attempt = attempt_rule(proposal, feedback, disabled)
        attempts.append(attempt)
        if attempt.get("error"):
            raise SystemExit(attempt["error"])
        print(f"Tentativo {number}: {attempt['seconds']} s | {attempt['usage'].get('total_tokens', '?')} token")
        print(attempt["answer"]["regola"])
        print_tests(attempt["tests"], disabled)
        if attempt["passed"]:
            break
        feedback = make_feedback(proposal, attempt)
    answer, results, extra, passed = attempt["answer"], attempt["tests"], attempt["extra"], attempt["passed"]
    record = {"time": time.time(), "scenario": proposal["scenario"], "disabled": sorted(disabled),
              "answer": answer, "usage": attempt["usage"],
              "tests": results, "tests_passed": passed, "attempts": attempts}

    if not passed:
        record["outcome"] = "test_falliti"
        print(f"Dopo {MAX_ATTEMPTS} tentativi la regola non supera i test: non viene aggiunta.")
    else:
        chat_id = find_chat_id()
        offset = next_offset()
        send_decision_request(chat_id, rule_message(answer, results, attempts), "Non convince")
        print("Regola inviata su Telegram: aspetto la risposta...")
        decision = wait_for_decision(chat_id, 30, offset)
        record["outcome"] = decision
        if decision == "ok":
            save_learned_rule(answer, extra, proposal["scenario"])
            call("sendMessage", chat_id=chat_id, text=f"Regola aggiunta: {answer['nome']}")
            print("Regola aggiunta a learned_rules.json.")
        else:
            call("sendMessage", chat_id=chat_id, text="Regola scartata.")
            reason = "hai premuto Non convince" if decision == "esplora" else "nessuna risposta su Telegram"
            print(f"Regola non aggiunta: {reason}.")

    with (PROJECT_DIR / "rule_events.jsonl").open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


if __name__ == "__main__":
    main()