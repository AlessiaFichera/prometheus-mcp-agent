"""Regole deterministiche del monitor. Questo e' il file che il sistema fara' crescere.

Ogni regola riceve le metriche della macchina con MySQL e restituisce una diagnosi, oppure
None se il pattern non corrisponde. Se nessuna regola scatta, il monitor chiama l'agente.
Le soglie sono iniziali: derivano dai test di stress e valgono da verificare.
"""

import ast
import json
from pathlib import Path


def _diagnosis(rule, state, cause, evidence, recommendation):
    return {"rule": rule, "state": state, "cause": cause,
            "evidence": evidence, "recommendation": recommendation}


def _mysql_complete(m):
    return all(m.get(key) is not None for key in (
        "threads_running", "connections", "max_connections", "lock_ms_s"))


def _lock_waiting(m):
    return m["lock_ms_s"] / 1000


def connections_limit(m):
    if not _mysql_complete(m):
        return None
    usage = 100 * m["connections"] / m["max_connections"]
    if usage < 80:
        return None
    state = "critico" if usage >= 95 else "attenzione"
    return _diagnosis(
        "connections_limit", state, "connessioni",
        {"connections": m["connections"], "max_connections": m["max_connections"]},
        "Usare un connection pool e controllare i client; aumentare max_connections solo se la macchina ha risorse.",
    )


def lock_contention(m):
    if not _mysql_complete(m) or m.get("cpu_pct") is None or m["cpu_pct"] >= 75:
        return None
    waiting = _lock_waiting(m)
    if waiting < 1:
        return None
    state = "critico" if waiting >= 5 else "attenzione"
    return _diagnosis(
        "lock_contention", state, "lock",
        {"lock_ms_s": m["lock_ms_s"], "connections_waiting": round(waiting, 2), "cpu_pct": m["cpu_pct"]},
        "Accorciare le transazioni e ridurre le scritture concorrenti sulle stesse righe.",
    )


def cpu_saturation(m):
    if not _mysql_complete(m) or m.get("cpu_pct") is None or m["cpu_pct"] < 75:
        return None
    state = "critico" if m["cpu_pct"] >= 90 else "attenzione"
    return _diagnosis(
        "cpu_saturation", state, "cpu",
        {"cpu_pct": m["cpu_pct"], "threads_running": m["threads_running"]},
        "Ottimizzare o limitare le query costose; valutare lo scaling della macchina.",
    )


def healthy(m):
    needed = ("cpu_pct", "threads_running", "connections", "lock_ms_s")
    if any(m.get(key) is None for key in needed):
        return None
    if m["cpu_pct"] >= 75 or _lock_waiting(m) >= 1:
        return None
    if m.get("max_connections") is not None:
        if 100 * m["connections"] / m["max_connections"] >= 80:
            return None
    elif m["connections"] >= 100:
        # Il limite e' un'impostazione e a volte l'exporter non lo riporta: senza di esso
        # il sistema si dichiara sano solo con un numero di connessioni chiaramente basso.
        return None
    return _diagnosis("healthy", "normale", "nessuna",
                      {"cpu_pct": m["cpu_pct"], "connections": m["connections"]}, "Nessuna azione.")


LEARNED_FILE = Path(__file__).resolve().parent / "learned_rules.json"
ALLOWED_CALLS = {"diagnosis", "round", "abs", "min", "max"}
ALLOWED_NODES = (
    ast.Module, ast.FunctionDef, ast.arguments, ast.arg, ast.Return, ast.If, ast.Assign, ast.Name,
    ast.Load, ast.Store, ast.Constant, ast.Compare, ast.BoolOp, ast.BinOp, ast.UnaryOp, ast.And,
    ast.Or, ast.Not, ast.Eq, ast.NotEq, ast.Gt, ast.GtE, ast.Lt, ast.LtE, ast.Is, ast.IsNot,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.USub, ast.Dict, ast.Call, ast.Attribute,
    ast.Expr,
)


def build_rule(code):
    """Compila una regola appresa solo se usa operazioni semplici e sicure.

    Una regola non deve poter importare moduli, leggere file o usare la rete.
    """
    tree = ast.parse(code)
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
    if len(tree.body) != 1 or len(functions) != 1:
        raise ValueError("La regola deve contenere una sola funzione")
    function = functions[0]
    if [arg.arg for arg in function.args.args] != ["m"]:
        raise ValueError("La funzione deve ricevere solo il parametro m")
    for node in ast.walk(tree):
        if not isinstance(node, ALLOWED_NODES):
            raise ValueError(f"Operazione non ammessa: {type(node).__name__}")
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise ValueError("Nome non ammesso")
        if isinstance(node, ast.Attribute) and not (
                node.attr == "get" and isinstance(node.value, ast.Name) and node.value.id == "m"):
            raise ValueError("Attributo non ammesso: si puo' usare solo m.get")
        if isinstance(node, ast.Call):
            target = node.func
            allowed = (isinstance(target, ast.Name) and target.id in ALLOWED_CALLS) or isinstance(target, ast.Attribute)
            if not allowed:
                raise ValueError("Chiamata non ammessa")
    scope = {"__builtins__": {}, "diagnosis": _diagnosis, "round": round, "abs": abs, "min": min, "max": max}
    exec(compile(tree, "learned_rule", "exec"), scope)
    return scope[function.name]


def load_learned():
    if not LEARNED_FILE.exists():
        return {"metrics": {}, "rules": []}
    return json.loads(LEARNED_FILE.read_text(encoding="utf-8"))


LEARNED_RULES = [build_rule(item["code"]) for item in load_learned()["rules"]]

# Le regole apprese vengono provate prima di concludere che il sistema e' in salute.
RULES = [connections_limit, lock_contention, cpu_saturation, *LEARNED_RULES, healthy]


_ACTIVE = {"mtime": LEARNED_FILE.stat().st_mtime if LEARNED_FILE.exists() else None, "rules": RULES}


def active_rules():
    """Le regole in uso. Se learned_rules.json cambia, lo ricarica: una regola appena
    approvata vale subito, anche con il monitor gia' acceso."""
    mtime = LEARNED_FILE.stat().st_mtime if LEARNED_FILE.exists() else None
    if mtime != _ACTIVE["mtime"]:
        learned = [build_rule(item["code"]) for item in load_learned()["rules"]]
        _ACTIVE.update(mtime=mtime, rules=[connections_limit, lock_contention, cpu_saturation, *learned, healthy])
    return _ACTIVE["rules"]


def evaluate(metrics, disabled=()):
    """Restituisce la prima diagnosi valida, o None se il pattern e' sconosciuto."""
    for rule in active_rules():
        if rule.__name__ in disabled:
            continue
        diagnosis = rule(metrics)
        if diagnosis:
            return diagnosis
    return None