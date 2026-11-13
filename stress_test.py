"""Test di stress a gradini su MySQL, SENZA agente.

Ogni gradino usa un numero maggiore di connessioni concorrenti. Tra un gradino e
l'altro c'e' una pausa di recupero. Il test si ferma da solo se il database mostra
un degrado oltre le soglie indicate.

Prova breve (circa 3 minuti, solo letture):
    python stress_test.py --levels 1,2 --baseline-seconds 30 --step-seconds 45 \
        --recovery-seconds 15 --mix read_pk=70,read_range=30

Prova completa (tabelle di test da creare una sola volta con --setup):
    python stress_test.py --setup
    python stress_test.py

Risultati in runs/<run_id>/: manifest.json, requests.csv, connections.csv, steps.csv
"""

import argparse
import csv
import json
import os
import queue
import random
import threading
import time
from datetime import datetime
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

DEFAULT_MIX = (
    "read_pk=50,read_range=20,join_report=10,join_heavy=5,"
    "write_insert=10,write_hot=5"
)
WRITE_CLASSES = {"write_insert", "write_hot"}
HOT_ROWS = 5
YEARS = (2003, 2004, 2005)

SETUP_SQL = [
    """CREATE TABLE IF NOT EXISTS tesi_stress_log (
           id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
           run_id VARCHAR(32) NOT NULL,
           worker_id INT NOT NULL,
           created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
           KEY idx_run (run_id)
       )""",
    """CREATE TABLE IF NOT EXISTS tesi_stress_hot (
           id INT NOT NULL PRIMARY KEY,
           counter BIGINT NOT NULL DEFAULT 0
       )""",
]

SQL = {
    "read_pk": (
        "SELECT customerName, city, creditLimit FROM customers "
        "WHERE customerNumber = %s"
    ),
    "read_range": (
        "SELECT orderNumber, orderDate, status FROM orders "
        "WHERE customerNumber = %s ORDER BY orderDate DESC"
    ),
    "join_report": (
        "SELECT c.country, p.productLine, "
        "SUM(od.quantityOrdered * od.priceEach) AS revenue, "
        "COUNT(DISTINCT o.orderNumber) AS n_orders "
        "FROM customers c "
        "JOIN orders o ON o.customerNumber = c.customerNumber "
        "JOIN orderdetails od ON od.orderNumber = o.orderNumber "
        "JOIN products p ON p.productCode = od.productCode "
        "WHERE YEAR(o.orderDate) = %s "
        "GROUP BY c.country, p.productLine ORDER BY revenue DESC"
    ),
    # Coppie di prodotti acquistati nello stesso ordine: self-join costoso anche
    # su un dataset piccolo.
    "join_heavy": (
        "SELECT a.productCode, b.productCode, COUNT(*) AS together "
        "FROM orderdetails a "
        "JOIN orderdetails b ON a.orderNumber = b.orderNumber "
        "AND a.productCode < b.productCode "
        "JOIN orders o ON o.orderNumber = a.orderNumber "
        "WHERE YEAR(o.orderDate) = %s "
        "GROUP BY a.productCode, b.productCode "
        "ORDER BY together DESC LIMIT 20"
    ),
    # Molto piu' costosa: self-join a tre vie, milioni di righe intermedie.
    "join_extreme": (
        "SELECT COUNT(*), SUM(a.quantityOrdered * c.priceEach) "
        "FROM orderdetails a "
        "JOIN orderdetails b ON a.productCode = b.productCode "
        "JOIN orderdetails c ON b.orderNumber = c.orderNumber "
        "WHERE a.quantityOrdered > %s"
    ),
    "write_insert": (
        "INSERT INTO tesi_stress_log (run_id, worker_id) VALUES (%s, %s)"
    ),
    # Poche righe aggiornate da molte connessioni: serve a creare attese sui lock.
    "write_hot": "UPDATE tesi_stress_hot SET counter = counter + 1 WHERE id = %s",
    "probe": "SELECT 1",
}

REQUEST_COLUMNS = [
    "start_epoch", "phase", "concurrency", "worker_id", "query_class",
    "exec_s", "commit_s", "total_s", "rows", "status", "errno",
]


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--levels", default="1,2,4,8,16,32,64",
                   help="connessioni concorrenti per ogni gradino")
    p.add_argument("--baseline-seconds", type=int, default=300)
    p.add_argument("--step-seconds", type=int, default=300)
    p.add_argument("--recovery-seconds", type=int, default=180,
                   help="deve superare 120 s, la finestra di rate usata nell'analisi")
    p.add_argument("--warmup-seconds", type=int, default=30,
                   help="secondi iniziali di ogni gradino esclusi dal riepilogo")
    p.add_argument("--mix", default=DEFAULT_MIX)
    p.add_argument("--think-ms", type=int, default=0,
                   help="pausa media tra due query della stessa connessione")
    p.add_argument("--query-timeout-ms", type=int, default=30000)
    p.add_argument("--stop-error-pct", type=float, default=5.0,
                   help="ferma il test se gli errori del gradino superano questa soglia")
    p.add_argument("--stop-p95-s", type=float, default=10.0,
                   help="ferma il test se il p95 del gradino supera questa soglia")
    p.add_argument("--abort-error-pct", type=float, default=20.0,
                   help="interrompe subito il gradino se gli errori recenti superano la soglia")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--setup", action="store_true",
                   help="crea le tabelle di test ed esce")
    p.add_argument("--yes", action="store_true", help="salta la conferma")
    return p.parse_args()


def parse_mix(text):
    mix = {}
    for part in text.split(","):
        name, _, weight = part.partition("=")
        name = name.strip()
        if name not in SQL or name == "probe":
            raise ValueError(f"Classe di query sconosciuta: {name}")
        mix[name] = float(weight)
    if not mix or sum(mix.values()) <= 0:
        raise ValueError("Il mix deve avere almeno un peso positivo")
    return mix


def connect(timeout_ms):
    conn = mysql.connector.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ["MYSQL_PORT"]),
        database=os.environ["MYSQL_DATABASE"],
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        connection_timeout=10,
        autocommit=True,  # Le letture non fanno un COMMIT inutile.
    )
    # Limite alle SELECT troppo lunghe. Non tutti i server supportano la variabile.
    cursor = conn.cursor()
    try:
        cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {int(timeout_ms)}")
    except mysql.connector.Error:
        pass
    finally:
        cursor.close()
    return conn


def server_identity(conn):
    """Restituisce nodo e ID di connessione: mostra dove instrada il load balancer."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT @@hostname, CONNECTION_ID()")
        host, connection_id = cursor.fetchone()
        return str(host), int(connection_id)
    finally:
        cursor.close()


def build_params(query_class, rng, customer_ids, run_id, worker_id):
    if query_class in ("read_pk", "read_range"):
        return (rng.choice(customer_ids),)
    if query_class in ("join_report", "join_heavy"):
        return (rng.choice(YEARS),)
    if query_class == "join_extreme":
        return (rng.randint(20, 40),)
    if query_class == "write_insert":
        return (run_id, worker_id)
    if query_class == "write_hot":
        return (rng.randint(1, HOT_ROWS),)
    return ()


def run_query(conn, cursor, query_class, params):
    """Esegue una query. Per le scritture misura a parte esecuzione e commit.

    Il cursore e' riutilizzato: aprirne uno nuovo a ogni query fa inviare al driver
    un ping, cioe' uno scambio di rete in piu' che gonfierebbe la latenza.
    """
    is_write = query_class in WRITE_CLASSES
    if is_write:
        conn.start_transaction()
    started = time.perf_counter()
    cursor.execute(SQL[query_class], params)
    rows = len(cursor.fetchall()) if cursor.with_rows else cursor.rowcount
    executed = time.perf_counter()
    if is_write:
        conn.commit()
    return executed - started, time.perf_counter() - executed, rows


def record(out, start, phase, concurrency, worker_id, query_class,
           exec_s, commit_s, total_s, rows, status, errno):
    out.put({
        "start_epoch": round(start, 3), "phase": phase,
        "concurrency": concurrency, "worker_id": worker_id,
        "query_class": query_class, "exec_s": round(exec_s, 5),
        "commit_s": round(commit_s, 5), "total_s": round(total_s, 5),
        "rows": rows, "status": status, "errno": errno,
    })


def worker(worker_id, phase, concurrency, args, mix, customer_ids, run_id,
           stop, out, connections):
    rng = random.Random(f"{args.seed}-{phase}-{worker_id}")
    classes, weights = list(mix), list(mix.values())
    conn = None
    cursor = None

    while not stop.is_set():
        query_class = rng.choices(classes, weights)[0]
        params = build_params(query_class, rng, customer_ids, run_id, worker_id)
        start = time.time()
        started = time.perf_counter()
        try:
            if conn is None:
                conn = connect(args.query_timeout_ms)
                host, connection_id = server_identity(conn)
                connections.put((phase, concurrency, worker_id, host, connection_id))
                cursor = conn.cursor()
            exec_s, commit_s, rows = run_query(conn, cursor, query_class, params)
            record(out, start, phase, concurrency, worker_id, query_class,
                   exec_s, commit_s, time.perf_counter() - started, rows, "OK", "")
        except Exception as error:  # Un errore non deve fermare gli altri worker.
            errno = getattr(error, "errno", "") or type(error).__name__
            record(out, start, phase, concurrency, worker_id, query_class,
                   0, 0, time.perf_counter() - started, 0, "ERROR", errno)
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None
            stop.wait(0.5)  # Evita una raffica di riconnessioni.
        if args.think_ms and not stop.is_set():
            stop.wait(rng.expovariate(1000 / args.think_ms))

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def probe(phase, concurrency, stop, out, timeout_ms):
    """Una SELECT 1 al secondo: stima la quota di latenza dovuta a rete e server."""
    conn = None
    cursor = None
    while not stop.wait(1.0):
        start = time.time()
        started = time.perf_counter()
        try:
            if conn is None:
                conn = connect(timeout_ms)
                cursor = conn.cursor()
                started = time.perf_counter()
            exec_s, commit_s, rows = run_query(conn, cursor, "probe", ())
            record(out, start, phase, concurrency, -1, "probe", exec_s, commit_s,
                   time.perf_counter() - started, rows, "OK", "")
        except Exception as error:
            errno = getattr(error, "errno", "") or type(error).__name__
            record(out, start, phase, concurrency, -1, "probe", 0, 0,
                   time.perf_counter() - started, 0, "ERROR", errno)
            conn = None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


class Recorder(threading.Thread):
    """Scrive su CSV in un solo thread e tiene gli esiti per controlli e riepilogo."""

    def __init__(self, path):
        super().__init__(daemon=True)
        self.queue = queue.Queue()
        self.path = path
        self.finished = threading.Event()
        self.lock = threading.Lock()
        self.rows = []

    def run(self):
        with self.path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=REQUEST_COLUMNS)
            writer.writeheader()
            while not (self.finished.is_set() and self.queue.empty()):
                try:
                    row = self.queue.get(timeout=0.2)
                except queue.Empty:
                    file.flush()
                    continue
                writer.writerow(row)
                with self.lock:
                    self.rows.append(row)

    def phase_rows(self, phase):
        with self.lock:
            return [r for r in self.rows if r["phase"] == phase]


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def summarize(rows, begin, end, warmup, phase, concurrency, client_cpu_pct):
    selected = [r for r in rows if r["start_epoch"] >= begin + warmup]
    load = [r for r in selected if r["query_class"] != "probe"]
    ok = [r["total_s"] for r in load if r["status"] == "OK"]
    errors = len(load) - len(ok)
    seconds = max(end - begin - warmup, 1e-9)
    probes = [r["total_s"] for r in selected
              if r["query_class"] == "probe" and r["status"] == "OK"]
    summary = {
        "phase": phase, "concurrency": concurrency,
        "begin_epoch": round(begin, 3), "end_epoch": round(end, 3),
        "requests": len(load), "errors": errors,
        "error_pct": round(100 * errors / len(load), 3) if load else 0,
        "throughput_qps": round(len(ok) / seconds, 3),
        "p50_s": percentile(ok, 0.50), "p95_s": percentile(ok, 0.95),
        "p99_s": percentile(ok, 0.99), "probe_p50_s": percentile(probes, 0.50),
        "client_cpu_pct": client_cpu_pct,
    }
    for name in sorted({r["query_class"] for r in load}):
        values = [r["total_s"] for r in load
                  if r["query_class"] == name and r["status"] == "OK"]
        summary[f"p95_{name}_s"] = percentile(values, 0.95)
    return summary


def run_phase(phase, concurrency, seconds, args, mix, customer_ids, run_id,
              recorder, connections):
    stop = threading.Event()
    threads = [threading.Thread(
        target=probe, daemon=True,
        args=(phase, concurrency, stop, recorder.queue, args.query_timeout_ms),
    )]
    threads += [threading.Thread(
        target=worker, daemon=True,
        args=(i, phase, concurrency, args, mix, customer_ids, run_id, stop,
              recorder.queue, connections),
    ) for i in range(concurrency)]

    begin = time.time()
    cpu_begin = time.process_time()
    for thread in threads:
        thread.start()

    aborted = False
    try:
        while time.time() - begin < seconds:
            time.sleep(min(5, max(0.1, seconds - (time.time() - begin))))
            recent = [r for r in recorder.phase_rows(phase)
                      if r["query_class"] != "probe"
                      and r["start_epoch"] >= time.time() - 30]
            if len(recent) >= 20:
                bad = sum(r["status"] != "OK" for r in recent)
                if 100 * bad / len(recent) > args.abort_error_pct:
                    print(f"  INTERRUZIONE: {bad}/{len(recent)} errori negli ultimi 30 s")
                    aborted = True
                    break
    finally:
        # Anche con CTRL + C il carico sul database deve fermarsi subito.
        stop.set()

    end = time.time()
    for thread in threads:
        thread.join(timeout=args.query_timeout_ms / 1000 + 15)
    cpu_pct = round(100 * (time.process_time() - cpu_begin) / max(end - begin, 1e-9), 1)
    while recorder.queue.qsize():
        time.sleep(0.05)

    summary = summarize(
        recorder.phase_rows(phase), begin, end,
        min(args.warmup_seconds, seconds // 3), phase, concurrency, cpu_pct,
    )
    summary["aborted"] = aborted
    return summary


def preflight(mix, customer_ids, run_id, timeout_ms):
    """Esegue ogni query una volta: un errore di schema emerge prima del carico."""
    conn = connect(timeout_ms)
    cursor = conn.cursor()
    rng = random.Random(0)
    try:
        for name in mix:
            exec_s, commit_s, rows = run_query(
                conn, cursor, name, build_params(name, rng, customer_ids, run_id, -1)
            )
            print(f"  ok {name:<13} {exec_s + commit_s:.3f} s, righe={rows}")
    finally:
        conn.close()


def setup_tables(timeout_ms):
    conn = connect(timeout_ms)
    cursor = conn.cursor()
    try:
        for statement in SETUP_SQL:
            cursor.execute(statement)
        for row_id in range(1, HOT_ROWS + 1):
            cursor.execute(
                "INSERT IGNORE INTO tesi_stress_hot (id, counter) VALUES (%s, 0)",
                (row_id,),
            )
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    print("Tabelle tesi_stress_log e tesi_stress_hot pronte.")


def load_customer_ids(timeout_ms):
    conn = connect(timeout_ms)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT customerNumber FROM customers")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


def main():
    args = parse_args()
    if args.setup:
        setup_tables(args.query_timeout_ms)
        return

    levels = [int(x) for x in args.levels.split(",")]
    mix = parse_mix(args.mix)
    total = (args.baseline_seconds + len(levels) * args.step_seconds
             + len(levels) * args.recovery_seconds)
    print(f"Gradini: {levels} | mix: {mix}")
    print(f"Durata massima: circa {total // 60} minuti")
    if not args.yes:
        answer = input("Confermi che questo carico e' autorizzato? (s/n): ")
        if answer.strip().lower() not in ("s", "si", "sì"):
            print("Test annullato.")
            return

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = PROJECT_DIR / "runs" / run_id
    run_dir.mkdir(parents=True)

    customer_ids = load_customer_ids(args.query_timeout_ms)
    print("Controllo preliminare delle query:")
    preflight(mix, customer_ids, run_id, args.query_timeout_ms)

    recorder = Recorder(run_dir / "requests.csv")
    recorder.start()
    connections = queue.Queue()
    summaries = []
    stop_reason = "ramp_completed"

    def execute(phase, concurrency, seconds):
        print(f"\n[{phase}] connessioni={concurrency}, {seconds} s")
        result = run_phase(phase, concurrency, seconds, args, mix, customer_ids,
                           run_id, recorder, connections)
        summaries.append(result)
        print(f"  qps={result['throughput_qps']} p50={result['p50_s']} "
              f"p95={result['p95_s']} errori={result['error_pct']}% "
              f"cpu_client={result['client_cpu_pct']}%")
        return result

    try:
        # Baseline: solo la sonda SELECT 1, nessun carico aggiuntivo.
        execute("baseline", 0, args.baseline_seconds)
        for index, level in enumerate(levels, start=1):
            result = execute(f"step_{index:02d}", level, args.step_seconds)
            execute(f"recovery_{index:02d}", 0, args.recovery_seconds)
            if result["aborted"]:
                stop_reason = f"aborted_at_{level}_connections"
            elif result["error_pct"] > args.stop_error_pct:
                stop_reason = f"error_pct_at_{level}_connections"
            elif result["p95_s"] is not None and result["p95_s"] > args.stop_p95_s:
                stop_reason = f"p95_at_{level}_connections"
            else:
                continue
            print(f"\nTest fermato: {stop_reason}")
            break
    except KeyboardInterrupt:
        stop_reason = "interrupted_by_user"
        print("\nTest interrotto dall'utente.")
    finally:
        recorder.finished.set()
        recorder.join(timeout=30)

        with (run_dir / "connections.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["phase", "concurrency", "worker_id", "server_host", "connection_id"])
            while not connections.empty():
                writer.writerow(connections.get())

        columns = []
        for summary in summaries:
            columns += [key for key in summary if key not in columns]
        with (run_dir / "steps.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(summaries)

        settings = {k: v for k, v in vars(args).items() if k != "yes"}
        manifest = {"run_id": run_id, "settings": settings, "mix": mix,
                    "stop_reason": stop_reason, "phases": summaries}
        (run_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
    print(f"\nRisultati in {run_dir}")
    print(f"Prossimo passo: python analyze_limit.py {run_dir}")


if __name__ == "__main__":
    main()