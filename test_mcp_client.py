
import asyncio
import json
import math
import sys
from pathlib import Path

from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters


# Configurazione delle query PromQL
GROUP_LABEL = "name"
WINDOW = "1m"

W, L = WINDOW, GROUP_LABEL


# (titolo, unità di misura, query per replica, query totale)
METRICS = [
    (
        "Query al secondo (QPS)",
        "q/s",
        f"sum by ({L}) (rate(mysql_global_status_queries[{W}]))",
        f"sum(rate(mysql_global_status_queries[{W}]))",
    ),
    (
        "Thread in esecuzione (threads_running)",
        "thread",
        f"sum by ({L}) (mysql_global_status_threads_running)",
        "sum(mysql_global_status_threads_running)",
    ),
    (
        "Uso delle connessioni (connected / max_connections)",
        "%",
        f"100 * sum by ({L}) (mysql_global_status_threads_connected)"
        f" / sum by ({L}) (mysql_global_variables_max_connections)",
        "100 * sum(mysql_global_status_threads_connected)"
        " / sum(mysql_global_variables_max_connections)",
    ),
    (
        "Hit ratio del buffer pool InnoDB",
        "%",
        f"100 * (1 - sum by ({L}) "
        f"(rate(mysql_global_status_innodb_buffer_pool_reads[{W}]))"
        f" / sum by ({L}) "
        f"(rate(mysql_global_status_innodb_buffer_pool_read_requests[{W}])))",
        f"100 * (1 - sum(rate(mysql_global_status_innodb_buffer_pool_reads[{W}]))"
        f" / sum(rate(mysql_global_status_innodb_buffer_pool_read_requests[{W}])))",
    ),
    (
        "Tempo di attesa sui row lock InnoDB",
        "ms/s",
        f"sum by ({L}) (rate(mysql_global_status_innodb_row_lock_time[{W}]))",
        f"sum(rate(mysql_global_status_innodb_row_lock_time[{W}]))",
    ),
]


def extract_result(tool_result):
    """Controlla la risposta MCP ed estrae i risultati Prometheus."""

    # Nell'SDK MCP, il campo si chiama isError.
    if getattr(tool_result, "isError", False):
        raise RuntimeError(f"Errore MCP: {tool_result.content}")

    for item in tool_result.content:
        if getattr(item, "type", None) == "text":
            response = json.loads(item.text)

            if response.get("status") != "success":
                raise RuntimeError(
                    response.get("error", "Errore nella risposta Prometheus")
                )

            return response["data"]["result"]

    raise RuntimeError("La risposta MCP non contiene testo JSON.")


def fmt(value):
    """Formatta un valore numerico, gestendo NaN e infinito."""
    value = float(value)

    if math.isnan(value) or math.isinf(value):
        return "n/d"

    return f"{value:,.2f}"


async def query(client, promql):
    """Invia una query PromQL attraverso lo strumento MCP."""
    result = await client.call_tool(
        "query_prometheus",
        {"promql": promql},
    )
    return extract_result(result)


async def print_metric(client, title, unit, query_replica, query_total):
    """Recupera e stampa i valori per replica e il totale."""
    print(f"\n{title} [{unit}]")

    # Recupera i valori delle singole repliche.
    rows = await query(client, query_replica)

    rows = sorted(
        rows,
        key=lambda row: row.get("metric", {}).get(L, ""),
    )

    for row in rows:
        name = row.get("metric", {}).get(L, "?")
        value = row["value"][1]
        print(f"  {name:<12} {fmt(value):>14}")

    # Recupera il totale con la query dedicata.
    total_rows = await query(client, query_total)

    total = total_rows[0]["value"][1] if total_rows else "nan"

    print(f"  {'TOTALE':<12} {fmt(total):>14}")


async def main():
    """Avvia il server MCP e interroga le metriche."""

    project_dir = Path(__file__).resolve().parent
    server_file = project_dir / "prometheus_mcp_server.py"

    if not server_file.exists():
        raise FileNotFoundError(
            f"Server MCP non trovato: {server_file}"
        )

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[str(server_file)],
        cwd=str(project_dir),
    )

    # Connessione al server tramite stdio.
    async with stdio_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as client:
            await client.initialize()

            for title, unit, query_replica, query_total in METRICS:
                try:
                    await print_metric(
                        client,
                        title,
                        unit,
                        query_replica,
                        query_total,
                    )
                except Exception as exc:
                    print(f"  errore: {exc}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"\nErrore durante l'esecuzione: {exc}")
