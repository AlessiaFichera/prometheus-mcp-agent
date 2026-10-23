
import asyncio
import json
import urllib.parse
import urllib.request

from mcp.server.fastmcp import FastMCP


# Configurazione di Prometheus
PROM_HOST = "35.84.236.220"
PROM_PORT = 9090

PROMETHEUS_URL = (
    f"http://{PROM_HOST}:{PROM_PORT}/api/v1/query"
)


# Creazione del server MCP
server = FastMCP("prometheus-mcp-server")


def _execute_prometheus_query(promql: str) -> str:
    """Esegue una query PromQL tramite l'API HTTP di Prometheus."""

    if not promql or not promql.strip():
        raise ValueError("La query PromQL non può essere vuota.")

    params = urllib.parse.urlencode({"query": promql})
    url = f"{PROMETHEUS_URL}?{params}"

    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json"},
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)

    if result.get("status") != "success":
        raise RuntimeError(
            result.get("error", "Errore durante la query Prometheus")
        )

    return json.dumps(
        result,
        ensure_ascii=False,
        indent=2,
    )


@server.tool(
    name="query_prometheus",
    description=(
        "Esegue una query PromQL su Prometheus e restituisce "
        "il risultato in formato JSON. Accetta una query PromQL."
    ),
)
async def query_prometheus(promql: str) -> str:
    """Strumento MCP per recuperare dati da Prometheus."""

    return await asyncio.to_thread(
        _execute_prometheus_query,
        promql,
    )


async def main():
    await server.run_stdio_async()


if __name__ == "__main__":
    server.run()
