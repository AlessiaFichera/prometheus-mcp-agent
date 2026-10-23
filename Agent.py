
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_groq import ChatGroq
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent


PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

PROVIDER = os.getenv("LLM_PROVIDER", "gemini").lower()

METRICS = {
    "1": {
        "name": "Query al secondo (QPS)",
        "query": "sum by (name) (rate(mysql_global_status_queries[1m]))",
    },
    "2": {
        "name": "Thread MySQL in esecuzione",
        "query": "sum by (name) (mysql_global_status_threads_running)",
    },
    "3": {
        "name": "Percentuale di connessioni utilizzate",
        "query": (
            "100 * sum by (name) (mysql_global_status_threads_connected)"
            " / sum by (name) (mysql_global_variables_max_connections)"
        ),
    },
    "4": {
        "name": "Hit ratio del buffer pool InnoDB",
        "query": (
            "100 * (1 - sum by (name) "
            "(rate(mysql_global_status_innodb_buffer_pool_reads[1m])) "
            "/ sum by (name) "
            "(rate(mysql_global_status_innodb_buffer_pool_read_requests[1m])))"
        ),
    },
    "5": {
        "name": "Tempo di attesa sui row lock InnoDB",
        "query": (
            "sum by (name) "
            "(rate(mysql_global_status_innodb_row_lock_time[1m]))"
        ),
    },
}


def create_model():
    """Crea il modello linguistico selezionato nel file .env."""
    if PROVIDER == "gemini":
        return ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            temperature=0,
        )

    if PROVIDER == "groq":
        return ChatGroq(
            model="llama-3.3-70b-versatile",
            temperature=0,
        )

    raise ValueError(f"Provider non supportato: {PROVIDER}")


def choose_metrics():
    """Mostra il menu e restituisce le metriche selezionate."""
    print("\n" + "=" * 55)
    print("       PROMETHEUS MONITORING AGENT")
    print("=" * 55)
    print("Seleziona la metrica da monitorare:\n")

    for key, metric in METRICS.items():
        print(f"{key}. {metric['name']}")

    print("0. Tutte le metriche")

    while True:
        choice = input("\nInserisci la scelta (0-5): ").strip()

        if choice == "0":
            return list(METRICS.values())

        if choice in METRICS:
            return [METRICS[choice]]

        print("Scelta non valida. Inserisci un numero da 0 a 5.")


async def main():
    server_file = PROJECT_DIR / "prometheus_mcp_server.py"

    if not server_file.exists():
        raise FileNotFoundError(
            f"Server MCP non trovato: {server_file}"
        )

    selected_metrics = choose_metrics()

    client = MultiServerMCPClient(
        {
            "prometheus": {
                "transport": "stdio",
                "command": sys.executable,
                "args": [str(server_file)],
            }
        }
    )

    tools = await client.get_tools()

    print(f"\nProvider LLM: {PROVIDER}")
    print("Strumenti MCP disponibili:")
    for tool in tools:
        print(f"- {tool.name}")

    model = create_model()
    agent = create_react_agent(model, tools)

    for metric in selected_metrics:
        print("\n" + "=" * 55)
        print(metric["name"].upper())
        print("=" * 55)

        question = (
            f"Usa lo strumento MCP query_prometheus per eseguire "
            f"esattamente questa query PromQL:\n{metric['query']}\n\n"
            "Riporta i risultati restituiti da Prometheus per ogni "
            "replica, usando i nomi e i valori effettivi. "
            "Indica quale replica ha il valore più alto, se i dati "
            "lo consentono. Non inventare valori e non sostituire "
            "la query con altre metriche. Se la query restituisce "
            "un errore o nessun dato, segnalalo esplicitamente."
        )

        try:
            result = await agent.ainvoke(
                {
                    "messages": [
                        {"role": "user", "content": question}
                    ]
                }
            )

            content = result["messages"][-1].content

            if isinstance(content, str):
                print(content)
            elif isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        print(item.get("text", ""))
                    elif isinstance(item, str):
                        print(item)
            else:
                print(content)

        except Exception as exc:
            print(f"Errore durante il recupero della metrica: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
