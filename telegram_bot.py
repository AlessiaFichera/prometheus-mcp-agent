"""Collegamento con la persona: invia un messaggio Telegram e aspetta OK o Esplora.

Prova del collegamento, senza agente e senza token LLM:
    python telegram_bot.py --test
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

API_BASE = os.getenv("TELEGRAM_API_BASE", "https://api.telegram.org")
MAX_LENGTH = 4000  # Telegram accetta al massimo 4096 caratteri per messaggio.
CHAT_FILE = Path(__file__).resolve().parent / "telegram_chat_id.txt"


class TelegramError(RuntimeError):
    pass


def call(method, http_timeout=40, **params):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise TelegramError("Manca TELEGRAM_BOT_TOKEN nel file .env")
    url = f"{API_BASE}/bot{token}/{method}"
    data = json.dumps(params).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=http_timeout) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        # Non mostriamo mai l'indirizzo: contiene il token.
        raise TelegramError(f"Telegram ha risposto con errore {error.code} su {method}") from None
    except urllib.error.URLError as error:
        raise TelegramError(f"Telegram non raggiungibile: {error.reason}") from None
    if not result.get("ok"):
        raise TelegramError(f"Errore Telegram su {method}: {result.get('description')}")
    return result["result"]


def find_chat_id():
    """Usa la chat gia' salvata, oppure quella dell'ultimo messaggio ricevuto dal bot.

    La salviamo perche' Telegram cancella i messaggi gia' letti dal bot.
    """
    configured = os.getenv("TELEGRAM_CHAT_ID")
    if configured:
        return int(configured)
    if CHAT_FILE.exists():
        return int(CHAT_FILE.read_text(encoding="utf-8").strip())
    for update in reversed(call("getUpdates", http_timeout=20)):
        chat = (update.get("message") or {}).get("chat") or {}
        if chat.get("type") == "private":
            CHAT_FILE.write_text(str(chat["id"]), encoding="utf-8")
            return chat["id"]
    raise TelegramError("Nessun messaggio trovato: apri il bot, premi Avvia e scrivigli 'ciao'.")


def next_offset():
    """Ignora le risposte vecchie: conta solo cio' che arriva dopo l'invio."""
    updates = call("getUpdates", http_timeout=20)
    return updates[-1]["update_id"] + 1 if updates else None


def send_decision_request(chat_id, text, second_label="Esplora"):
    keyboard = {"inline_keyboard": [[
        {"text": "OK", "callback_data": "ok"},
        {"text": second_label, "callback_data": "esplora"},
    ]]}
    return call("sendMessage", chat_id=chat_id, text=text[:MAX_LENGTH], reply_markup=keyboard)


def wait_for_decision(chat_id, wait_minutes=30, offset=None):
    """Restituisce 'ok', 'esplora', oppure 'nessuna_risposta' allo scadere del tempo."""
    deadline = time.monotonic() + wait_minutes * 60
    while time.monotonic() < deadline:
        poll = min(25, max(1, int(deadline - time.monotonic())))
        params = {"timeout": poll}
        if offset is not None:
            params["offset"] = offset
        for update in call("getUpdates", http_timeout=poll + 15, **params):
            offset = update["update_id"] + 1
            query = update.get("callback_query")
            if query and query.get("message", {}).get("chat", {}).get("id") == chat_id:
                call("answerCallbackQuery", callback_query_id=query["id"])
                if query.get("data") in ("ok", "esplora"):
                    return query["data"]
            message = update.get("message") or {}
            if message.get("chat", {}).get("id") == chat_id:
                text = (message.get("text") or "").strip().lower()
                if text in ("ok", "esplora"):
                    return text
    return "nessuna_risposta"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--test", action="store_true", required=True)
    parser.add_argument("--wait-minutes", type=float, default=5)
    args = parser.parse_args()

    chat_id = find_chat_id()
    offset = next_offset()
    send_decision_request(chat_id, "Prova del monitor: scegli OK o Esplora.")
    print("Messaggio inviato: rispondi su Telegram...")
    decision = wait_for_decision(chat_id, args.wait_minutes, offset)
    call("sendMessage", chat_id=chat_id, text=f"Risposta ricevuta: {decision}")
    print(f"Risposta: {decision}")


if __name__ == "__main__":
    main()