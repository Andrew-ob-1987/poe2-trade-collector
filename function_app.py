from ggg_currency_exchange import collect_ggg_currency_exchange
import azure.functions as func
import logging
import json
import urllib.request
import urllib.parse
import os

from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from poe2scout_history import collect_history

app = func.FunctionApp()

BASE_URL = "https://poe.ninja/poe2/api/economy"

CATEGORIES = [
    "Currency",
    "Fragments",
    "Abyss",
    "UncutGems",
    "LineageSupportGems",
    "Essences",
    "SoulCores",
    "Idols",
    "Runes",
    "Ritual",
    "Expedition",
    "Delirium",
    "Breach",
    "Verisium",
]

HEADERS = {
    "User-Agent": "poe2-trade-collector/0.1 (personal economy research; contact: ob61887@gmail.com)",
    "Accept": "application/json",
}

def fetch_json(url):
    request = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)

@app.timer_trigger(
    schedule="0 0 * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def collect_poe2_prices(timer: func.TimerRequest) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    logging.info("PoE 2 currency collector started: %s", timestamp)

    try:
        leagues = fetch_json(f"{BASE_URL}/leagues")
        if not leagues:
            raise ValueError("No active leagues returned")

        league = leagues[0]["id"]
        logging.info("Collecting league: %s", league)

        total_items = 0
        failures = []

        for category in CATEGORIES:
            try:
                params = urllib.parse.urlencode({
                    "league": league,
                    "type": category,
                })
                url = f"{BASE_URL}/exchange/current/overview?{params}"

                data = fetch_json(url)
                lines = data.get("lines", [])
                total_items += len(lines)

                logging.info(
                    "Category %s: %d price records; reference currency: %s",
                    category,
                    len(lines),
                    data.get("core", {}).get("primary"),
                )

            except Exception as exc:
                failures.append(category)
                logging.error(
                    "Failed to collect %s: %s",
                    category,
                    str(exc),
                )

        logging.info(
            "Collection finished. Total records: %d; failed categories: %s",
            total_items,
            failures,
        )

        if failures:
            raise RuntimeError(
                f"Collection failed for: {', '.join(failures)}"
            )

    except Exception:
        logging.exception("PoE 2 collector execution failed")
        raise

@app.timer_trigger(
    schedule="0 15 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def collect_poe2_history(timer: func.TimerRequest) -> None:
    logging.info("Starting POE2 Scout historical collection")

    account_url = (
        "https://poe2tradecollectorf8fb3.blob.core.windows.net"
    )

    credential = DefaultAzureCredential()

    blob_service = BlobServiceClient(
        account_url=account_url,
        credential=credential
    )

    count = collect_history(
        league="forbiddenrites",
        blob_service_client=blob_service,
        container_name="poe2-prices"
    )

    logging.info(
        "Historical collection completed: %s currencies",
        count
    )


@app.timer_trigger(
    schedule="0 10 * * * *",
    arg_name="ggg_timer",
    run_on_startup=False,
    use_monitor=True,
)
def collect_ggg_hourly(ggg_timer: func.TimerRequest) -> None:
    logging.info("Starting GGG hourly Currency Exchange collection")

    blob_service = BlobServiceClient(
        account_url="https://poe2tradecollectorf8fb3.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )

    count = collect_ggg_currency_exchange(
        blob_service_client=blob_service,
        container_name="poe2-prices",
    )

    logging.info("GGG collection completed: %d hourly digests", count)
