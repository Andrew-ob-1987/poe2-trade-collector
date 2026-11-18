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
from ggg_backfill import run_backfill
from ggg_parquet import convert_hour
from ggg_compaction import (
    compact_day,
    compact_pending_days,
    preview_compacted_day_cleanup,
)


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

@app.route(
    route="ggg/backfill",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
def backfill_ggg_history(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        if not isinstance(body, dict):
            raise ValueError("Request body must be a JSON object")

        start_id = body.get("start_id")
        end_id = body.get("end_id")
        batch_size = body.get("batch_size", 10)

        blob_service = BlobServiceClient(
            account_url=(
                "https://poe2tradecollectorf8fb3.blob.core.windows.net"
            ),
            credential=DefaultAzureCredential(),
        )

        result = run_backfill(
            blob_service_client=blob_service,
            container_name="poe2-prices",
            start_id=start_id,
            end_id=end_id,
            batch_size=batch_size,
        )

        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json",
        )

    except (ValueError, TypeError) as exc:
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=400,
            mimetype="application/json",
        )

    except Exception:
        logging.exception("GGG historical backfill failed")
        return func.HttpResponse(
            json.dumps({
                "error": "Backfill failed. Check Azure function logs."
            }),
            status_code=500,
            mimetype="application/json",
        )

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="backfill_timer",
    run_on_startup=False,
    use_monitor=True,
)
def auto_backfill_ggg_history(
    backfill_timer: func.TimerRequest,
) -> None:
    """Resume an active GGG backfill every five minutes."""

    logging.info("Checking for active GGG historical backfill")

    blob_service = BlobServiceClient(
        account_url=(
            "https://poe2tradecollectorf8fb3.blob.core.windows.net"
        ),
        credential=DefaultAzureCredential(),
    )

    container = blob_service.get_container_client("poe2-prices")

    from ggg_backfill import read_state

    state = read_state(container)

    if not state:
        logging.info("No historical backfill configured")
        return

    if state.get("status") == "completed":
        logging.info("Historical backfill already completed")
        return

    if state.get("status") != "running":
        logging.warning("Unknown backfill status: %s", state.get("status"))
        return

    result = run_backfill(
        blob_service_client=blob_service,
        container_name="poe2-prices",
        batch_size=24,
    )

    logging.info(
        "Backfill progress: %d processed, %d saved, status=%s",
        result["processed"],
        result["saved"],
        result["status"],
    )

@app.route(
    route="ggg/convert-hour",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
def convert_ggg_hour(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        change_id = body.get("change_id")

        if type(change_id) is not int or change_id <= 0 or change_id % 3600:
            raise ValueError("change_id must be a positive hour-aligned integer")

        blob_service = BlobServiceClient(
            account_url="https://poe2tradecollectorf8fb3.blob.core.windows.net",
            credential=DefaultAzureCredential(),
        )

        container = blob_service.get_container_client("poe2-prices")

        blob_name, row_count = convert_hour(container, change_id)

        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "change_id": change_id,
                "parquet_blob": blob_name,
                "rows": row_count,
            }),
            status_code=200,
            mimetype="application/json",
        )

    except (ValueError, KeyError, TypeError) as exc:
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=400,
            mimetype="application/json",
        )

    except Exception:
        logging.exception("GGG Parquet conversion failed")
        return func.HttpResponse(
            json.dumps({"error": "Conversion failed; check function logs"}),
            status_code=500,
            mimetype="application/json",
        )

@app.blob_trigger(
    arg_name="blob",
    path="poe2-prices/ggg/poe2/hourly/{change_id}.json",
    connection="AzureWebJobsStorage",
    source="EventGrid",
)
def convert_ggg_blob(blob: func.InputStream) -> None:
    change_id = int(blob.name.rsplit("/", 1)[-1].removesuffix(".json"))

    blob_service = BlobServiceClient(
        account_url="https://poe2tradecollectorf8fb3.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )

    container = blob_service.get_container_client("poe2-prices")
    target, rows = convert_hour(container, change_id)

    logging.info(
        "Converted GGG snapshot %d to %s (%d rows)",
        change_id,
        target,
        rows,
    )

@app.route(
    route="ggg/compact-day",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
def compact_ggg_day(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        day = body.get("date")

        if not isinstance(day, str):
            raise ValueError("date must be a YYYY-MM-DD string")

        blob_service = BlobServiceClient(
            account_url="https://poe2tradecollectorf8fb3.blob.core.windows.net",
            credential=DefaultAzureCredential(),
        )

        container = blob_service.get_container_client("poe2-prices")
        result = compact_day(container, day)

        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json",
        )

    except (ValueError, TypeError) as exc:
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=400,
            mimetype="application/json",
        )

    except Exception:
        logging.exception("GGG daily compaction failed")
        return func.HttpResponse(
            json.dumps({"error": "Compaction failed; check function logs"}),
            status_code=500,
            mimetype="application/json",
        )

@app.timer_trigger(
    schedule="0 */10 * * * *",
    arg_name="compaction_timer",
    run_on_startup=False,
    use_monitor=True,
)
def auto_compact_ggg_days(
    compaction_timer: func.TimerRequest,
) -> None:
    """Compact up to five completed UTC days every ten minutes."""

    blob_service = BlobServiceClient(
        account_url="https://poe2tradecollectorf8fb3.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )

    container = blob_service.get_container_client("poe2-prices")
    results = compact_pending_days(container, max_days=5)

    logging.info(
        "GGG daily compaction completed: %d days",
        len(results),
    )

    for result in results:
        logging.info(
            "Compacted %s: %d rows",
            result["date"],
            result["rows"],
        )


@app.route(
    route="ggg/cleanup-day",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
def cleanup_ggg_day(req: func.HttpRequest) -> func.HttpResponse:
    """Read-only preview of hourly Parquet cleanup."""
    try:
        body = req.get_json()

        if not isinstance(body, dict):
            raise ValueError("Request body must be a JSON object")

        day = body.get("date")
        dry_run = body.get("dry_run")

        if not isinstance(day, str):
            raise ValueError("date must be a YYYY-MM-DD string")

        if dry_run is not True:
            raise ValueError(
                "Only dry_run=true is supported. "
                "Deletion is disabled."
            )

        blob_service = BlobServiceClient(
            account_url=(
                "https://poe2tradecollectorf8fb3.blob.core.windows.net"
            ),
            credential=DefaultAzureCredential(),
        )

        container = blob_service.get_container_client("poe2-prices")

        result = preview_compacted_day_cleanup(container, day)

        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json",
        )

    except (ValueError, TypeError, KeyError) as exc:
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=400,
            mimetype="application/json",
        )

    except Exception:
        logging.exception("GGG cleanup preview failed")
        return func.HttpResponse(
            json.dumps({
                "error": "Cleanup preview failed; check function logs"
            }),
            status_code=500,
            mimetype="application/json",
        )
