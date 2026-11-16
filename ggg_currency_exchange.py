
"""Collect hourly PoE 2 currency exchange data from GGG's public CDN."""

import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timezone

from azure.core.exceptions import ResourceNotFoundError

BASE_URL = "https://web.poecdn.com/api/currency-exchange/poe2"
CURSOR_BLOB = "ggg/poe2/cursor.json"
DEFAULT_START_ID = 1794700800
HOUR = 3600


def fetch_digest(change_id):
    request = urllib.request.Request(
        f"{BASE_URL}/{change_id}",
        headers={
            "User-Agent": "poe2-trade-collector/0.3",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_cursor(container):
    try:
        blob = container.download_blob(CURSOR_BLOB).readall()
    except ResourceNotFoundError:
        return None

    cursor = json.loads(blob)["next_change_id"]
    if type(cursor) is not int or cursor <= 0 or cursor % HOUR:
        raise ValueError(f"Invalid stored cursor: {cursor!r}")
    return cursor


def collect_ggg_currency_exchange(
    blob_service_client,
    container_name="poe2-prices",
):
    container = blob_service_client.get_container_client(container_name)

    change_id = load_cursor(container)
    if change_id is None:
        change_id = int(
            os.getenv("GGG_START_CHANGE_ID", str(DEFAULT_START_ID))
        )

    if change_id <= 0 or change_id % HOUR:
        raise ValueError("Starting ID must be an hour-aligned timestamp")

    max_digests = int(os.getenv("GGG_MAX_DIGESTS_PER_RUN", "24"))
    if not 1 <= max_digests <= 168:
        raise ValueError("GGG_MAX_DIGESTS_PER_RUN must be 1 through 168")

    # Only collect hours that have finished.
    last_completed_hour = (int(time.time()) // HOUR - 1) * HOUR

    collected = 0

    for _ in range(max_digests):
        if change_id > last_completed_hour:
            logging.info("GGG collector is caught up")
            break

        digest = fetch_digest(change_id)
        next_id = digest.get("next_change_id")
        markets = digest.get("markets")

        if (
            type(next_id) is not int
            or next_id <= change_id
            or next_id % HOUR
        ):
            raise ValueError(f"Invalid next_change_id: {next_id!r}")

        if not isinstance(markets, list):
            raise ValueError("GGG response has no markets array")

        # Persist raw data before advancing the cursor.
        blob_name = f"ggg/poe2/hourly/{change_id}.json"

        payload = {
            "change_id": change_id,
            "next_change_id": next_id,
            "retrieved_at": utc_now(),
            "markets": markets,
        }

        container.upload_blob(
            name=blob_name,
            data=json.dumps(payload, separators=(",", ":")),
            overwrite=True,
        )

        container.upload_blob(
            name=CURSOR_BLOB,
            data=json.dumps({
                "next_change_id": next_id,
                "last_processed_change_id": change_id,
                "updated_at": utc_now(),
            }),
            overwrite=True,
        )

        logging.info(
            "Saved GGG digest %s: %s markets, next=%s",
            change_id,
            len(markets),
            next_id,
        )

        change_id = next_id
        collected += 1

    return collected
