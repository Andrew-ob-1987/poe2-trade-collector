
"""On-demand historical backfill for GGG PoE 2 exchange data."""

import json
import logging
from datetime import datetime, timezone

from azure.core.exceptions import ResourceNotFoundError
from ggg_currency_exchange import fetch_digest

STATE_BLOB = "ggg/poe2/backfill_cursor.json"
HOURLY_PREFIX = "ggg/poe2/hourly"
EMPTY_PREFIX = "ggg/poe2/empty"
HOUR = 3600


def now():
    return datetime.now(timezone.utc).isoformat()


def valid_id(value):
    return (
        type(value) is int
        and value > 0
        and value % HOUR == 0
    )


def read_state(container):
    try:
        data = container.download_blob(STATE_BLOB).readall()
        return json.loads(data)
    except ResourceNotFoundError:
        return None


def save_state(container, state):
    state["updated_at"] = now()
    container.upload_blob(
        name=STATE_BLOB,
        data=json.dumps(state),
        overwrite=True,
    )


def blob_exists(container, name):
    return container.get_blob_client(name).exists()


def run_backfill(
    blob_service_client,
    container_name="poe2-prices",
    start_id=None,
    end_id=None,
    batch_size=10,
):
    if type(batch_size) is not int or not 1 <= batch_size <= 24:
        raise ValueError("batch_size must be between 1 and 24")

    container = blob_service_client.get_container_client(
        container_name
    )

    state = read_state(container)

    if start_id is not None or end_id is not None:
        if not valid_id(start_id) or not valid_id(end_id):
            raise ValueError(
                "start_id and end_id must be hour-aligned integers"
            )

        if start_id > end_id:
            raise ValueError("start_id must not exceed end_id")

        if state and state.get("status") == "running":
            raise ValueError(
                "A backfill is already active. Resume it without "
                "start_id/end_id, or finish it before starting another."
            )

        state = {
            "start_id": start_id,
            "end_id": end_id,
            "next_change_id": start_id,
            "processed": 0,
            "saved": 0,
            "skipped_existing": 0,
            "empty": 0,
            "status": "running",
        }
        save_state(container, state)

    elif not state:
        raise ValueError(
            "No backfill exists. Supply start_id and end_id."
        )

    if state["status"] == "completed":
        return state

    for _ in range(batch_size):
        change_id = state["next_change_id"]

        if change_id > state["end_id"]:
            state["status"] = "completed"
            save_state(container, state)
            break

        # Fetch even if a blob exists: the response gives the
        # authoritative next_change_id.
        digest = fetch_digest(change_id)
        next_id = digest.get("next_change_id")
        markets = digest.get("markets")

        if not valid_id(next_id) or next_id <= change_id:
            raise ValueError(
                f"Invalid next_change_id for {change_id}: {next_id}"
            )

        if not isinstance(markets, list):
            raise ValueError(
                f"Missing markets array for {change_id}"
            )

        if markets:
            blob_name = f"{HOURLY_PREFIX}/{change_id}.json"

            if blob_exists(container, blob_name):
                state["skipped_existing"] += 1
            else:
                payload = {
                    "change_id": change_id,
                    "next_change_id": next_id,
                    "retrieved_at": now(),
                    "markets": markets,
                }
                container.upload_blob(
                    name=blob_name,
                    data=json.dumps(payload, separators=(",", ":")),
                    overwrite=False,
                )
                state["saved"] += 1
        else:
            marker = f"{EMPTY_PREFIX}/{change_id}.json"
            container.upload_blob(
                name=marker,
                data=json.dumps({
                    "change_id": change_id,
                    "next_change_id": next_id,
                    "checked_at": now(),
                    "status": "empty_markets",
                }),
                overwrite=True,
            )
            state["empty"] += 1

        state["processed"] += 1
        state["next_change_id"] = next_id

        if next_id > state["end_id"]:
            state["status"] = "completed"

        # Checkpoint after each successfully processed hour.
        save_state(container, state)

        logging.info(
            "Backfill %s: %d markets; next=%s",
            change_id,
            len(markets),
            next_id,
        )

        if state["status"] == "completed":
            break

    return state
