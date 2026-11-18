
"""Persistent, read-only GGG daily verification job."""

import io
import json
import uuid
import threading
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError
from azure.storage.blob import BlobLeaseClient

from ggg_compaction import daily_paths, table_fingerprint
from ggg_parquet import SCHEMA

from azure.core.exceptions import AzureError

import logging

ROOT = "analytics/ggg/poe2/verification_jobs/v1"
MANIFEST_PREFIX = "analytics/ggg/poe2/manifests/daily_v1/"
STATE = ROOT + "/active.json"


def _json(blob):
    return json.loads(blob.download_blob().readall())


def _save(blob, value, **kwargs):
    blob.upload_blob(
        json.dumps(value, indent=2).encode(),
        overwrite=True,
        **kwargs,
    )


def _now():
    return datetime.now(timezone.utc).isoformat()


def _lock(container):
    blob = container.get_blob_client(STATE)

    try:
        blob.upload_blob(b"{}", overwrite=False)
    except ResourceExistsError:
        pass

    lease = BlobLeaseClient(blob)
    lease.acquire(lease_duration=60)
    return blob, lease


class RenewingLease:
    """Renew a 60-second lease until the operation finishes."""

    def __init__(self, lease):
        self.lease = lease
        self.stop = threading.Event()
        self.thread = threading.Thread(
            target=self._renew,
            daemon=True,
        )

    def _renew(self):
        while not self.stop.wait(20):
            try:
                self.lease.renew()
            except Exception:
                logging.exception(
                    "GGG verification job lease renewal failed"
                )
                break

    def __enter__(self):
        self.thread.start()
        return self.lease

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=2)
        self.lease.release()


def _job_prefix(job_id):
    return f"{ROOT}/jobs/{job_id}"


def start_job(container, version=1, limit=0):
    if type(version) is not int or version not in (1, 2):
        raise ValueError("version must be 1 or 2")

    if type(limit) is not int or limit < 0:
        raise ValueError("limit must be a nonnegative integer")

    state_blob, lease = _lock(container)

    with RenewingLease(lease):
        current = _json(state_blob)

        if current.get("status") == "running":
            raise ValueError("A verification job is already running")

        selected = []

        for item in container.list_blobs(
            name_starts_with=MANIFEST_PREFIX
        ):
            if not item.name.endswith(".json"):
                continue

            manifest_blob = container.get_blob_client(item.name)
            record = _json(manifest_blob)

            if record.get("verification_version", 1) == version:
                selected.append({
                    "date": record.get("date"),
                    "manifest": item.name,
                    "manifest_etag": str(
                        manifest_blob.get_blob_properties().etag
                    ),
                })

        selected.sort(key=lambda x: x["manifest"])

        if limit:
            selected = selected[:limit]

        job_id = uuid.uuid4().hex
        prefix = _job_prefix(job_id)

        _save(
            container.get_blob_client(prefix + "/selection.json"),
            selected,
        )

        state = {
            "job_id": job_id,
            "status": "running" if selected else "completed",
            "version": version,
            "total": len(selected),
            "next_index": 0,
            "passed": 0,
            "failed": 0,
            "created_at": _now(),
            "updated_at": _now(),
            "read_only": True,
        }

        _save(state_blob, state, lease=lease)
        return state


def status(container):
    blob = container.get_blob_client(STATE)

    if not blob.exists():
        return {"status": "not_started"}

    return _json(blob)


def verify_day(container, item):
    day_string = item["date"]
    day = date.fromisoformat(day_string)

    if day.isoformat() != day_string:
        raise ValueError("Invalid date")

    sources, target, manifest_name = daily_paths(day)

    if manifest_name != item["manifest"]:
        raise ValueError("Manifest path mismatch")

    manifest_blob = container.get_blob_client(manifest_name)

    if str(manifest_blob.get_blob_properties().etag) != item["manifest_etag"]:
        raise ValueError("Manifest changed since job started")

    record = _json(manifest_blob)

    if record.get("status") != "verified":
        raise ValueError("Manifest not verified")

    if record.get("hours") != 24:
        raise ValueError("Manifest does not cover 24 hours")

    if record.get("date") != day_string:
        raise ValueError("Manifest date mismatch")

    if record.get("output") != target:
        raise ValueError("Manifest output mismatch")

    recorded = record.get("sources")

    if not isinstance(recorded, list) or len(recorded) != 24:
        raise ValueError("Expected 24 recorded sources")

    start = datetime(
        day.year, day.month, day.day,
        tzinfo=timezone.utc,
    )

    tables = []
    count = 0

    for hour, path in enumerate(sources):
        change_id = int(
            (start + timedelta(hours=hour)).timestamp()
        )

        if recorded[hour].get("blob") != path:
            raise ValueError(
                f"Hour {hour}: manifest source path mismatch"
            )

        if recorded[hour].get("change_id") != change_id:
            raise ValueError(
                f"Hour {hour}: manifest change ID mismatch"
            )

        table = pq.read_table(
            io.BytesIO(container.download_blob(path).readall())
        )

        if not table.schema.equals(SCHEMA, check_metadata=False):
            raise ValueError(f"Hour {hour}: schema mismatch")

        if table.num_rows == 0:
            raise ValueError(f"Hour {hour}: empty source")

        if table.num_rows != recorded[hour].get("rows"):
            raise ValueError(f"Hour {hour}: row count mismatch")

        if any(
            x != change_id
            for x in table.column("change_id").to_pylist()
        ):
            raise ValueError(f"Hour {hour}: timestamp mismatch")

        tables.append(table)
        count += table.num_rows

    combined = pa.concat_tables(tables)

    daily = pq.read_table(
        io.BytesIO(container.download_blob(target).readall())
    )

    if not daily.schema.equals(SCHEMA, check_metadata=False):
        raise ValueError("Daily schema mismatch")

    if daily.num_rows != count:
        raise ValueError("Daily row count mismatch")

    if record.get("rows") != count:
        raise ValueError("Manifest row count mismatch")

    input_hash = table_fingerprint(combined)
    output_hash = table_fingerprint(daily)

    if input_hash != output_hash:
        raise ValueError("Source/daily SHA-256 mismatch")

    if record.get("verification_version", 1) == 2:
        if record.get("input_sha256") != input_hash:
            raise ValueError("V2 manifest input fingerprint mismatch")

        if record.get("output_sha256") != output_hash:
            raise ValueError("V2 manifest output fingerprint mismatch")

    return {
        "rows": count,
        "hours": 24,
        "input_sha256": input_hash,
        "output_sha256": output_hash,
        "manifest_etag": item["manifest_etag"],
    }


def run_one(container):
    """Process one day per timer invocation."""

    try:
        state_blob, lease = _lock(container)
    except ResourceModifiedError:
        return {"status": "busy"}
    except Exception as exc:
        if getattr(exc, "status_code", None) in (409, 412):
            return {"status": "busy"}
        raise

    with RenewingLease(lease):
        state = _json(state_blob)

        if state.get("status") != "running":
            return state

        index = state["next_index"]
        prefix = _job_prefix(state["job_id"])

        selected = _json(
            container.get_blob_client(
                prefix + "/selection.json"
            )
        )

        if index >= len(selected):
            state["status"] = "completed"
            state["updated_at"] = _now()
            _save(state_blob, state, lease=lease)
            return state

        item = selected[index]

        result_blob = container.get_blob_client(
            prefix + "/results/" + item["date"] + ".json"
        )

        if result_blob.exists():
            result = _json(result_blob)
        else:
            result = {
                "date": item["date"],
                "manifest": item["manifest"],
                "version": state["version"],
                "audited_at": _now(),
            }

            try:
                result.update(verify_day(container, item))
                result["result"] = "PASS"
            except AzureError:
                # Temporary storage failure: do not advance the cursor.
                # The next scheduled invocation will retry this day.
                raise
            except Exception as exc:
                result["result"] = "FAIL"
                result["error"] = (
                    f"{type(exc).__name__}: {exc}"
                )

            # Confirm we still own the job lease before
            # publishing the verification result.
            lease.renew()

            result_blob.upload_blob(
                json.dumps(result, indent=2).encode(),
                overwrite=False,
            )

        state["next_index"] += 1

        if result["result"] == "PASS":
            state["passed"] += 1
        else:
            state["failed"] += 1

        state["updated_at"] = _now()

        if state["next_index"] >= state["total"]:
            state["status"] = "completed"
            state["finished_at"] = _now()

        # Do not advance progress if our lease has expired.
        lease.renew()
        _save(state_blob, state, lease=lease)

        return {
            "status": state["status"],
            "date": item["date"],
            "result": result["result"],
            "processed": state["next_index"],
            "total": state["total"],
        }
