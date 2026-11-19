
"""Persistent, resumable GGG daily manifest V2 upgrade job."""

import json
import logging
import uuid
from datetime import datetime, timezone

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobLeaseClient

from ggg_manifest_upgrade import upgrade_one_day
from ggg_verification_job import RenewingLease

ROOT = "analytics/ggg/poe2/manifest_upgrade_jobs/v1"
STATE = ROOT + "/active.json"
AUDIT_ROOT = "analytics/ggg/poe2/verification_jobs/v1/jobs"


def _now():
    return datetime.now(timezone.utc).isoformat()


def _read(blob):
    return json.loads(blob.download_blob().readall())


def _write(blob, data, **kwargs):
    blob.upload_blob(
        json.dumps(data, indent=2).encode("utf-8"),
        overwrite=True,
        **kwargs,
    )


def _lock(container):
    blob = container.get_blob_client(STATE)

    try:
        blob.upload_blob(b"{}", overwrite=False)
    except ResourceExistsError:
        pass

    lease = BlobLeaseClient(blob)
    lease.acquire(lease_duration=60)
    return blob, lease


def _prefix(job_id):
    return f"{ROOT}/jobs/{job_id}"


def start_upgrade_job(container, audit_job_id):
    """Create a job from a completed, successful V1 audit."""

    if (
        not isinstance(audit_job_id, str)
        or len(audit_job_id) != 32
        or any(c not in "0123456789abcdef" for c in audit_job_id)
    ):
        raise ValueError("Invalid audit_job_id")

    state_blob, lease = _lock(container)

    with RenewingLease(lease):
        current = _read(state_blob)

        if current.get("status") in ("running", "failed"):
            raise ValueError(
                "An upgrade job already exists and is not completed"
            )

        audit_prefix = f"{AUDIT_ROOT}/{audit_job_id}"

        audit_state = _read(
            container.get_blob_client(
                "analytics/ggg/poe2/verification_jobs/v1/active.json"
            )
        )

        if (
            audit_state.get("job_id") != audit_job_id
            or audit_state.get("status") != "completed"
            or audit_state.get("version") != 1
            or audit_state.get("failed") != 0
            or audit_state.get("passed") != audit_state.get("total")
            or audit_state.get("total") != 135
        ):
            raise ValueError(
                "Expected the completed 135/135 successful V1 audit"
            )

        selection = _read(
            container.get_blob_client(
                audit_prefix + "/selection.json"
            )
        )

        if not isinstance(selection, list) or len(selection) != 135:
            raise ValueError("Audit selection must contain 135 days")

        days = [item["date"] for item in selection]

        if len(set(days)) != len(days):
            raise ValueError("Duplicate dates in audit selection")

        for item in selection:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("date"), str)
                or not isinstance(item.get("manifest"), str)
            ):
                raise ValueError("Invalid audit selection entry")

        job_id = uuid.uuid4().hex
        prefix = _prefix(job_id)

        container.get_blob_client(
            prefix + "/selection.json"
        ).upload_blob(
            json.dumps(days, indent=2).encode(),
            overwrite=False,
        )

        state = {
            "job_id": job_id,
            "audit_job_id": audit_job_id,
            "status": "running",
            "total": len(days),
            "next_index": 0,
            "upgraded": 0,
            "already_upgraded": 0,
            "failed": 0,
            "created_at": _now(),
            "updated_at": _now(),
            "cleanup_enabled": False,
        }

        _write(state_blob, state, lease=lease)
        return state


def upgrade_status(container):
    blob = container.get_blob_client(STATE)

    if not blob.exists():
        return {"status": "not_started"}

    return _read(blob)


def run_upgrade_one(container):
    """Upgrade at most one manifest, under an exclusive job lease."""

    try:
        state_blob, lease = _lock(container)
    except Exception as exc:
        if getattr(exc, "status_code", None) in (409, 412):
            return {"status": "busy"}
        raise

    with RenewingLease(lease):
        state = _read(state_blob)

        if state.get("status") != "running":
            return state

        index = state["next_index"]
        prefix = _prefix(state["job_id"])

        selection = _read(
            container.get_blob_client(prefix + "/selection.json")
        )

        if index >= len(selection):
            state["status"] = "completed"
            state["finished_at"] = _now()
            state["updated_at"] = _now()
            _write(state_blob, state, lease=lease)
            return state

        day = selection[index]

        result_blob = container.get_blob_client(
            prefix + "/results/" + day + ".json"
        )

        # A prior invocation may have saved the result but crashed
        # before advancing the cursor.
        if result_blob.exists():
            result = _read(result_blob)
        else:
            try:
                result = upgrade_one_day(
                    container,
                    day_string=day,
                    audit_job_id=state["audit_job_id"],
                )

                if result.get("status") not in (
                    "upgraded", "already_upgraded"
                ):
                    raise ValueError("Unexpected upgrade result")

                # Confirm the lease is still valid before publishing.
                lease.renew()

                result_blob.upload_blob(
                    json.dumps(result, indent=2).encode(),
                    overwrite=False,
                )

            except Exception as exc:
                logging.exception("Manifest upgrade failed for %s", day)

                state["status"] = "failed"
                state["failed"] += 1
                state["failed_date"] = day
                state["error"] = f"{type(exc).__name__}: {exc}"
                state["updated_at"] = _now()

                lease.renew()
                _write(state_blob, state, lease=lease)

                return {
                    "status": "failed",
                    "date": day,
                    "error": state["error"],
                }

        state["next_index"] += 1

        if result["status"] == "upgraded":
            state["upgraded"] += 1
        else:
            state["already_upgraded"] += 1

        state["updated_at"] = _now()

        if state["next_index"] >= state["total"]:
            state["status"] = "completed"
            state["finished_at"] = _now()

        lease.renew()
        _write(state_blob, state, lease=lease)

        return {
            "status": state["status"],
            "date": day,
            "result": result["status"],
            "processed": state["next_index"],
            "total": state["total"],
        }
