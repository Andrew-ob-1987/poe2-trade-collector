
"""Guarded, single-day upgrade of independently audited GGG daily manifests."""

import hashlib
import json
from datetime import date, datetime, timezone

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError
from ggg_compaction import daily_paths

AUDIT_ROOT = "analytics/ggg/poe2/verification_jobs/v1/jobs"
BACKUP_ROOT = "analytics/ggg/poe2/manifest_upgrade_backups/v1"


def _load(blob):
    return json.loads(blob.download_blob().readall())


def upgrade_one_day(container, day_string, audit_job_id):
    day = date.fromisoformat(day_string)
    if day.isoformat() != day_string:
        raise ValueError("date must be YYYY-MM-DD")

    if (
        not isinstance(audit_job_id, str)
        or len(audit_job_id) != 32
        or any(c not in "0123456789abcdef" for c in audit_job_id)
    ):
        raise ValueError(
            "audit_job_id must be a 32-character lowercase hex ID"
        )

    _, expected_output, manifest_name = daily_paths(day)

    audit_name = (
        f"{AUDIT_ROOT}/{audit_job_id}/results/{day_string}.json"
    )
    audit_blob = container.get_blob_client(audit_name)
    audit = _load(audit_blob)

    if (
        audit.get("result") != "PASS"
        or audit.get("version") != 1
        or audit.get("date") != day_string
        or audit.get("manifest") != manifest_name
        or audit.get("hours") != 24
        or not isinstance(audit.get("rows"), int)
        or audit.get("rows") <= 0
    ):
        raise ValueError(
            "Audit result is not a matching successful version 1 audit"
        )

    input_hash = audit.get("input_sha256")
    output_hash = audit.get("output_sha256")

    if (
        not isinstance(input_hash, str)
        or len(input_hash) != 64
        or any(c not in "0123456789abcdef" for c in input_hash)
        or input_hash != output_hash
    ):
        raise ValueError("Audit fingerprints missing or mismatched")

    manifest_blob = container.get_blob_client(manifest_name)
    props = manifest_blob.get_blob_properties()
    current_etag = str(props.etag)

    original_bytes = manifest_blob.download_blob(
        etag=props.etag,
        match_condition=MatchConditions.IfNotModified,
    ).readall()

    record = json.loads(original_bytes)

    backup_name = (
        f"{BACKUP_ROOT}/jobs/{audit_job_id}/{day_string}.json"
    )
    backup_blob = container.get_blob_client(backup_name)
    original_audit_etag = audit.get("manifest_etag")

    # Support safe retries after an upgrade.
    if record.get("verification_version") == 2:
        if (
            record.get("upgrade_audit_job_id") == audit_job_id
            and record.get("input_sha256") == input_hash
            and record.get("output_sha256") == output_hash
            and backup_blob.exists()
        ):
            return {
                "status": "already_upgraded",
                "date": day_string,
                "manifest": manifest_name,
                "backup": backup_name,
            }

        raise ValueError(
            "Manifest is already version 2 "
            "or was changed by another process"
        )

    # Ensure the manifest has not changed since the audit.
    if current_etag != original_audit_etag:
        raise ValueError(
            "Manifest ETag differs from independent audit; "
            "refusing upgrade"
        )

    if (
        record.get("date") != day_string
        or record.get("status") != "verified"
        or record.get("hours") != 24
        or record.get("rows") != audit["rows"]
        or record.get("output") != expected_output
        or not isinstance(record.get("sources"), list)
        or len(record["sources"]) != 24
    ):
        raise ValueError(
            "Original manifest metadata does not match audit"
        )

    # Create a deterministic backup that cannot be overwritten.
    try:
        backup_blob.upload_blob(
            original_bytes,
            overwrite=False,
        )
    except ResourceExistsError:
        if backup_blob.download_blob().readall() != original_bytes:
            raise ValueError(
                "Existing backup differs from original manifest"
            )

    # Verify backup before modifying the original.
    if backup_blob.download_blob().readall() != original_bytes:
        raise RuntimeError(
            "Backup readback verification failed"
        )

    updated = dict(record)

    updated.update({
        "verification_version": 2,
        "input_sha256": input_hash,
        "output_sha256": output_hash,
        "upgraded_at": datetime.now(timezone.utc).isoformat(),
        "upgrade_audit_job_id": audit_job_id,
        "upgrade_original_etag": original_audit_etag,
        "upgrade_backup": backup_name,
        "upgrade_original_sha256": hashlib.sha256(
            original_bytes
        ).hexdigest(),
    })

    payload = json.dumps(updated, indent=2).encode("utf-8")

    # Conditional update prevents overwriting concurrent changes.
    try:
        manifest_blob.upload_blob(
            payload,
            overwrite=True,
            etag=props.etag,
            match_condition=MatchConditions.IfNotModified,
        )
    except ResourceModifiedError as exc:
        raise ValueError(
            "Manifest changed concurrently; upgrade not applied"
        ) from exc

    # Read back the updated manifest.
    confirmed = _load(manifest_blob)

    if confirmed != updated:
        raise RuntimeError(
            "Manifest readback verification failed"
        )

    return {
        "status": "upgraded",
        "date": day_string,
        "rows": updated["rows"],
        "verification_version": 2,
        "manifest": manifest_name,
        "backup": backup_name,
        "sha256": output_hash,
        "cleanup_status": updated.get("cleanup_status"),
    }
