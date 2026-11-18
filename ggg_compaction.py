
"""Verified daily compaction for GGG currency snapshots."""

import io
import json
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from ggg_parquet import ROOT, SCHEMA

import hashlib
from collections import Counter

DAILY_ROOT = "analytics/ggg/poe2/daily_v1"
MANIFEST_ROOT = "analytics/ggg/poe2/manifests/daily_v1"

def daily_paths(day):
    prefix = f"{ROOT}/year={day:%Y}/month={day:%m}/day={day:%d}/"
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)

    sources = [
        f"{prefix}{int((start + timedelta(hours=h)).timestamp())}.parquet"
        for h in range(24)
    ]
    target = (
        f"{DAILY_ROOT}/year={day:%Y}/month={day:%m}/"
        f"{day:%Y-%m-%d}.parquet"
    )
    manifest = (
        f"{MANIFEST_ROOT}/year={day:%Y}/month={day:%m}/"
        f"{day:%Y-%m-%d}.json"
    )
    return sources, target, manifest


def table_fingerprint(table):
    """Fingerprint all rows and columns in a deterministic order."""
    digest = hashlib.sha256()

    for batch in table.to_batches(max_chunksize=4096):
        for row in batch.to_pylist():
            encoded = json.dumps(
                row,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
                allow_nan=False,
            ).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)

    return digest.hexdigest()



def compact_day(container, day_string):
    day = date.fromisoformat(day_string)
    if day.isoformat() != day_string:
        raise ValueError("Date must be YYYY-MM-DD")

    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    if start + timedelta(days=1) > datetime.now(timezone.utc):
        raise ValueError("Cannot compact an incomplete UTC day")

    sources, target, manifest = daily_paths(day)

    if container.get_blob_client(manifest).exists():
        if container.get_blob_client(target).exists():
            return {
                "status": "already_compacted",
                "date": day_string,
                "parquet_blob": target,
            }
        raise RuntimeError("Manifest exists but daily Parquet is missing")

    available = {
        blob.name
        for blob in container.list_blobs(
            name_starts_with=(
                f"{ROOT}/year={day:%Y}/month={day:%m}/day={day:%d}/"
            )
        )
    }

    missing = [name for name in sources if name not in available]
    if missing:
        raise ValueError(
            f"Day {day_string} is missing {len(missing)} hourly files"
        )

    tables = []
    source_counts = []

    for hour, name in enumerate(sources):
        raw = container.download_blob(name).readall()
        table = pq.read_table(io.BytesIO(raw), schema=SCHEMA)

        expected_id = int(
            (start + timedelta(hours=hour)).timestamp()
        )

        ids = table.column("change_id").to_pylist()
        if not ids or any(value != expected_id for value in ids):
            raise ValueError(f"Invalid hourly timestamp: {name}")

        tables.append(table)
        source_counts.append({
            "blob": name,
            "change_id": expected_id,
            "rows": table.num_rows,
        })

    combined = pa.concat_tables(tables)

    output = io.BytesIO()
    pq.write_table(combined, output, compression="zstd")

    container.upload_blob(target, output.getvalue(), overwrite=True)

    verified = pq.read_table(
        io.BytesIO(container.download_blob(target).readall()),
        schema=SCHEMA,
    )

    if verified.num_rows != combined.num_rows:
        raise RuntimeError("Output row count verification failed")

    expected_ids = [
        item["change_id"] for item in source_counts
    ]
    actual_ids = verified.column("change_id").to_pylist()

    if Counter(actual_ids) != Counter(
        combined.column("change_id").to_pylist()
    ):
        raise RuntimeError("Output timestamp verification failed")

    if set(actual_ids) != set(expected_ids):
        raise RuntimeError("Output is missing expected hourly timestamps")
    
    input_hash = table_fingerprint(combined)
    output_hash = table_fingerprint(verified)

    if input_hash != output_hash:
        raise RuntimeError(
            "Daily Parquet content verification failed"
        )


    record = {
        "status": "verified",
        "date": day_string,
        "hours": 24,
        "rows": verified.num_rows,
        "sources": source_counts,
        "output": target,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "cleanup_status": "disabled",
        "input_sha256": input_hash,
        "output_sha256": output_hash,
        "verification_version": 2,
    }

    container.upload_blob(
        manifest,
        json.dumps(record, indent=2).encode("utf-8"),
        overwrite=True,
    )

    return {
        "status": "success",
        "date": day_string,
        "hours": 24,
        "rows": verified.num_rows,
        "parquet_blob": target,
        "manifest_blob": manifest,
    }


def compact_pending_days(container, max_days=5):
    """Compact up to max_days complete, unprocessed UTC days."""
    prefix = f"{ROOT}/"
    dates = set()

    for blob in container.list_blobs(name_starts_with=prefix):
        parts = blob.name.split("/")
        try:
            year = int(next(p[5:] for p in parts if p.startswith("year=")))
            month = int(next(p[6:] for p in parts if p.startswith("month=")))
            day = int(next(p[4:] for p in parts if p.startswith("day=")))
            dates.add(date(year, month, day))
        except (ValueError, StopIteration):
            continue

    results = []
    for day in sorted(dates):
        if len(results) >= max_days:
            break

        sources, target, manifest = daily_paths(day)
        if container.get_blob_client(manifest).exists():
            continue

        available = {
            blob.name
            for blob in container.list_blobs(
                name_starts_with=(
                    f"{ROOT}/year={day:%Y}/month={day:%m}/day={day:%d}/"
                )
            )
        }

        if not all(source in available for source in sources):
            continue

        if datetime.now(timezone.utc).date() <= day:
            continue

        results.append(compact_day(container, day.isoformat()))

    return results


def preview_compacted_day_cleanup(container, day_string):
    """
    Verify a compacted day and preview hourly Parquet cleanup.

    Read-only: never deletes or modifies blobs or manifests.
    """
    day = date.fromisoformat(day_string)
    if day.isoformat() != day_string:
        raise ValueError("Date must be YYYY-MM-DD")

    sources, target, manifest_name = daily_paths(day)

    manifest_blob = container.get_blob_client(manifest_name)
    if not manifest_blob.exists():
        raise ValueError("Daily compaction manifest is missing")

    record = json.loads(manifest_blob.download_blob().readall())

    if record.get("verification_version") != 2:
        raise ValueError(
            "Manifest requires version 2 verification"
        )

    if record.get("status") != "verified":
        raise ValueError("Daily manifest is not verified")

    if record.get("date") != day_string:
        raise ValueError("Manifest date does not match request")

    if record.get("output") != target:
        raise ValueError("Manifest output path does not match")

    if record.get("hours") != 24:
        raise ValueError("Manifest does not cover 24 hours")

    recorded_sources = record.get("sources", [])
    if not isinstance(recorded_sources, list):
        raise ValueError("Invalid manifest sources")

    if [s.get("blob") for s in recorded_sources] != sources:
        raise ValueError("Manifest source paths do not match")

    verified_at = datetime.fromisoformat(
        record["verified_at"].replace("Z", "+00:00")
    )
    if verified_at.tzinfo is None:
        raise ValueError("Manifest verification time lacks timezone")

    elapsed = datetime.now(timezone.utc) - verified_at

    if elapsed < timedelta(hours=24):
        raise ValueError(
            "Must wait 24 hours after verification"
        )

    output_blob = container.get_blob_client(target)
    if not output_blob.exists():
        raise ValueError("Daily Parquet output is missing")

    output_table = pq.read_table(
        io.BytesIO(output_blob.download_blob().readall()),
        schema=SCHEMA,
    )

    output_hash = table_fingerprint(output_table)

    if not record.get("input_sha256"):
        raise ValueError("Manifest input fingerprint is missing")

    if output_hash != record.get("output_sha256"):
        raise ValueError("Daily output fingerprint mismatch")

    if output_hash != record["input_sha256"]:
        raise ValueError("Input and output fingerprints differ")

    if output_table.num_rows != record.get("rows"):
        raise ValueError("Daily output row count mismatch")

    hourly_tables = []
    candidates = []

    start = datetime(day.year, day.month, day.day,
                     tzinfo=timezone.utc)

    for hour, name in enumerate(sources):
        blob = container.get_blob_client(name)

        if not blob.exists():
            raise ValueError(f"Hourly source missing: {name}")

        properties = blob.get_blob_properties()
        raw = blob.download_blob().readall()
        table = pq.read_table(io.BytesIO(raw), schema=SCHEMA)

        expected_id = int(
            (start + timedelta(hours=hour)).timestamp()
        )

        if table.num_rows == 0:
            raise ValueError(f"Empty hourly source: {name}")

        if any(
            value != expected_id
            for value in table.column("change_id").to_pylist()
        ):
            raise ValueError(f"Invalid source timestamp: {name}")

        if recorded_sources[hour].get("change_id") != expected_id:
            raise ValueError("Manifest change ID mismatch")

        if recorded_sources[hour].get("rows") != table.num_rows:
            raise ValueError("Manifest source row count mismatch")

        hourly_tables.append(table)

        candidates.append({
            "blob": name,
            "size_bytes": properties.size,
            "change_id": expected_id,
            "rows": table.num_rows,
        })

    combined = pa.concat_tables(hourly_tables)
    combined_hash = table_fingerprint(combined)

    if combined_hash != output_hash:
        raise ValueError(
            "Hourly sources do not match daily output"
        )

    total_bytes = sum(
        item["size_bytes"] for item in candidates
    )

    return {
        "status": "eligible",
        "mode": "dry_run",
        "date": day_string,
        "verification_version": 2,
        "hours": 24,
        "rows": output_table.num_rows,
        "sha256": output_hash,
        "daily_blob": target,
        "manifest_blob": manifest_name,
        "files_to_delete": candidates,
        "file_count": len(candidates),
        "total_bytes": total_bytes,
        "total_megabytes": round(
            total_bytes / (1024 * 1024), 2
        ),
        "deletion_performed": False,
    }
