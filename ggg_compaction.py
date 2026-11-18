
"""Verified daily compaction for GGG currency snapshots."""

import io
import json
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from ggg_parquet import ROOT, SCHEMA

import hashlib
from collections import Counter


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

    from collections import Counter

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
