
"""Compact a complete UTC day of GGG hourly Parquet snapshots."""

import io
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from ggg_parquet import ROOT, SCHEMA

DAILY_ROOT = "analytics/ggg/poe2/daily_v1"


def compact_day(container, day_string):
    day = date.fromisoformat(day_string)
    start = datetime(
        day.year, day.month, day.day, tzinfo=timezone.utc
    )

    if start + timedelta(days=1) > datetime.now(timezone.utc):
        raise ValueError("Cannot compact an incomplete UTC day")

    prefix = (
        f"{ROOT}/year={day:%Y}/month={day:%m}/day={day:%d}/"
    )

    expected = [
        f"{prefix}{int((start + timedelta(hours=h)).timestamp())}.parquet"
        for h in range(24)
    ]

    available = {
        blob.name
        for blob in container.list_blobs(name_starts_with=prefix)
    }

    missing = [name for name in expected if name not in available]

    if missing:
        raise ValueError(
            f"Day {day_string} is missing {len(missing)} hourly files"
        )

    tables = []
    for name in expected:
        data = container.download_blob(name).readall()
        table = pq.read_table(io.BytesIO(data), schema=SCHEMA)
        tables.append(table)

    combined = pa.concat_tables(tables)

    target = (
        f"{DAILY_ROOT}/year={day:%Y}/month={day:%m}/"
        f"{day:%Y-%m-%d}.parquet"
    )

    output = io.BytesIO()
    pq.write_table(combined, output, compression="zstd")

    container.upload_blob(
        target,
        output.getvalue(),
        overwrite=True,
    )

    return {
        "status": "success",
        "date": day_string,
        "hours": 24,
        "rows": combined.num_rows,
        "parquet_blob": target,
    }
