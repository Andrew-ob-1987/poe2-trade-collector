cat > /tmp/verify_ggg_day.py <<'PY'
import hashlib
import io
import json
import sys
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient

ACCOUNT = "poe2tradecollectorf8fb3"
CONTAINER = "poe2-prices"

def fingerprint(table):
    digest = hashlib.sha256()
    for batch in table.to_batches(max_chunksize=4096):
        for row in batch.to_pylist():
            encoded = json.dumps(
                row, sort_keys=True, separators=(",", ":"),
                default=str, allow_nan=False
            ).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
    return digest.hexdigest()

def verify(day_string):
    day = date.fromisoformat(day_string)
    if day.isoformat() != day_string:
        raise ValueError("Expected YYYY-MM-DD")

    client = BlobServiceClient(
        account_url=f"https://{ACCOUNT}.blob.core.windows.net",
        credential=AzureCliCredential()
    ).get_container_client(CONTAINER)

    prefix = (
        f"analytics/ggg/poe2/staging_v1/"
        f"year={day:%Y}/month={day:%m}/day={day:%d}"
    )
    daily = (
        f"analytics/ggg/poe2/daily_v1/"
        f"year={day:%Y}/month={day:%m}/{day}.parquet"
    )
    manifest = (
        f"analytics/ggg/poe2/manifests/daily_v1/"
        f"year={day:%Y}/month={day:%m}/{day}.json"
    )

    record = json.loads(client.download_blob(manifest).readall())
    assert record["date"] == day_string, "Manifest date mismatch"
    assert record["status"] == "verified", "Manifest not verified"
    assert record["hours"] == 24, "Manifest hours mismatch"
    assert record["output"] == daily, "Manifest output mismatch"

    recorded_sources = record.get("sources")
    assert isinstance(recorded_sources, list), "Invalid sources"
    assert len(recorded_sources) == 24, "Expected 24 source records"

    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    tables = []
    schema = None
    source_rows = 0

    for hour in range(24):
        change_id = int((start + timedelta(hours=hour)).timestamp())
        path = f"{prefix}/{change_id}.parquet"
        source = recorded_sources[hour]

        assert source["blob"] == path, f"Source path mismatch: {hour}"
        assert source["change_id"] == change_id, (
            f"Source ID mismatch: {hour}"
        )

        raw = client.download_blob(path).readall()
        table = pq.read_table(io.BytesIO(raw))

        if schema is None:
            schema = table.schema
        assert table.schema.equals(schema), (
            f"Schema mismatch: {hour}"
        )
        assert table.num_rows > 0, f"Empty hourly file: {hour}"
        assert source["rows"] == table.num_rows, (
            f"Source row count mismatch: {hour}"
        )
        assert all(
            value == change_id
            for value in table.column("change_id").to_pylist()
        ), f"Timestamp mismatch: {hour}"

        source_rows += table.num_rows
        tables.append(table)
        print(f"Hour {hour:02d}: {table.num_rows:,} rows", flush=True)

    combined = pa.concat_tables(tables)
    daily_table = pq.read_table(
        io.BytesIO(client.download_blob(daily).readall())
    )

    assert daily_table.schema.equals(schema), "Daily schema mismatch"
    assert combined.num_rows == source_rows, "Source total mismatch"
    assert daily_table.num_rows == source_rows, "Daily row count mismatch"
    assert record["rows"] == source_rows, "Manifest row count mismatch"

    source_hash = fingerprint(combined)
    daily_hash = fingerprint(daily_table)

    assert source_hash == daily_hash, "SHA-256 content mismatch"

    if record.get("verification_version") == 2:
        assert record.get("input_sha256") == source_hash, (
            "Manifest input fingerprint mismatch"
        )
        assert record.get("output_sha256") == daily_hash, (
            "Manifest output fingerprint mismatch"
        )

    print("\n=== VERIFICATION RESULT ===")
    print("Date:", day_string)
    print("Original manifest version:",
          record.get("verification_version", 1))
    print("Hourly files:", len(tables))
    print("Rows:", f"{source_rows:,}")
    print("Source SHA-256:", source_hash)
    print("Daily SHA-256: ", daily_hash)
    print("Result: PASS")
    print("Read-only: no blobs modified or deleted.")

if __name__ == "__main__":
    try:
        verify(sys.argv[1] if len(sys.argv) > 1 else "2025-01-01")
    except Exception as exc:
        print(f"\nRESULT: FAIL — {type(exc).__name__}: {exc}")
        sys.exit(1)
PY

python /tmp/verify_ggg_day.py 2025-01-01