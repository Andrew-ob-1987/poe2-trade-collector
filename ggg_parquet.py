"""Convert a single GGG hourly currency digest to deterministic Parquet staging."""
import io
import json
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = 'analytics/ggg/poe2/staging_v1'
FIELDS = [
    ('change_id', pa.int64()),
    ('snapshot_time', pa.timestamp('us', tz='UTC')),
    ('league', pa.string()),
    ('market_id', pa.string()),
    ('item_a', pa.string()),
    ('item_b', pa.string()),
]
for field in ('volume', 'lowest_stock', 'highest_stock', 'lowest_ratio', 'highest_ratio'):
    FIELDS.extend([(f'{field}_a', pa.int64()), (f'{field}_b', pa.int64())])
SCHEMA = pa.schema(FIELDS)
SOURCE_KEYS = {
    'volume': 'volume_traded',
    'lowest_stock': 'lowest_stock',
    'highest_stock': 'highest_stock',
    'lowest_ratio': 'lowest_ratio',
    'highest_ratio': 'highest_ratio',
}


def convert_hour(container, change_id):
    """Read a saved JSON snapshot, write one Parquet file, return its blob name."""
    change_id = int(change_id)
    if change_id <= 0 or change_id % 3600:
        raise ValueError('change_id must be a positive hour-aligned timestamp')
    source = f'ggg/poe2/hourly/{change_id}.json'
    data = json.loads(container.download_blob(source).readall())
    if int(data['change_id']) != change_id:
        raise ValueError(f'Snapshot ID mismatch in {source}')
    markets = data['markets']
    if not isinstance(markets, list):
        raise ValueError('markets must be an array')
    snapshot = datetime.fromtimestamp(change_id, tz=timezone.utc)
    rows = []
    for market in markets:
        pair = market['market_pair']
        if len(pair) != 2:
            raise ValueError(f'Unexpected market_pair: {pair}')
        a, b = pair
        row = {
            'change_id': change_id,
            'snapshot_time': snapshot,
            'league': market.get('league'),
            'market_id': market.get('market_id'),
            'item_a': a,
            'item_b': b,
        }
        for target, source_key in SOURCE_KEYS.items():
            values = market.get(source_key) or {}
            row[f'{target}_a'] = values.get(a)
            row[f'{target}_b'] = values.get(b)
        rows.append(row)
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    output = io.BytesIO()
    pq.write_table(table, output, compression='zstd')
    target = (
        f'{ROOT}/year={snapshot:%Y}/month={snapshot:%m}/day={snapshot:%d}/'
        f'{change_id}.parquet'
    )
    container.upload_blob(target, output.getvalue(), overwrite=True)
    return target, table.num_rows
