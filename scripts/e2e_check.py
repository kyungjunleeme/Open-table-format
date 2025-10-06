import os
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from open_table_format.iceber_ops import (
    reset_demo_state,
    gen_parquet_ns,
    rewrite_ns_to_us,
    upload_local_to_s3,
    append_from_parquet,
    add_files_register,
    inspect_table,
    write_manual_rows,
    s3_object_exists,
)


def main() -> None:
    wh = os.getenv("WAREHOUSE", "file:///app/warehouse")
    dp = os.getenv("DATAPATH", "s3://iceberg/data")
    print("ENV WAREHOUSE=", wh)
    print("ENV DATAPATH =", dp)

    # Reset state
    local_targets = [
        Path("data/step1_events_ns.parquet"),
        Path("data/step1_events_us.parquet"),
        Path("data/step3_append_us.parquet"),
        Path("data/step4_register_ns.parquet"),
        Path("data/custom_ns.parquet"),
        Path("data/custom_us.parquet"),
        Path("data/rewrite_us.parquet"),
    ]
    s3_targets = [
        f"{dp}/step2_events_ns.parquet",
        f"{dp}/step3_append_ns.parquet",
        f"{dp}/e2e_step3_append_ns.parquet",
    ]
    print("RESET:", reset_demo_state(wh, local_targets, s3_targets))

    # Step 1: Editable data → Arrow ns/us and save
    rows = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 12:34:56.123456789Z",
                    "2024-01-01 12:34:56.123456790Z",
                    "2024-01-01 12:34:56.123456791Z",
                ],
                utc=True,
            ),
        }
    )
    arr_ns = pa.Table.from_pandas(rows, preserve_index=False)
    opts_us = pa.compute.CastOptions(
        target_type=pa.timestamp("us", tz="UTC"), allow_time_truncate=True
    )
    ts_idx = arr_ns.schema.get_field_index("timestamp")
    arr_us = arr_ns.set_column(
        ts_idx, "timestamp", pa.compute.cast(arr_ns["timestamp"], options=opts_us)
    )
    Path("data").mkdir(parents=True, exist_ok=True)
    custom_ns = Path("data/custom_ns.parquet")
    custom_us = Path("data/custom_us.parquet")
    pq.write_table(arr_ns, custom_ns)
    pq.write_table(arr_us, custom_us)
    print("STEP1 saved:", custom_ns, custom_us)

    # Also create ts_ns source via helper and rewrite to us
    step1_ns = gen_parquet_ns(Path("data/step1_events_ns.parquet"))
    rewrite_out = Path("data/rewrite_us.parquet")
    rewrite_ns_to_us(step1_ns, rewrite_out)
    print("STEP1 rewrite_out (ts_ns->us):", rewrite_out)

    # Step 2: Upload to S3 (ns source)
    s2_uri = f"{dp}/step2_events_ns.parquet"
    upload_local_to_s3(step1_ns, s2_uri)
    print("STEP2 uploaded exists:", s3_object_exists(s2_uri))

    # Step 3: Append from S3 (ns source; writer path handles cast)
    s3_uri = f"{dp}/e2e_step3_append_ns.parquet"
    upload_local_to_s3(step1_ns, s3_uri)
    append_snap = append_from_parquet(wh, s3_uri)
    print("STEP3 append snapshot:", append_snap)

    # Step 3: add_files with local path — must match Iceberg schema exactly
    # Prepare a local file with id:int32 (not null), ts:timestamp[us] (naive) and correct names
    reg_local = Path("data/step4_register_ns.parquet")
    t = pq.read_table(rewrite_out)
    # rename ts_ns -> ts
    names = ["id", "ts"] if "ts_ns" in t.schema.names else t.schema.names
    t = t.rename_columns(names)
    id_idx = t.schema.get_field_index("id")
    ts_idx = t.schema.get_field_index("ts")
    cast_id = pa.compute.cast(t["id"], target_type=pa.int32())
    cast_ts = pa.compute.cast(
        t["ts"], options=pa.compute.CastOptions(target_type=pa.timestamp("us"), allow_time_truncate=True)
    )
    t = t.set_column(id_idx, "id", cast_id)
    t = t.set_column(ts_idx, "ts", cast_ts)
    target_schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("ts", pa.timestamp("us"), nullable=True),
    ])
    t = t.cast(target_schema)
    pq.write_table(t, reg_local)
    reg_snap = add_files_register(wh, str(reg_local))
    print("STEP3 add_files snapshot:", reg_snap)

    # Step 4: Inspect
    info = inspect_table(wh)
    print("STEP4 inspect keys:", list(info.keys()))
    print("STEP4 snapshots count:", len(info.get("snapshots", [])))

    # Step 5: Manual rows
    manual_rows = [
        {"id": 1, "category": "electronics", "amount": 299.99},
        {"id": 2, "category": "clothing", "amount": 79.99},
        {"id": 3, "category": "groceries", "amount": 45.50},
    ]
    msnap = write_manual_rows(wh, "db.manual", manual_rows)
    print("STEP5 manual snapshot:", msnap)
    print("OK E2E DONE")


if __name__ == "__main__":
    main()

