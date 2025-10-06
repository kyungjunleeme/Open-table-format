from pathlib import Path
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    BooleanType,
    TimestampType,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.manifest import DataFile
from pyiceberg.exceptions import NoSuchNamespaceError
from urllib.parse import urlparse
from typing import Any, Dict, List

DEFAULT_WAREHOUSE = os.getenv("WAREHOUSE", "s3://iceberg/warehouse")
DEFAULT_DATAPATH  = os.getenv("DATAPATH",  "s3://iceberg/data")
TABLE_NAME = "db.events"

def _catalog(warehouse_uri: str):
    # Use a SQL-backed catalog (SQLite) for metadata, and S3 for the warehouse location.
    # This works with pyiceberg>=0.10 where file catalogs are not inferred from s3:// URIs.
    catalog_uri = os.getenv("CATALOG_URI", "sqlite:///catalog.db")
    return load_catalog("dev", uri=catalog_uri, warehouse=warehouse_uri)

def ensure_table(warehouse_uri: str = DEFAULT_WAREHOUSE, table_name: str = TABLE_NAME):
    cat = _catalog(warehouse_uri)
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "ts", TimestampType(), required=False),
    )
    try:
        return cat.load_table(table_name)
    except Exception:
        # Ensure namespace exists
        if "." in table_name:
            ns = tuple(table_name.split(".")[:-1])
            try:
                cat.create_namespace(ns)
            except Exception:
                pass
        return cat.create_table(
            identifier=table_name,
            schema=schema,
            sort_order=UNSORTED_SORT_ORDER,
            properties={"format-version": "2"},
        )

def gen_parquet_ns(out_path: Path = Path("data/events_ns.parquet")) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "id": [1,2,3],
        "ts_ns": pd.to_datetime([
            "2024-01-01 12:34:56.123456789",
            "2024-01-01 12:34:56.123456790",
            "2024-01-01 12:34:56.123456791",
        ], utc=True)
    })
    t = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(t, out_path)
    return out_path

def rewrite_ns_to_us(src: Path, dst: Path) -> Path:
    t = pq.read_table(src)
    if "ts_ns" not in t.schema.names:
        raise ValueError("expected column 'ts_ns'")
    # Allow truncation from ns -> us and preserve timezone
    opts = pa.compute.CastOptions(
        target_type=pa.timestamp("us", tz="UTC"),
        allow_time_truncate=True,
    )
    t2 = t.set_column(
        t.schema.get_field_index("ts_ns"),
        "ts_ns",
        pa.compute.cast(t["ts_ns"], options=opts),
    )
    pq.write_table(t2, dst)
    return dst

def upload_local_to_s3(local: Path, s3_uri: str):
    import boto3
    p = urlparse(s3_uri)
    if p.scheme != "s3":
        raise ValueError("dst must be s3://...")
    bucket, key = p.netloc, p.path.lstrip("/")
    s3 = boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"), region_name=os.getenv("AWS_REGION"))
    s3.upload_file(str(local), bucket, key)
    return s3_uri

def _s3_filesystem():
    """Create a configured S3 filesystem for PyArrow that points to MinIO.

    Uses common env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL, AWS_REGION.
    """
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION", "us-east-1")
    scheme = "http" if endpoint and endpoint.startswith("http://") else "https"
    return pa.fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=endpoint,
        region=region,
        scheme=scheme,
    )

def _is_s3_uri(uri: str) -> bool:
    return urlparse(uri).scheme == "s3"

def append_from_parquet(warehouse: str, parquet_uri: str):
    os.environ["PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE"] = "True"
    tbl = ensure_table(warehouse)
    if _is_s3_uri(parquet_uri):
        fs = _s3_filesystem()
        p = urlparse(parquet_uri)
        s3_path = f"{p.netloc}/{p.path.lstrip('/')}"
        table = pq.read_table(s3_path, filesystem=fs)
    else:
        table = pq.read_table(parquet_uri)
    table = table.rename_columns(["id", "ts"])  # id: int64 -> int32, ts: ns tz-> us (naive)
    # Cast id to Iceberg int (32-bit) and timestamps to microseconds without tz
    id_idx = table.schema.get_field_index("id")
    ts_idx = table.schema.get_field_index("ts")
    id_opts = pa.compute.CastOptions(target_type=pa.int32())
    ts_opts = pa.compute.CastOptions(target_type=pa.timestamp("us"), allow_time_truncate=True)
    table = table.set_column(id_idx, "id", pa.compute.cast(table["id"], options=id_opts))
    table = table.set_column(ts_idx, "ts", pa.compute.cast(table["ts"], options=ts_opts))
    # Enforce non-nullability for 'id' to match Iceberg required field
    target_schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("ts", pa.timestamp("us"), nullable=True),
    ])
    table = table.cast(target_schema)
    tbl.append(table)
    return tbl.current_snapshot().snapshot_id

def add_files_register(warehouse: str, parquet_uri: str):
    tbl = ensure_table(warehouse)
    # Normalize path: use file:// for local paths to avoid S3 resolution
    if _is_s3_uri(parquet_uri):
        path = parquet_uri
    else:
        path = Path(parquet_uri).resolve().as_uri()
    try:
        tbl.add_files([path])
    except Exception as e:
        # Gracefully handle re-registering the same file
        if "already referenced" in str(e):
            snap = tbl.current_snapshot()
            return snap.snapshot_id if snap else None
        raise
    return tbl.current_snapshot().snapshot_id

def s3_object_exists(s3_uri: str) -> bool:
    import boto3
    p = urlparse(s3_uri)
    if p.scheme != "s3":
        return False
    bucket, key = p.netloc, p.path.lstrip("/")
    s3 = boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"), region_name=os.getenv("AWS_REGION"))
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def s3_delete_object(s3_uri: str) -> bool:
    import boto3
    p = urlparse(s3_uri)
    if p.scheme != "s3":
        return False
    bucket, key = p.netloc, p.path.lstrip("/")
    s3 = boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"), region_name=os.getenv("AWS_REGION"))
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def drop_table_if_exists(warehouse_uri: str = DEFAULT_WAREHOUSE, table_name: str = TABLE_NAME) -> bool:
    try:
        cat = _catalog(warehouse_uri)
        if cat.table_exists(table_name):
            cat.drop_table(table_name)
            return True
        return False
    except Exception:
        return False

def reset_demo_state(warehouse_uri: str, local_paths: list[Path], s3_uris: list[str]) -> dict:
    """Drop table and delete local/S3 demo files. Returns a summary dict."""
    summary = {"dropped_table": False, "deleted_local": [], "deleted_s3": []}
    summary["dropped_table"] = drop_table_if_exists(warehouse_uri, TABLE_NAME)
    for lp in local_paths:
        try:
            if lp.exists():
                lp.unlink()
                summary["deleted_local"].append(str(lp))
        except Exception:
            pass
    for su in s3_uris:
        if s3_delete_object(su):
            summary["deleted_s3"].append(su)
    return summary

def edit_local_ns_file(path: Path, add_rows: int = 1) -> Path:
    """Append a few rows to the local ns parquet file to simulate edits."""
    import pandas as pd
    if not path.exists():
        gen_parquet_ns(path)
    t = pq.read_table(path)
    df = t.to_pandas()
    last_id = int(df["id"].max()) if not df.empty else 0
    base_ts = df["ts_ns"].max() if "ts_ns" in df.columns and not df.empty else pd.Timestamp("2024-01-01 00:00:00Z")
    new_ids = list(range(last_id + 1, last_id + 1 + add_rows))
    new_ts = [pd.to_datetime(base_ts) + pd.to_timedelta(i, unit="ns") for i in range(1, add_rows + 1)]
    df2 = pd.DataFrame({"id": new_ids, "ts_ns": pd.to_datetime(new_ts, utc=True)})
    out = pd.concat([df, df2], ignore_index=True)
    pq.write_table(pa.Table.from_pandas(out, preserve_index=False), path)
    return path

# ========= Manual rows (UI helper) =========
def _pa_type_to_iceberg(dt: pa.DataType):
    if pa.types.is_int8(dt) or pa.types.is_int16(dt) or pa.types.is_int32(dt):
        return IntegerType()
    if pa.types.is_int64(dt):
        return LongType()
    if pa.types.is_float32(dt):
        return FloatType()
    if pa.types.is_float64(dt):
        return DoubleType()
    if pa.types.is_string(dt):
        return StringType()
    if pa.types.is_boolean(dt):
        return BooleanType()
    if pa.types.is_timestamp(dt):
        return TimestampType()
    raise ValueError(f"Unsupported Arrow type for dynamic schema: {dt}")

def _ensure_namespace(cat, table_name: str):
    if "." in table_name:
        ns = tuple(table_name.split(".")[:-1])
        try:
            cat.create_namespace(ns)
        except Exception:
            pass

def write_manual_rows(warehouse_uri: str, table_name: str, rows: list[dict]) -> int:
    """Recreate the table with a schema derived from rows and append them.

    Returns the new snapshot id.
    """
    cat = _catalog(warehouse_uri)
    _ensure_namespace(cat, table_name)
    t = pa.Table.from_pylist(rows)
    # Build Iceberg schema
    nested = []
    fid = 1
    for f in t.schema:
        it = _pa_type_to_iceberg(f.type)
        nested.append(NestedField(fid, f.name, it, required=False))
        fid += 1
    schema = Schema(*nested)
    # Recreate table to match edited schema
    try:
        if cat.table_exists(table_name):
            cat.drop_table(table_name)
    except Exception:
        pass
    tbl = cat.create_table(
        identifier=table_name,
        schema=schema,
        sort_order=UNSORTED_SORT_ORDER,
        properties={"format-version": "2"},
    )
    tbl.append(t)
    return tbl.current_snapshot().snapshot_id

def preview_table_rows(warehouse_uri: str, table_name: str, limit: int = 100) -> Dict[str, Any]:
    """Return a small preview of table rows and schema for display in UI.

    Tries pandas first; falls back to Arrow batches if needed.
    """
    cat = _catalog(warehouse_uri)
    tbl = cat.load_table(table_name)
    result: Dict[str, Any] = {"schema": str(tbl.schema()), "rows": [], "count": 0}
    try:
        scan = tbl.scan(limit=limit)
        try:
            # Preferred: convert directly to pandas
            df = scan.to_pandas()
            result["rows"] = df.head(limit).to_dict(orient="records")
            result["count"] = len(df)
            return result
        except Exception:
            pass
        try:
            # Fallback: Arrow batches
            batches = list(scan.to_arrow())
            if batches:
                import pyarrow as pa
                tb = pa.Table.from_batches(batches)
                import pandas as pd
                df = tb.to_pandas()
                result["rows"] = df.head(limit).to_dict(orient="records")
                result["count"] = len(df)
        except Exception:
            # Ignore and let outer handler capture any errors
            pass
    except Exception as e:
        result["error"] = str(e)
    return result
def inspect_table(warehouse: str):
    tbl = ensure_table(warehouse)
    md = tbl.metadata
    return {
        "format_version": md.format_version,
        "location": md.location,
        "current_snapshot_id": md.current_snapshot_id,
        "snapshots": [s.snapshot_id for s in md.snapshots] if md.snapshots else [],
        "schemas": str(md.schemas),
    }
