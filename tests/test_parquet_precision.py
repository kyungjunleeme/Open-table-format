import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

def test_ns_to_us_cast(tmp_path):
    df = pd.DataFrame({
        "id": [1,2,3],
        "ts_ns": pd.to_datetime([
            "2024-01-01 12:34:56.123456789",
            "2024-01-01 12:34:56.123456790",
            "2024-01-01 12:34:56.123456791",
        ], utc=True)
    })
    t = pa.Table.from_pandas(df, preserve_index=False)
    src = tmp_path / "ns.parquet"
    pq.write_table(t, src)
    t2 = t.set_column(
        t.schema.get_field_index("ts_ns"), "ts_ns",
        pa.compute.cast(t["ts_ns"], pa.timestamp("us"))
    )
    dst = tmp_path / "us.parquet"
    pq.write_table(t2, dst)
    r = pq.read_table(dst)
    assert str(r.schema.field("ts_ns").type).startswith("timestamp[us]")
