import argparse
import os
from pathlib import Path

from open_table_format.iceber_ops import (
    reset_demo_state,
    gen_parquet_ns,
    edit_local_ns_file,
    upload_local_to_s3,
    s3_object_exists,
)


def _paths(datapath: str):
    local_ns = Path("data/step1_events_ns.parquet")
    local_us = Path("data/step1_events_us.parquet")
    l3_local_us = Path("data/step3_append_us.parquet")
    l4_local_ns = Path("data/step4_register_ns.parquet")

    s2_s3_ns = f"{datapath}/step2_events_ns.parquet"
    s3_s3_ns = f"{datapath}/step3_append_ns.parquet"
    s4_s3_ns = f"{datapath}/step4_register_ns.parquet"
    return {
        "LOCAL_NS": local_ns,
        "LOCAL_US": local_us,
        "L3_LOCAL_US": l3_local_us,
        "L4_LOCAL_NS": l4_local_ns,
        "S2_S3_NS": s2_s3_ns,
        "S3_S3_NS": s3_s3_ns,
        "S4_S3_NS": s4_s3_ns,
    }


def cmd_reset(args):
    wh = os.getenv("WAREHOUSE", "file:///app/warehouse")
    dp = os.getenv("DATAPATH", "s3://iceberg/data")
    p = _paths(dp)
    res = reset_demo_state(wh, [p["LOCAL_NS"], p["LOCAL_US"], p["L3_LOCAL_US"], p["L4_LOCAL_NS"]], [p["S2_S3_NS"], p["S3_S3_NS"]])
    print(res)


def cmd_make_data(args):
    dp = os.getenv("DATAPATH", "s3://iceberg/data")
    p = _paths(dp)
    out = gen_parquet_ns(p["LOCAL_NS"])
    print("generated:", out)


def cmd_edit_data(args):
    dp = os.getenv("DATAPATH", "s3://iceberg/data")
    p = _paths(dp)
    out = edit_local_ns_file(p["LOCAL_NS"], add_rows=args.rows)
    print("edited:", out)


def cmd_upload_step2(args):
    dp = os.getenv("DATAPATH", "s3://iceberg/data")
    p = _paths(dp)
    upload_local_to_s3(p["LOCAL_NS"], p["S2_S3_NS"])
    print("uploaded:", p["S2_S3_NS"], "exists=", s3_object_exists(p["S2_S3_NS"]))


def main():
    ap = argparse.ArgumentParser(description="Developer utilities for the demo")
    sp = ap.add_subparsers(dest="cmd", required=True)

    sp_reset = sp.add_parser("reset", help="Drop table and delete demo files (local + S3)")
    sp_reset.set_defaults(func=cmd_reset)

    sp_make = sp.add_parser("make-data", help="Generate Step 1 ns Parquet locally")
    sp_make.set_defaults(func=cmd_make_data)

    sp_edit = sp.add_parser("edit-data", help="Append rows to the Step 1 ns file")
    sp_edit.add_argument("--rows", type=int, default=1)
    sp_edit.set_defaults(func=cmd_edit_data)

    sp_up2 = sp.add_parser("upload-step2", help="Upload Step 1 ns → Step 2 S3 path")
    sp_up2.set_defaults(func=cmd_upload_step2)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
