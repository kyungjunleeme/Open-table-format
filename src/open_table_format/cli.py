import argparse
from pathlib import Path

from open_table_format.iceber_ops import (
    DEFAULT_WAREHOUSE, DEFAULT_DATAPATH,
    rewrite_ns_to_us, append_from_parquet, add_files_register, inspect_table
)

def rewrite():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/events_ns.parquet")
    ap.add_argument("--dst", default="data/events_us.parquet")
    args = ap.parse_args()
    rewrite_ns_to_us(Path(args.src), Path(args.dst))
    print("Converted ns->us:", args.dst)

def append():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    ap.add_argument("--source", default=f"{DEFAULT_DATAPATH}/events_ns.parquet")
    args = ap.parse_args()
    sid = append_from_parquet(args.warehouse, args.source)
    print("append snapshot:", sid)

def add_files():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    ap.add_argument("--file", default=f"{DEFAULT_DATAPATH}/events_ns.parquet")
    args = ap.parse_args()
    sid = add_files_register(args.warehouse, args.file)
    print("add_files snapshot:", sid)

def inspect():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    args = ap.parse_args()
    print(inspect_table(args.warehouse))
