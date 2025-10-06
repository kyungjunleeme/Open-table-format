import os
import shutil
import time
import json
import ast
from pathlib import Path
import streamlit as st
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from open_table_format.iceber_ops import (
    DEFAULT_WAREHOUSE,
    DEFAULT_DATAPATH,
    gen_parquet_ns,
    upload_local_to_s3,
    rewrite_ns_to_us,
    append_from_parquet,
    add_files_register,
    inspect_table,
    s3_object_exists,
    reset_demo_state,
    write_manual_rows,
)


st.set_page_config(page_title="Open Table Format • PyIceberg Demo", layout="wide")


def _init_state():
    for k in [
        "logs_gen",
        "logs_upload",
        "logs_rewrite",
        "logs_append",
        "logs_add_files",
        "logs_inspect",
    ]:
        st.session_state.setdefault(k, [])
    st.session_state.setdefault("dark_mode", False)


def _log(key: str, msg: str) -> None:
    st.session_state[key].append(msg)


def _flow_dot(datapath: str) -> str:
    return f"""
    digraph G {{
      rankdir=LR;
      node [shape=box, style=rounded];
      A [label="Generate Parquet (ns timestamps)"];
      B [label="Upload to MinIO (\"{datapath}/events_ns.parquet\")"];
      C [label="Rewrite ns→us (local optional)"];
      D [label="Append via writer path (downcast applies)"];
      E [label="Register via add_files (no downcast)"];
      F [label="Inspect Iceberg table metadata"];
      A -> B;
      A -> C;
      B -> D;
      B -> E;
      D -> F;
      E -> F;
    }}
    """


def run():
    _init_state()
    WAREHOUSE = os.getenv("WAREHOUSE", DEFAULT_WAREHOUSE)
    DATAPATH = os.getenv("DATAPATH", DEFAULT_DATAPATH)
    # Unique, visible names per step
    LOCAL_NS = Path("data/step1_events_ns.parquet")
    LOCAL_US = Path("data/step1_events_us.parquet")
    S2_S3_NS = f"{DATAPATH}/step2_events_ns.parquet"
    S3_S3_NS = f"{DATAPATH}/step3_append_ns.parquet"
    L3_LOCAL_US = Path("data/step3_append_us.parquet")
    L4_LOCAL_NS = Path("data/step4_register_ns.parquet")

    with st.sidebar:
        st.header("⚙️ Config")
        st.caption("Environment-driven settings used by the steps below.")
        st.code(f"WAREHOUSE={WAREHOUSE}\nDATAPATH={DATAPATH}", language="bash")
        st.caption("Demo file paths (unique per step):")
        st.code("\n".join([
            f"STEP1_LOCAL_NS={LOCAL_NS}",
            f"STEP1_LOCAL_US={LOCAL_US}",
            f"STEP2_S3_NS={S2_S3_NS}",
            f"STEP3_S3_NS={S3_S3_NS}",
            f"STEP3_LOCAL_US={L3_LOCAL_US}",
            f"STEP4_LOCAL_NS={L4_LOCAL_NS}",
        ]), language="bash")
        st.divider()
        st.subheader("Appearance")
        st.session_state.dark_mode = st.toggle("Dark mode", value=st.session_state.dark_mode, help="Apply a dark theme for the current session")
        if st.button("Clear logs", help="Reset all step logs to empty"):
            for key in [
                "logs_gen",
                "logs_upload",
                "logs_rewrite",
                "logs_append",
                "logs_add_files",
                "logs_inspect",
            ]:
                st.session_state[key] = []
            st.success("Cleared logs.")

    # Apply minimal CSS-based dark theme if toggled
    if st.session_state.dark_mode:
        st.markdown(
            """
            <style>
              .stApp { background-color: #0f172a; color: #e2e8f0; }
              .stMarkdown, .stCode, .stText, .stCaption, .stSubheader, .stHeader, .stDataFrame, .stJson { color: #e2e8f0 !important; }
              pre, code { background: #0b1220 !important; color: #e2e8f0 !important; }
              .stButton>button { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; }
              .stButton>button:hover { background: #0b1220; }
            </style>
            """,
            unsafe_allow_html=True,
        )

    st.title("🧊 Open Table Format: ns → us write paths")
    st.write(
        "This interactive demo reproduces timestamp downcast behavior when writing Parquet to an Iceberg table via PyIceberg."
    )

    # Flow diagram
    st.subheader("📈 Flow Overview")
    dot = _flow_dot(DATAPATH)
    try:
        st.graphviz_chart(dot, use_container_width=True)
    except Exception:
        st.caption("Graphviz not available; showing DOT source instead:")
        st.code(dot, language="dot")

    st.divider()

    # Section: Prepare data (editable grid + generate/rewrite)
    st.header("🧪 Step 1 — Prepare Sample Data")
    st.caption("Create or edit rows (id + timestamp), preview ns/us schemas, and save to your chosen path.")

    # Initialize editable data
    if "df_edit" not in st.session_state:
        st.session_state.df_edit = pd.DataFrame({
            "id": [1, 2, 3],
            "timestamp": pd.to_datetime([
                "2024-01-01 12:34:56.123456789Z",
                "2024-01-01 12:34:56.123456790Z",
                "2024-01-01 12:34:56.123456791Z",
            ], utc=True),
        })
    if "step1_output" not in st.session_state:
        st.session_state.step1_output = str(LOCAL_NS)

    # Editable grid
    st.subheader("Step 1A — Editable Data Grid")
    st.caption("Edit values freely; keep 'id' and 'timestamp' columns present.")
    st.session_state.df_edit = st.data_editor(
        st.session_state.df_edit,
        num_rows="dynamic",
        use_container_width=True,
        key="grid_step1",
    )

    # Helper to build Arrow table with target unit
    def _arrow_from_df(df: pd.DataFrame, unit: str) -> pa.Table:
        df2 = df.copy()
        # Ensure types
        df2["id"] = pd.to_numeric(df2["id"], errors="coerce").astype("Int64").fillna(0).astype(int)
        df2["timestamp"] = pd.to_datetime(df2["timestamp"], utc=True, errors="coerce")
        t = pa.Table.from_pandas(df2[["id", "timestamp"]], preserve_index=False)
        # Cast timestamp to requested unit with UTC tz
        opts = pa.compute.CastOptions(target_type=pa.timestamp(unit, tz="UTC"), allow_time_truncate=(unit=="us"))
        ts_idx = t.schema.get_field_index("timestamp")
        t2 = t.set_column(ts_idx, "timestamp", pa.compute.cast(t["timestamp"], options=opts))
        return t2

    # Timestamp column selection + per-column units
    if "ts_cols" not in st.session_state:
        st.session_state.ts_cols = [c for c in st.session_state.df_edit.columns if c.lower() in ("timestamp", "ts")] or ["timestamp"]
    if "ts_units" not in st.session_state:
        st.session_state.ts_units = {c: "ns" for c in st.session_state.ts_cols}

    st.subheader("Select timestamp columns & units")
    cols = list(st.session_state.df_edit.columns)
    st.session_state.ts_cols = st.multiselect(
        "Timestamp columns",
        options=cols,
        default=[c for c in st.session_state.ts_cols if c in cols],
        help="Choose one or more timestamp columns to cast when saving",
    )
    # Ensure units dict only for chosen columns
    st.session_state.ts_units = {c: st.session_state.ts_units.get(c, "ns") for c in st.session_state.ts_cols}

    # Per-column unit pickers
    if st.session_state.ts_cols:
        cc = st.columns(min(4, len(st.session_state.ts_cols)))
        for i, col in enumerate(st.session_state.ts_cols):
            with cc[i % len(cc)]:
                st.session_state.ts_units[col] = st.selectbox(
                    f"Unit for {col}", ["ns", "us"], index=0 if st.session_state.ts_units.get(col, "ns") == "ns" else 1
                )

    # Build Arrow table applying selected per-column units
    def _arrow_from_df_multi(df: pd.DataFrame, units_map: dict[str, str]) -> pa.Table:
        df2 = df.copy()
        if "id" in df2:
            df2["id"] = pd.to_numeric(df2["id"], errors="coerce").astype("Int64").fillna(0).astype(int)
        # Normalize timestamp-like columns
        for c in units_map:
            if c in df2:
                df2[c] = pd.to_datetime(df2[c], utc=True, errors="coerce")
        t = pa.Table.from_pandas(df2, preserve_index=False)
        for c, unit in units_map.items():
            if c in t.schema.names:
                idx = t.schema.get_field_index(c)
                opts = pa.compute.CastOptions(target_type=pa.timestamp(unit, tz="UTC"), allow_time_truncate=(unit == "us"))
                t = t.set_column(idx, c, pa.compute.cast(t[c], options=opts))
        return t

    out_path = st.text_input("Output Parquet path", value=st.session_state.step1_output, key="step1_out_path")
    c_pa, c_pb = st.columns(2)
    with c_pa:
        if st.button("Preview schema (selected units)", use_container_width=True):
            t_sel = _arrow_from_df_multi(st.session_state.df_edit, st.session_state.ts_units)
            st.session_state["schema_preview_selected"] = str(t_sel.schema)
        if st.session_state.get("schema_preview_selected"):
            st.code(st.session_state["schema_preview_selected"], language="text")
    with c_pb:
        if st.button("Save with selected units", use_container_width=True):
            t_sel = _arrow_from_df_multi(st.session_state.df_edit, st.session_state.ts_units)
            pq.write_table(t_sel, out_path)
            st.session_state.step1_output = out_path
            st.success(f"Saved: {out_path}")
            st.code(str(t_sel.schema), language="text")

    st.divider()
    st.subheader("Step 1B — Quick Generate/Rewrite")
    st.caption(f"Generate ns Parquet to a path; optionally rewrite ns→us to another path.")
    c1, c2 = st.columns(2)
    with c1:
        gen_path = st.text_input("Generate ns →", value=str(LOCAL_NS), key="gen_ns_path")
        if st.button("Generate ns Parquet", use_container_width=True):
            with st.spinner("Generating sample Parquet with ns timestamps..."):
                p = gen_parquet_ns(Path(gen_path))
                _log("logs_gen", f"Generated: {p}")
            st.success("Generated sample Parquet.")
        with st.expander("Logs / Output", expanded=False):
            st.code("\n".join(st.session_state["logs_gen"]))
    with c2:
        rw_path = st.text_input("Rewrite ns→us →", value=str(LOCAL_US), key="rw_us_path")
        if st.button("Rewrite ns → us (local)", use_container_width=True):
            with st.spinner("Rewriting ns → us locally (allows truncation)..."):
                src = Path(st.session_state.get("step1_output", str(LOCAL_NS)))
                out = rewrite_ns_to_us(src, Path(rw_path))
                _log("logs_rewrite", f"Rewritten file: {out}")
            st.success(f"Rewritten to microseconds: {rw_path}")
        with st.expander("Logs / Output", expanded=False):
            st.code("\n".join(st.session_state["logs_rewrite"]))

    st.divider()

    # Section: Upload
    st.header("☁️ Step 2 — Upload to MinIO (S3 API)")
    st.caption("Choose a local source path and S3 destination URI; then upload.")
    up_src = st.text_input("Local source path", value=st.session_state.get("step1_output", str(LOCAL_NS)), key="upload_src")
    up_dst = st.text_input("S3 destination URI", value=S2_S3_NS, key="upload_dst")
    if st.button("Step 2 — Upload", use_container_width=True):
        with st.spinner("Uploading to MinIO via boto3..."):
            upload_local_to_s3(Path(up_src), up_dst)
            exists = s3_object_exists(up_dst)
            _log("logs_upload", f"Uploaded: {up_dst}; exists={exists}")
        st.success(f"Uploaded to {up_dst}")
    with st.expander("Logs / Output", expanded=False):
        st.code("\n".join(st.session_state["logs_upload"]))

    st.divider()

    # Section: Write paths
    st.header("✍️ Step 3 — Write Paths")
    st.caption(f"Append test uses {S3_S3_NS} (or local {L3_LOCAL_US}).")
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Append via writer path (downcast applies)")
        st.caption(
            "Calls tbl.append(table). PyIceberg downcasts ns→us on write when enabled."
        )
        src_choice = st.radio(
            "Choose data source for append",
            ["S3 ns (recommended)", "Local us (fallback)"],
            horizontal=True,
            index=0,
            key="append_src",
        )
        if src_choice.startswith("S3"):
            append_src_uri = st.text_input("Append S3 URI", value=S3_S3_NS, key="append_s3_uri")
        else:
            append_src_uri = st.text_input("Append local path", value=str(L3_LOCAL_US), key="append_local_path")
        if st.button(f"Step 3 — Append now (create snapshot)", use_container_width=True):
            with st.spinner("Appending Parquet via writer path..."):
                try:
                    if src_choice.startswith("S3"):
                        # Ensure the S3 object exists by uploading the latest Step1 output
                        upload_local_to_s3(Path(st.session_state.get("step1_output", str(LOCAL_NS))), append_src_uri)
                        uri = append_src_uri
                    else:
                        # Ensure local us file exists by rewriting from latest Step1 output
                        rewrite_ns_to_us(Path(st.session_state.get("step1_output", str(LOCAL_NS))), Path(append_src_uri))
                        uri = append_src_uri
                    sid = append_from_parquet(WAREHOUSE, uri)
                    _log("logs_append", f"Append source={uri} snapshot={sid}")
                    st.success(f"Append committed. snapshot={sid}")
                except Exception as e:
                    _log("logs_append", f"Append failed: {e}")
                    st.error(f"Append failed: {e}")
        with st.expander("Logs / Output", expanded=False):
            st.code("\n".join(st.session_state["logs_append"]))

    with c4:
        st.subheader("Register via add_files (no downcast)")
        st.caption("Registers an existing file path into table metadata (no data rewrite).")
        st.caption("Hint: Reusing the exact same file path is a no-op and may not create a new snapshot. Use a unique filename to verify the behavior.")
        reg_path = st.text_input("Register path (local file or s3 uri)", value=str(L4_LOCAL_NS), key="register_path")
        if st.button(f"Step 3 — Register file with add_files", use_container_width=True):
            with st.spinner("Registering file via add_files..."):
                try:
                    # If local path: ensure a file exists by copying the latest Step1 output
                    if not reg_path.startswith("s3://"):
                        src_local = Path(st.session_state.get("step1_output", str(LOCAL_NS)))
                        Path(reg_path).parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(src_local, reg_path)
                    sid = add_files_register(WAREHOUSE, reg_path)
                    _log("logs_add_files", f"add_files snapshot: {sid}")
                    st.success(f"add_files committed. snapshot={sid}")
                except Exception as e:
                    _log("logs_add_files", f"add_files failed: {e}")
                    st.error(f"add_files failed: {e}")
        with st.expander("Logs / Output", expanded=False):
            st.code("\n".join(st.session_state["logs_add_files"]))

    st.divider()

    # Section: Inspect
    st.header("🔍 Step 4 — Inspect Table Metadata")
    st.caption("Show current snapshot IDs, locations, and schemas.")
    if st.button("Step 4 — Inspect Iceberg table metadata", use_container_width=True):
        with st.spinner("Loading table metadata..."):
            info = inspect_table(WAREHOUSE)
            _log("logs_inspect", "Fetched metadata.")
        st.json(info)
    with st.expander("Logs / Output", expanded=False):
        st.code("\n".join(st.session_state["logs_inspect"]))

    st.divider()
    st.header("🚀 Quick Test — Run All Steps")
    st.caption("Executes the full flow sequentially and prints a compact summary.")
    if st.button("Run end-to-end (generate → upload → append → add_files → inspect)", use_container_width=True):
        summary = {}
        try:
            p = gen_parquet_ns(LOCAL_NS)
            _log("logs_gen", f"Generated: {p}")
            summary["generated"] = str(p)
            out = rewrite_ns_to_us(LOCAL_NS, LOCAL_US)
            _log("logs_rewrite", f"Rewritten: {out}")
            summary["rewritten"] = str(out)
            upload_local_to_s3(LOCAL_NS, S2_S3_NS)
            _log("logs_upload", f"Uploaded: {S2_S3_NS}")
            summary["uploaded"] = S2_S3_NS
            # Ensure a unique S3 object for append
            upload_local_to_s3(LOCAL_NS, S3_S3_NS)
            sid_append = append_from_parquet(WAREHOUSE, S3_S3_NS)
            _log("logs_append", f"Snapshot append: {sid_append}")
            summary["snapshot_append"] = sid_append
            # Ensure a unique local file for add_files registration
            L4_LOCAL_NS.parent.mkdir(parents=True, exist_ok=True)
            unique_local = Path(str(L4_LOCAL_NS).replace('.parquet', f"_{int(time.time())}.parquet"))
            shutil.copyfile(LOCAL_NS, unique_local)
            sid_add = add_files_register(WAREHOUSE, str(unique_local))
            _log("logs_add_files", f"Snapshot add_files: {sid_add}")
            summary["snapshot_add_files"] = sid_add
            info = inspect_table(WAREHOUSE)
            _log("logs_inspect", "Fetched metadata after run-all")
            summary["inspect"] = info
            st.success("End-to-end flow completed.")
        except Exception as e:
            st.error(f"Run-all failed: {e}")
        with st.expander("Run-all Summary (JSON)", expanded=True):
            st.json(summary)

    st.divider()
    st.header("🧹 Reset & Utilities")
    st.caption("Drop Iceberg table and remove demo files (local/S3).")
    cU1, cU2 = st.columns(2)
    with cU1:
        if st.button("Reset demo state (drop table + delete files)", use_container_width=True):
            local_targets = [LOCAL_NS, LOCAL_US, L3_LOCAL_US, L4_LOCAL_NS]
            s3_targets = [S2_S3_NS, S3_S3_NS]
            with st.spinner("Resetting demo state..."):
                res = reset_demo_state(WAREHOUSE, local_targets, s3_targets)
            st.success("Reset complete.")
            st.json(res)
    with cU2:
        if st.button(f"Make data (overwrite ns) → {LOCAL_NS}", use_container_width=True):
            with st.spinner("Generating fresh ns data..."):
                p = gen_parquet_ns(LOCAL_NS)
                _log("logs_gen", f"Make data → {p}")
            st.success("Generated fresh sample data.")

    st.divider()
    st.header("✏️ Step 5 — Manual Row Editor")
    st.caption("Edit rows as JSON and write to a table. The table is recreated with a matching schema.")
    default_rows = [
        {"id": 1, "category": "electronics", "amount": 299.99},
        {"id": 2, "category": "clothing", "amount": 79.99},
        {"id": 3, "category": "groceries", "amount": 45.50},
        {"id": 4, "category": "electronics", "amount": 999.99},
        {"id": 5, "category": "clothing", "amount": 120.00},
    ]
    col_m1, col_m2 = st.columns([2, 1])
    with col_m1:
        json_text = st.text_area(
            "Rows (JSON list of objects)", value=json.dumps(default_rows, indent=2), height=220,
            help="Provide valid JSON (double quotes). You may also paste Python dict literal; we'll try to parse it."
        )
    with col_m2:
        target_table = st.text_input("Target table name", value="db.manual")
        st.caption("The table will be dropped and recreated with the edited schema.")
        if st.button("Step 5 — Write rows (recreate table)", use_container_width=True):
            try:
                try:
                    rows = json.loads(json_text)
                except json.JSONDecodeError:
                    rows = ast.literal_eval(json_text)
                assert isinstance(rows, list) and all(isinstance(r, dict) for r in rows)
            except Exception as e:
                st.error(f"Invalid JSON rows: {e}")
            else:
                try:
                    sid = write_manual_rows(WAREHOUSE, target_table, rows)
                    st.success(f"Wrote rows to {target_table}. snapshot={sid}")
                    with st.expander("Preview rows", expanded=False):
                        st.json(rows)
                except Exception as e:
                    st.error(f"Write failed: {e}")

    # Target table viewer
    st.subheader("🔎 Target Table Viewer")
    st.caption("Preview rows from the target table and its schema.")
    view_table = st.text_input("Table to view", value="db.manual", key="view_table_name")
    view_limit = st.number_input("Row limit", min_value=1, max_value=5000, value=100, step=50, key="view_limit")
    if st.button("Preview target table", use_container_width=True):
        try:
            from open_table_format.iceber_ops import preview_table_rows
            preview = preview_table_rows(WAREHOUSE, view_table, int(view_limit))
            st.code(preview.get("schema", ""), language="text")
            if "error" in preview:
                st.error(f"Preview failed: {preview['error']}")
            else:
                st.dataframe(preview.get("rows", []), use_container_width=True)
                st.caption(f"Rows shown: {preview.get('count', 0)}")
        except Exception as e:
            st.error(f"Preview failed: {e}")


if __name__ == "__main__":
    run()
