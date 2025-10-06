# Repository Guidelines

## Project Structure & Module Organization
- Source: `src/open_table_format/` — `app.py` (Streamlit UI), `iceber_ops.py` (PyIceberg helpers), `cli.py` (utility CLI), `__init__.py` (package entry).
- Tests: `tests/` — unit (`test_parquet_precision.py`) and e2e (`test_e2e.py`).
- Config: `pyproject.toml` (deps, pytest), `docker-compose.yml` (MinIO/S3), `README.md`.

## Build, Test, and Development Commands
- Setup (recommended): `uv sync --dev` (Python 3.12+). Alternative: `python -m venv .venv && source .venv/bin/activate && pip install -e . && pip install pytest`.
- Start MinIO: `docker compose up -d`.
- Run tests: `pytest -q`. E2E: `export AWS_ENDPOINT_URL=http://localhost:9000 WAREHOUSE=s3://iceberg/warehouse DATAPATH=s3://iceberg/data AWS_REGION=us-east-1 && pytest -m e2e -q`.
- Run UI: `streamlit run src/open_table_format/app.py`.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation and type hints.
- Names: modules/files `snake_case.py`; functions/variables `snake_case`; classes `PascalCase`.
- Imports: prefer absolute `open_table_format...` imports.
- Keep functions small; document non-obvious behavior with concise docstrings.

## Testing Guidelines
- Use `pytest`; place tests under `tests/` named `test_*.py`.
- Use fixtures like `tmp_path`; avoid network calls in unit tests.
- Mark integration tests with `@pytest.mark.e2e`; keep them reproducible with MinIO.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise subject (e.g., "Add ns→us cast helper").
- PRs: include summary, linked issues, test plan, and (for UI) screenshots. Update docs when behavior changes.

## Security & Configuration
- Never commit credentials. Configure via env vars: `WAREHOUSE`, `DATAPATH`, `AWS_ENDPOINT_URL`, `AWS_REGION`.
- Default S3 URIs: `s3://iceberg/warehouse` and `s3://iceberg/data`.

## Agent Notes
- Keep changes minimal and scoped; do not rename files or alter public APIs without discussion.
- Match existing patterns; add or update tests with code changes.

