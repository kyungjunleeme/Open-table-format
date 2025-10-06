SHELL := /bin/bash

# Environment for local MinIO/S3
ENV_VARS = AWS_ACCESS_KEY_ID=minioadmin \
           AWS_SECRET_ACCESS_KEY=minioadmin \
           AWS_REGION=us-east-1 \
           AWS_ENDPOINT_URL=http://localhost:9000 \
           AWS_ENDPOINT_URL_S3=http://localhost:9000 \
           AWS_S3_ADDRESSING_STYLE=path \
           WAREHOUSE=s3://iceberg/warehouse \
           DATAPATH=s3://iceberg/data

.PHONY: help setup dev.up dev.down app test test.e2e e2e dev.reset data.make data.edit data.upload2

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_\.-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-16s %s\n", $$1, $$2}'

setup: ## Install dependencies (uv or pip)
	@command -v uv >/dev/null 2>&1 && uv sync --dev || \
		( python -m venv .venv && . .venv/bin/activate && pip install -e . && pip install -U pytest )

dev.up: ## Start MinIO + mc bootstrap
	docker compose up -d

dev.down: ## Stop stack and remove volumes
	docker compose down -v

app: ## Run Streamlit UI with local MinIO env
	env $(ENV_VARS) streamlit run src/open_table_format/app.py

test: ## Run unit tests
	pytest -q

test.e2e: ## Run e2e tests against local MinIO
	env $(ENV_VARS) pytest -m e2e -q

e2e: ## Run end-to-end demo script inside the app container
	docker compose up -d
	docker compose exec -T app python scripts/e2e_check.py

dev.reset: ## Reset demo state (drop table + delete files)
	docker compose exec -T app python scripts/dev_tasks.py reset || true

data.make: ## Generate Step 1 ns Parquet locally
	docker compose exec -T app python scripts/dev_tasks.py make-data || true

data.edit: ## Append 1 row to the Step 1 ns file
	docker compose exec -T app python scripts/dev_tasks.py edit-data --rows 1 || true

data.upload2: ## Upload Step 1 ns → Step 2 S3 path
	docker compose exec -T app python scripts/dev_tasks.py upload-step2 || true
