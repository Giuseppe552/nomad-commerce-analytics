# ====== Nomad Commerce Analytics — Makefile ======
SHELL := /bin/bash
.DEFAULT_GOAL := help

# Load .env if present (safe, non-fatal)
ifneq (,$(wildcard .env))
include .env
export
endif

# Defaults (can be overridden in .env or CLI: MODE=synth make ingest)
PYTHON ?= ./.venv/bin/python
PIP ?= ./.venv/bin/pip
MODE ?= real                           # real | synth
DBT_PROFILES_DIR ?= ./dbt
DUCKDB_PATH ?= warehouse/nomad.duckdb

# Helpers
ACT := source .venv/bin/activate

.PHONY: help
help: ## Show help for each target
	@grep -E '^[a-zA-Z0-9_\-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf " \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------- Environment ----------
.PHONY: env
env: ## Create .env from example if missing
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")
	@echo "Using MODE=$(MODE), DUCKDB_PATH=$(DUCKDB_PATH), DBT_PROFILES_DIR=$(DBT_PROFILES_DIR)"

.PHONY: venv
venv: ## Create virtualenv at .venv
	@test -d .venv || python3 -m venv .venv
	@$(ACT) && $(PIP) install --upgrade pip

.PHONY: setup
setup: venv env ## Install project + dev tooling, pre-commit hooks
	@$(ACT) && $(PIP) install -e .[dev]
	@$(ACT) && pre-commit install
	@mkdir -p warehouse artifacts/logs exports site data/synth
	@echo "Setup complete."

# ---------- Data / Warehouse ----------
.PHONY: duckdb
duckdb: ## Ensure DuckDB file exists
	@mkdir -p warehouse
	@$(ACT) && $(PYTHON) - <<'PY'
import os, duckdb
path = os.environ.get("DUCKDB_PATH","warehouse/nomad.duckdb")
os.makedirs(os.path.dirname(path), exist_ok=True)
duckdb.connect(path).close()
print(f"Ensured DuckDB at {path}")
PY

.PHONY: ingest
ingest: duckdb ## Ingest data into DuckDB (MODE=real|synth)
	@echo "Ingesting with MODE=$(MODE)"
	@if [ "$(MODE)" = "synth" ]; then \
		$(ACT) && $(PYTHON) scripts/generate_synth_data.py --out data/synth; \
		$(ACT) && $(PYTHON) scripts/ingest_olist.py --source synth --db $(DUCKDB_PATH); \
	else \
		test -f data/real/olist_orders_dataset.csv || (echo "Missing Olist CSVs in data/real/. Place them first." && exit 2); \
		$(ACT) && $(PYTHON) scripts/ingest_olist.py --source real --db $(DUCKDB_PATH); \
	fi

.PHONY: quality
quality: ## Run pre-dbt quality checks (contracts)
	@$(ACT) && $(PYTHON) scripts/quality_checks.py --db $(DUCKDB_PATH) --mode $(MODE)

# ---------- dbt ----------
.PHONY: dbt_deps
dbt_deps: ## Install dbt packages
	@$(ACT) && dbt deps --project-dir dbt --profiles-dir $(DBT_PROFILES_DIR)

.PHONY: dbt_build
dbt_build: dbt_deps ## Build + test dbt models (mode-aware)
	@$(ACT) && dbt build --project-dir dbt --profiles-dir $(DBT_PROFILES_DIR) --vars 'mode: $(MODE)'

.PHONY: docs
docs: dbt_deps ## Build dbt docs (site/)
	@$(ACT) && dbt docs generate --project-dir dbt --profiles-dir $(DBT_PROFILES_DIR)
	@$(ACT) && $(PYTHON) scripts/snapshot_db_docs.py || true

# ---------- App ----------
.PHONY: app
app: ## Run Streamlit app (reads MODE from env)
	@STREAMLIT_BROWSER_GATHER_USAGE_STATS=0 $(ACT) && streamlit run app/streamlit_app.py

# ---------- Tests & Lint ----------
.PHONY: lint
lint: ## Ruff + Black + SQLFluff (no fixes)
	@$(ACT) && ruff check .
	@$(ACT) && black --check .
	@$(ACT) && sqlfluff lint dbt --dialect duckdb || true

.PHONY: fix
fix: ## Auto-fix Python & SQL style
	@$(ACT) && ruff check . --fix
	@$(ACT) && black .
	@$(ACT) && sqlfluff fix dbt --dialect duckdb || true

.PHONY: test
test: ## Run unit + e2e tests
	@$(ACT) && pytest -q

# ---------- CI aggregate ----------
.PHONY: ci
ci: setup lint ingest quality dbt_build test docs ## Full pipeline for CI

# ---------- Utilities ----------
.PHONY: clean
clean: ## Remove build artifacts and caches
	@rm -rf .pytest_cache .ruff_cache site target artifacts/logs/*
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Cleaned."

.PHONY: hash
hash: ## Write determinism hash of key KPI table to artifacts/build_hash.txt
	@$(ACT) && $(PYTHON) - <<'PY'
import duckdb, os, hashlib
db=os.environ.get("DUCKDB_PATH","warehouse/nomad.duckdb")
con=duckdb.connect(db)
try:
    df=con.execute("select * from mrt_kpis_daily_real order by 1,2,3").fetchdf()
except Exception:
    df=con.execute("select * from mrt_kpis_daily_synth order by 1,2,3").fetchdf()
h=hashlib.sha256(df.to_parquet(index=False)).hexdigest()
os.makedirs("artifacts", exist_ok=True)
open("artifacts/build_hash.txt","w").write(h+"\n")
print("Wrote artifacts/build_hash.txt")
PY
