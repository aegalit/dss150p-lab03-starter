# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).

## Goal 1 — Reproducible Environment

- Local Python: 3.12.1 (see `.venv`), installed via `python -m venv .venv` and `pip install -r requirements.txt`
- Virtual environment (`.venv/`) is git-ignored; not committed. Committing it would tie the repo to one machine's binary paths and compiled dependencies, breaking reproducibility on another machine. `requirements.txt` pins exact versions so anyone can rebuild an equivalent environment.
- Secrets/config: `config/settings.yml` holds non-secret defaults; `.env` (git-ignored) holds environment-specific values (DB host/user/password); `src/config.py` is the only place merging them.
- Verified locally: `python -m src.cli validate-env`
- Verified in Docker: `docker compose build pipeline`, `docker compose up -d postgres`, `docker compose run --rm pipeline python -m src.cli validate-env`
- Verified PostgreSQL schemas: `audit`, `curated`, `staging` created; `curated.sales_order_lines` table exists
- Git checkpoint: branch `goal1-reproducible-environment`, commit `feat: add reproducible pipeline environment`


## Goal 2 — ETL Pipeline

- Implemented `src/extract/files.py`: copies customers.csv, products.json, orders.csv into a run-specific `data/raw/run_id=<run_id>/` snapshot without modifying source files.
- Implemented `src/transform/staging.py`:
  - Deduplicates customers/products/orders by business key, keeping the row with the latest `updated_at`.
  - Normalizes email (lowercase/trim) and city (title case); missing email is retained with an `email_quality_flag` rather than quarantined.
  - Flattens `category.name`/`category.department` from products JSON.
  - Validates order quantity (1-20), allowed statuses, and product price; invalid records written to `data/quarantine/staging_quarantine_<run_id>.csv` with a reason.
  - Adds `pipeline_run_id` and `staged_at_utc` to every staging dataset; writes staging output as Parquet.
- Implemented `src/transform/curated.py`:
  - Joins orders to customers/products; orphan references (missing customer/product) are quarantined to `data/quarantine/curated_quarantine_<run_id>.csv`, not silently dropped.
  - Calculates `gross_amount`, `discount_amount`, `net_amount`.
  - Adds audit columns `source_updated_at`, `pipeline_run_id`, `processed_at_utc`, and a deterministic `record_hash` based on business content only (not timestamps), so reruns with unchanged data don't produce a different hash.
- Implemented `src/load/postgres.py`: rerun-safe UPSERT into `curated.sales_order_lines` using `order_id` as the conflict key; update only fires when `record_hash` differs, so an unchanged rerun is a no-op.
- Implemented `src/validate/quality.py`: checks order_id uniqueness/non-null, quantity range, non-negative amounts, allowed statuses, and required audit columns.
- Wired `src/cli.py` commands: `extract`, `transform`, `load`, `validate`, `run-all`.
- Verified rerun-safety: ran `python -m src.cli run-all` twice; `SELECT COUNT(*) vs COUNT(DISTINCT order_id)` on `curated.sales_order_lines` returned 49897 = 49897 (no duplicates).
- Git checkpoint: branch `goal2-etl-pipeline`, commit `feat: implement Goal 2 ETL pipeline (extract, staging, curated, quarantine, rerun-safe upsert)`.