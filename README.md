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


## Goal 3 — Storage Formats, Benchmarking, Partitioning

- Implemented `src/benchmark/storage.py`:
  - `run_benchmark()` materializes the curated dataset as CSV, JSON Lines, and Snappy-compressed Parquet, plus queries PostgreSQL directly.
  - Measures file size (bytes), write time, full-read time, and filtered-read time (`status = DELIVERED`), using the median of 5 repeated read/query runs per format.
  - PostgreSQL size measured via `pg_total_relation_size()` (server-side table size), not a single file-size number.
  - Results written to `data/benchmarks/benchmark_results.csv`.
  - `write_partitioned_parquet()` partitions the curated dataset by `order_year`/`order_month` into `data/partitioned/order_year=YYYY/order_month=M/`.
- Wired `src/cli.py` `benchmark --repeats N` command, and added partition writing to `run-all`.
- Verified partition structure: `data/partitioned/order_year=2025/order_month=1..12` and `order_year=2026/order_month=1..9`.
- Verified `load-partition --year 2026 --month 1`: loaded 2506 rows, recorded in `audit.partition_loads`. Rerun of the same partition updated the existing row (same partition_key, refreshed `loaded_at_utc`/`pipeline_run_id`) instead of creating a duplicate.

### Benchmark results (this machine, 49,897 rows, 5 repeats, median)

| Format     | Size (bytes) | Write time (s) | Full read (s) | Filtered read (s) |
|------------|-------------:|----------------:|----------------:|---------------------:|
| CSV        | 21,609,871   | 2.03            | 0.53            | 0.89                 |
| JSON Lines | 43,277,683   | 1.74            | 1.35            | 1.34                 |
| Parquet    | 6,027,599    | 0.21            | 0.08            | 0.04                 |
| PostgreSQL | 16,506,880   | N/A (already loaded) | 1.91       | 0.37                 |

These numbers are specific to this machine/run and are not a universal ranking of the formats.

### Analysis Questions

1. **Smallest file format?** Parquet, by a wide margin. Its columnar layout groups values of the same type together, which compresses far better than row-oriented text formats, and Snappy compression removes additional redundancy.
2. **Fastest full read? Best for every workload?** Parquet was fastest for a full read on this machine. That doesn't make it best for every workload — for example, a system that needs to append single rows one at a time or stream records line-by-line (like a log pipeline) is better served by JSON Lines or a database, since Parquet files are optimized for bulk columnar reads, not incremental single-row writes.
3. **Filtered retrieval: Parquet vs PostgreSQL?** Parquet's filtered read (0.04s) was faster than PostgreSQL's (0.37s) on this run because PostgreSQL had no index on `status`, so it performed a full sequential scan. Adding a B-tree index on `status` would let PostgreSQL use an index scan instead, which should substantially close or reverse that gap, especially as the table grows.
4. **Why is JSON Lines more pipeline-friendly than one big JSON array?** JSON Lines stores one complete, independent JSON object per line, so a consumer can read, process, and append records incrementally without ever loading or parsing the entire file. A single large JSON array must be fully parsed (and usually fully rewritten) to add or stream a single record, which doesn't scale for append-heavy or streaming pipelines.
5. **High-cardinality or poorly-localized partition key?** If a partition key has extremely high cardinality (e.g. partitioning by exact timestamp instead of year/month), the pipeline ends up creating an enormous number of tiny partition folders/files. This increases filesystem/metadata overhead, makes directory listing and query planning slower, and can leave many partitions with very few rows, which defeats the purpose of partitioning (reducing I/O) since the overhead of opening many small files can exceed the savings from skipping irrelevant data.

Git checkpoint: branch `goal3-storage-benchmark`, commit `feat: implement Goal 3 storage benchmarking and partitioning`.

## Goal 4  Airflow 

- Add execution_timeout to prevent hung tasks
- Branch load task on run_mode param (full vs partition)
- Add detailed failure_callback with run_id and exception context
- Verified: full run, partition run (audit.partition_loads), deliberate failure + retry + recovery