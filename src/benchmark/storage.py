import json
import statistics
import time
from pathlib import Path

import pandas as pd
import psycopg

from src.config import DB, SETTINGS


def _connect():
    return psycopg.connect(
        host=DB['host'], port=DB['port'], dbname=DB['dbname'],
        user=DB['user'], password=DB['password'],
    )


def _median_time(func, repeats: int) -> float:
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        func()
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    filter_status = SETTINGS['storage_benchmark']['filter_status']

    df = pd.read_parquet(curated_path)
    row_count = len(df)

    csv_path = output_dir / 'sales_order_lines.csv'
    jsonl_path = output_dir / 'sales_order_lines.jsonl'
    parquet_path = output_dir / 'sales_order_lines.parquet'

    results = []

    # ---------------- CSV ----------------
    write_time = _median_time(lambda: df.to_csv(csv_path, index=False), 1)
    size_bytes = csv_path.stat().st_size
    full_read_time = _median_time(lambda: pd.read_csv(csv_path), repeats)
    filtered_read_time = _median_time(
        lambda: pd.read_csv(csv_path).query(f"status == '{filter_status}'"), repeats
    )
    results.append({
        'format': 'csv', 'size_bytes': size_bytes, 'write_time_s': write_time,
        'full_read_time_s': full_read_time, 'filtered_read_time_s': filtered_read_time,
        'row_count': row_count,
    })

    # ---------------- JSON Lines ----------------
    write_time = _median_time(lambda: df.to_json(jsonl_path, orient='records', lines=True, date_format='iso'), 1)
    size_bytes = jsonl_path.stat().st_size
    full_read_time = _median_time(lambda: pd.read_json(jsonl_path, lines=True), repeats)
    filtered_read_time = _median_time(
        lambda: pd.read_json(jsonl_path, lines=True).query(f"status == '{filter_status}'"), repeats
    )
    results.append({
        'format': 'jsonl', 'size_bytes': size_bytes, 'write_time_s': write_time,
        'full_read_time_s': full_read_time, 'filtered_read_time_s': filtered_read_time,
        'row_count': row_count,
    })

    # ---------------- Parquet (compressed) ----------------
    write_time = _median_time(lambda: df.to_parquet(parquet_path, index=False, compression='snappy'), 1)
    size_bytes = parquet_path.stat().st_size
    full_read_time = _median_time(lambda: pd.read_parquet(parquet_path), repeats)
    filtered_read_time = _median_time(
        lambda: pd.read_parquet(parquet_path, filters=[('status', '==', filter_status)]), repeats
    )
    results.append({
        'format': 'parquet', 'size_bytes': size_bytes, 'write_time_s': write_time,
        'full_read_time_s': full_read_time, 'filtered_read_time_s': filtered_read_time,
        'row_count': row_count,
    })

    # ---------------- PostgreSQL ----------------
    def _pg_full_read():
        with _connect() as conn:
            return pd.read_sql('SELECT * FROM curated.sales_order_lines', conn)

    def _pg_filtered_read():
        with _connect() as conn:
            return pd.read_sql(
                'SELECT * FROM curated.sales_order_lines WHERE status = %(status)s',
                conn, params={'status': filter_status},
            )

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_total_relation_size('curated.sales_order_lines')"
            )
            pg_size_bytes = cur.fetchone()[0]

    pg_full_read_time = _median_time(_pg_full_read, repeats)
    pg_filtered_read_time = _median_time(_pg_filtered_read, repeats)
    with _connect() as conn:
        pg_row_count = pd.read_sql('SELECT COUNT(*) AS n FROM curated.sales_order_lines', conn)['n'].iloc[0]

    results.append({
        'format': 'postgresql', 'size_bytes': pg_size_bytes, 'write_time_s': None,
        'full_read_time_s': pg_full_read_time, 'filtered_read_time_s': pg_filtered_read_time,
        'row_count': int(pg_row_count),
    })

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_dir / 'benchmark_results.csv', index=False)
    print(results_df.to_string(index=False))
    return results_df


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_dir, partition_cols=['order_year', 'order_month'], index=False)
    return output_dir