import json
from pathlib import Path

import pandas as pd

from src.config import path_for, SETTINGS
from src.common.audit import utc_now_iso


def _load_customers(raw_dir: Path) -> pd.DataFrame:
    return pd.read_csv(raw_dir / 'customers.csv', dtype=str)


def _load_products(raw_dir: Path) -> pd.DataFrame:
    with (raw_dir / 'products.json').open(encoding='utf-8') as f:
        records = json.load(f)
    return pd.json_normalize(records)


def _load_orders(raw_dir: Path) -> pd.DataFrame:
    return pd.read_csv(raw_dir / 'orders.csv', dtype=str)


def _quarantine(df, reason):
    df = df.copy()
    df['quarantine_reason'] = reason
    return df


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets."""
    raw_dir = Path(raw_dir)
    quality = SETTINGS['quality']
    staged_at = utc_now_iso()
    quarantine_frames = []

    # ---------------- customers ----------------
    customers = _load_customers(raw_dir)
    customers['updated_at'] = pd.to_datetime(customers['updated_at'], utc=True, errors='coerce')
    customers = customers.sort_values('updated_at', ascending=False)
    customers = customers.drop_duplicates(subset='customer_id', keep='first')

    customers['email'] = customers['email'].astype(str).str.strip().str.lower()
    customers.loc[customers['email'].isin(['nan', 'none', '']), 'email'] = pd.NA
    customers['email_quality_flag'] = customers['email'].isna().map({True: 'missing_email', False: 'ok'})

    customers['city'] = customers['city'].astype(str).str.strip().str.title()
    customers['created_at'] = pd.to_datetime(customers['created_at'], utc=True, errors='coerce')

    customers['pipeline_run_id'] = run_id
    customers['staged_at_utc'] = staged_at

    # ---------------- products ----------------
    products = _load_products(raw_dir)
    products = products.rename(columns={'category.name': 'category_name', 'category.department': 'category_department'})
    products['unit_price'] = pd.to_numeric(products['unit_price'], errors='coerce')
    products['updated_at'] = pd.to_datetime(products['updated_at'], utc=True, errors='coerce')
    products = products.sort_values('updated_at', ascending=False)
    products = products.drop_duplicates(subset='product_id', keep='first')

    invalid_price_mask = products['unit_price'].isna() | (products['unit_price'] < 0)
    products_quarantine = _quarantine(products[invalid_price_mask], 'invalid_or_negative_price')
    products = products[~invalid_price_mask].copy()

    products['pipeline_run_id'] = run_id
    products['staged_at_utc'] = staged_at
    if not products_quarantine.empty:
        quarantine_frames.append(
            products_quarantine.assign(source_dataset='products', pipeline_run_id=run_id, staged_at_utc=staged_at)
        )

    # ---------------- orders ----------------
    orders = _load_orders(raw_dir)
    orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp'], utc=True, errors='coerce')
    orders['updated_at'] = pd.to_datetime(orders['updated_at'], utc=True, errors='coerce')
    orders = orders.sort_values('updated_at', ascending=False)
    orders = orders.drop_duplicates(subset='order_id', keep='first')

    orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce')
    orders['unit_price'] = pd.to_numeric(orders['unit_price'], errors='coerce')
    orders['discount_pct'] = pd.to_numeric(orders['discount_pct'], errors='coerce')

    allowed_statuses = set(quality['allowed_order_statuses'])
    min_q, max_q = quality['min_quantity'], quality['max_quantity']

    invalid_mask = (
        orders['order_timestamp'].isna()
        | orders['quantity'].isna()
        | (orders['quantity'] < min_q)
        | (orders['quantity'] > max_q)
        | (~orders['status'].isin(allowed_statuses))
    )
    orders_quarantine = _quarantine(orders[invalid_mask], 'invalid_quantity_status_or_timestamp')
    orders = orders[~invalid_mask].copy()

    orders['pipeline_run_id'] = run_id
    orders['staged_at_utc'] = staged_at
    if not orders_quarantine.empty:
        quarantine_frames.append(
            orders_quarantine.assign(source_dataset='orders', pipeline_run_id=run_id, staged_at_utc=staged_at)
        )

    staging = {'customers': customers, 'products': products, 'orders': orders}

    quarantine_dir = path_for('quarantine_dir')
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine_df = pd.concat(quarantine_frames, ignore_index=True) if quarantine_frames else pd.DataFrame()
    if not quarantine_df.empty:
        quarantine_df.to_csv(quarantine_dir / f'staging_quarantine_{run_id}.csv', index=False)

    staging_dir = path_for('staging_dir')
    staging_dir.mkdir(parents=True, exist_ok=True)
    for name, df in staging.items():
        df.to_parquet(staging_dir / f'{name}.parquet', index=False)

    return staging, quarantine_df