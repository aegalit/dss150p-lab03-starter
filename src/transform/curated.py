import pandas as pd

from src.config import path_for
from src.common.audit import utc_now_iso, record_hash


def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows."""
    customers = staging['customers']
    products = staging['products']
    orders = staging['orders']

    merged = orders.merge(
        customers[['customer_id', 'city', 'customer_tier', 'updated_at']].rename(
            columns={'city': 'customer_city', 'updated_at': 'customer_updated_at'}
        ),
        on='customer_id', how='left', indicator='customer_match'
    )
    merged = merged.merge(
        products[['product_id', 'name', 'category_name', 'brand', 'unit_price', 'updated_at']].rename(
            columns={
                'name': 'product_name',
                'category_name': 'category',
                'unit_price': 'product_unit_price',
                'updated_at': 'product_updated_at',
            }
        ),
        on='product_id', how='left', indicator='product_match'
    )

    orphan_mask = (merged['customer_match'] == 'left_only') | (merged['product_match'] == 'left_only')
    orphans = merged[orphan_mask].copy()
    orphans['quarantine_reason'] = orphans.apply(
        lambda r: 'orphan_customer' if r['customer_match'] == 'left_only' else 'orphan_product', axis=1
    )

    curated = merged[~orphan_mask].copy()
    curated = curated.drop(columns=['customer_match', 'product_match'])

    curated['gross_amount'] = curated['quantity'] * curated['unit_price']
    curated['discount_amount'] = curated['gross_amount'] * curated['discount_pct']
    curated['net_amount'] = curated['gross_amount'] - curated['discount_amount']

    curated['source_updated_at'] = curated[['updated_at', 'customer_updated_at', 'product_updated_at']].max(axis=1)
    curated['pipeline_run_id'] = run_id
    curated['processed_at_utc'] = utc_now_iso()

    hash_keys = ['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price', 'discount_pct', 'status', 'order_timestamp']
    curated['record_hash'] = curated.apply(lambda r: record_hash(r.to_dict(), hash_keys), axis=1)

    curated['order_year'] = curated['order_timestamp'].dt.year
    curated['order_month'] = curated['order_timestamp'].dt.month

    curated_dir = path_for('curated_dir')
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(curated_dir / 'sales_order_lines.parquet', index=False)

    quarantine_dir = path_for('quarantine_dir')
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    if not orphans.empty:
        orphans.assign(pipeline_run_id=run_id, processed_at_utc=utc_now_iso()).to_csv(
            quarantine_dir / f'curated_quarantine_{run_id}.csv', index=False
        )

    return curated, orphans