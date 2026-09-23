from src.config import SETTINGS


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors."""
    errors = []

    if df['order_id'].isna().any():
        errors.append('Found null order_id values')
    if df['order_id'].duplicated().any():
        errors.append('Found duplicate order_id values')

    quality = SETTINGS['quality']
    min_q, max_q = quality['min_quantity'], quality['max_quantity']
    if (df['quantity'] < min_q).any() or (df['quantity'] > max_q).any():
        errors.append(f'Found quantity outside allowed range {min_q}-{max_q}')

    for col in ('gross_amount', 'discount_amount', 'net_amount'):
        if (df[col] < 0).any():
            errors.append(f'Found negative {col}')

    allowed_statuses = set(quality['allowed_order_statuses'])
    if not df['status'].isin(allowed_statuses).all():
        errors.append('Found disallowed status values')

    required_audit_cols = ['pipeline_run_id', 'processed_at_utc', 'record_hash', 'source_updated_at']
    for col in required_audit_cols:
        if col not in df.columns or df[col].isna().any():
            errors.append(f'Missing or null required audit column: {col}')

    return errors