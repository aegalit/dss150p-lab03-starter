from pathlib import Path
import shutil
from src.config import path_for


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory."""
    source_dir = path_for('source_dir')
    raw_dir = path_for('raw_dir') / f'run_id={run_id}'
    raw_dir.mkdir(parents=True, exist_ok=True)
    for name in ('customers.csv', 'products.json', 'orders.csv'):
        shutil.copy2(source_dir / name, raw_dir / name)
    return raw_dir