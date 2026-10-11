import importlib
import sys

import psycopg

from src.config import DB, path_for

REQUIRED_PACKAGES = ('pandas', 'pyarrow', 'psycopg', 'yaml', 'dotenv')
SOURCE_FILES = ('customers.csv', 'products.json', 'orders.csv')


def check_python():
    version = sys.version_info
    return 'python_version', version >= (3, 11), f'{version.major}.{version.minor}.{version.micro}'


def check_packages():
    results = []
    for name in REQUIRED_PACKAGES:
        try:
            module = importlib.import_module(name)
            results.append((f'package:{name}', True, getattr(module, '__version__', 'installed')))
        except ImportError as exc:
            results.append((f'package:{name}', False, str(exc)))
    return results


def check_source_files():
    source_dir = path_for('source_dir')
    return [(f'source:{name}', (source_dir / name).is_file(), str(source_dir / name)) for name in SOURCE_FILES]


def check_database():
    target = f"{DB['host']}:{DB['port']}/{DB['dbname']}"
    try:
        with psycopg.connect(
            host=DB['host'], port=DB['port'], dbname=DB['dbname'],
            user=DB['user'], password=DB['password'], connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1')
        return 'database_connection', True, target
    except psycopg.Error as exc:
        return 'database_connection', False, f'{target} ({str(exc).strip().splitlines()[0]})'


def run_checks(require_db=False):
    results = [check_python(), *check_packages(), *check_source_files(), check_database()]
    failed = [
        name for name, ok, _ in results
        if not ok and (name != 'database_connection' or require_db)
    ]
    return results, failed