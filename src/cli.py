import argparse

from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.load.postgres import upsert_curated, load_partition
from src.validate.quality import validate_curated


def _run_pipeline(run_id):
    raw_dir = extract_sources(run_id)
    staging, _ = build_staging(raw_dir, run_id)
    curated, _ = build_curated(staging, run_id)
    return curated


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    run_id = new_run_id()

    if args.command == 'extract':
        raw_dir = extract_sources(run_id)
        print('Extracted to', raw_dir)

    elif args.command == 'transform':
        raw_dir = extract_sources(run_id)
        staging, quarantine = build_staging(raw_dir, run_id)
        curated, orphans = build_curated(staging, run_id)
        print('Staging rows:', {k: len(v) for k, v in staging.items()})
        print('Curated rows:', len(curated))
        print('Quarantined (staging):', len(quarantine))
        print('Quarantined (orphans):', len(orphans))

    elif args.command == 'load':
        curated = _run_pipeline(run_id)
        count = upsert_curated(curated, run_id)
        print(f'Upserted {count} curated rows (run_id={run_id})')

    elif args.command == 'validate':
        curated = _run_pipeline(run_id)
        errors = validate_curated(curated)
        if errors:
            print('Validation FAILED:')
            for e in errors:
                print(' -', e)
        else:
            print('Validation PASSED')

    elif args.command == 'load-partition':
        curated = _run_pipeline(run_id)
        count = load_partition(curated, args.year, args.month, run_id)
        print(f'Loaded {count} rows for partition {args.year}-{args.month:02d}')

    elif args.command == 'run-all':
        curated = _run_pipeline(run_id)
        errors = validate_curated(curated)
        count = upsert_curated(curated, run_id)
        print('Run ID:', run_id)
        print('Curated rows:', len(curated))
        print('Validation errors:', errors or 'none')
        print('Upserted rows:', count)

    elif args.command == 'benchmark':
        from src.benchmark.storage import run_benchmark
        from src.config import path_for
        curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
        run_benchmark(curated_path, path_for('benchmark_dir'), repeats=args.repeats)


if __name__ == '__main__':
    main()