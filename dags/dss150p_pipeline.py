from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'


def failure_callback(context):
    """Print concise, traceable failure context: run, task, and error."""
    ti = context['task_instance']
    print(
        'TASK FAILED:',
        f"dag_run_id={context['run_id']}",
        f"task_id={ti.task_id}",
        f"try_number={ti.try_number}",
        f"exception={context.get('exception')}",
    )


DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    # Daily at 02:00 UTC: source systems finish their exports overnight,
    # and analysts need the curated dataset ready before business hours.
    schedule='0 2 * * *',
    # catchup=False: this pipeline reflects the current state of source systems,
    # not a historical replay. Backfilling would need explicit, deliberate runs
    # (see docs/backfill note), not automatic catch-up of every missed day.
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli extract',
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli transform',
    )

    # Orchestration-level choice only: which existing CLI command to call.
    # No transformation/business logic lives here.
    load = BashOperator(
        task_id='load',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
            '{% if params.run_mode == "partition" %}'
            'python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }}'
            '{% else %}'
            'python -m src.cli load'
            '{% endif %}'
        ),
    )

    validate = BashOperator(
        task_id='validate',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli validate',
    )

    extract >> transform >> load >> validate