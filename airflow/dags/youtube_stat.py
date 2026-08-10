from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT_DIR = '/home/maxim/user/programming/youtube-stat'

with DAG(
    dag_id='youtube_stat',
    start_date=datetime(2026, 8, 9),
    schedule='0 3 * * *',
    catchup=False,
    max_active_runs=1,
    default_args={'cwd': PROJECT_DIR},
):
    extract = BashOperator(
        task_id='extract',
        bash_command='python -m extract_load.run',
        retries=2,
    )
    bronze = BashOperator(
        task_id='bronze',
        bash_command='python -m dataplatform run-layer bronze',
    )
    silver = BashOperator(
        task_id='silver',
        bash_command='python -m dataplatform run-layer silver',
    )
    gold = BashOperator(
        task_id='gold',
        bash_command='python -m dataplatform run-layer gold',
    )

    extract >> bronze >> silver >> gold
