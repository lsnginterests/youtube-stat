from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from assets import LAKEHOUSE

PROJECT_DIR = '/home/maxim/user/programming/youtube-stat'

with DAG(
    dag_id='youtube_stat_dq',
    start_date=datetime(2026, 8, 9),
    schedule=[LAKEHOUSE],
    catchup=False,
    max_active_runs=1,
    default_args={'cwd': PROJECT_DIR},
):
    BashOperator(
        task_id='dq',
        bash_command='python -m dataplatform dq',
    )
