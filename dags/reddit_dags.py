import os
import sys
from datetime import datetime
sys.path.insert(1,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.redit_pipeline import reddit_pipeline
from pipelines.postgres_pipeline import postgres_pipeline

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Darshil Shah',
    'start_date': datetime(2024, 5, 21)
}

file_postfix = datetime.now().strftime("%Y%m%d%H")
SUBREDDIT = 'dataengineering'


with  DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
) as dag:

    # extraction from reddit
    extract = PythonOperator(
        task_id='reddit_extraction',
        python_callable=reddit_pipeline,
        op_kwargs={
            'file_name': f'reddit_{file_postfix}',
            'subreddit': SUBREDDIT,
            'time_filter': 'day',
            'limit': 1000
        }
    )

    postgres = PythonOperator(
    task_id='load_postgres',
    python_callable = postgres_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}.csv'
    }
    )

    extract >> postgres


