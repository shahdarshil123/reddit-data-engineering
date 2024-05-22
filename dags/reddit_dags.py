import os
import sys
from datetime import datetime
sys.path.insert(1,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



from pipelines.redit_pipeline import reddit_pipeline
from airflow import DAG
from airflow.operators.python import PythonOperator


# sys.path.append(os.path.dirname(os.path.abspath(__file__)))


default_args = {
    'owner': 'Darshil Shah',
    'start_date': datetime(2024, 5, 21)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)

# extraction from reddit
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

# reddit_pipeline('test',limit=25)