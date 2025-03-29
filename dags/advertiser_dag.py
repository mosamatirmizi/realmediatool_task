
# dags/advertiser_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils.db_utils import get_engine
from utils.airflow_utils import get_conn_string
from curated.advertiser import process_advertiser
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False
}

with DAG(
    'populate_advertiser_curated',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:
    
    def run_advertiser_pipeline():
        logging.info("Starting advertiser pipeline execution.")
        source_engine = get_engine(get_conn_string('source_db_conn'))
        curated_engine = get_engine(get_conn_string('curated_db_conn'))
        process_advertiser(source_engine, curated_engine, source_name='postgres_source')

    run_pipeline = PythonOperator(
        task_id='run_advertiser_pipeline',
        python_callable=run_advertiser_pipeline
    )
