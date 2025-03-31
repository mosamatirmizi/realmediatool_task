
# dags/advertiser_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils.db_utils import get_ch_client, get_engine
from analytical.daily_campaign_performance import populate_ch_daily_agg
from airflow.utils import timezone
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False
}

with DAG(
    'rmt_analytical',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:
    
    def run_daily_agg(**context):
        logging.info("Starting daily agg.")
        psql_engine = get_engine()
        populate_ch_daily_agg(psql_engine)

    impression_cur_task = PythonOperator(
        task_id='run_anallytical_pipeline',
        python_callable=run_daily_agg
    )

    impression_cur_task