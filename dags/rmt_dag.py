
# dags/advertiser_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils.db_utils import get_engine
from curated.advertiser import process_advertiser
from curated.campaign import process_campaign
from curated.clicks import process_click
from curated.impressions import process_impression
from airflow.utils import timezone
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False
}

with DAG(
    'rmt_curated',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:
    
    def run_advertiser_cur_pipeline(**context):
        logging.info("Starting advertiser curation execution.")
        psql_engine = get_engine()
        prev_exec_date = context.get('prev_data_interval_end_success') or timezone.datetime(1970, 1, 1)
        logging.info(f"Last execution date: {prev_exec_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        process_advertiser(
            source_engine=psql_engine,
            curated_engine=psql_engine, 
            source_name='postgres_source',
            last_run_time=prev_exec_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            )

    def run_campaign_cur_pipeline(**context):
        logging.info("Starting campaign curation execution.")
        psql_engine = get_engine()
        prev_exec_date = context.get('prev_data_interval_end_success') or timezone.datetime(1970, 1, 1)
        logging.info(f"Last execution date: {prev_exec_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        process_campaign(
            source_engine=psql_engine,
            curated_engine=psql_engine, 
            source_name='postgres_source',
            last_run_time=prev_exec_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            )
        
    def run_clicks_cur_pipeline(**context):
        logging.info("Starting clicks curation execution.")
        psql_engine = get_engine()
        prev_exec_date = context.get('prev_data_interval_end_success') or timezone.datetime(1970, 1, 1)
        logging.info(f"Last execution date: {prev_exec_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        process_click(
            source_engine=psql_engine,
            curated_engine=psql_engine, 
            source_name='postgres_source',
            last_run_time=prev_exec_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            )
        
    def run_impression_cur_pipeline(**context):
        logging.info("Starting impression curation execution.")
        psql_engine = get_engine()
        prev_exec_date = context.get('prev_data_interval_end_success') or timezone.datetime(1970, 1, 1)
        logging.info(f"Last execution date: {prev_exec_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        process_impression(
            source_engine=psql_engine,
            curated_engine=psql_engine, 
            source_name='postgres_source',
            last_run_time=prev_exec_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            )


    advertiser_cur_task = PythonOperator(
        task_id='run_advertiser_cur_pipeline',
        python_callable=run_advertiser_cur_pipeline
    )

    campaign_cur_task = PythonOperator(
        task_id='run_campaign_cur_pipeline',
        python_callable=run_campaign_cur_pipeline
    )

    clicks_cur_task = PythonOperator(
        task_id='run_clicks_cur_pipeline',
        python_callable=run_clicks_cur_pipeline
    )

    impression_cur_task = PythonOperator(
        task_id='run_impression_cur_pipeline',
        python_callable=run_impression_cur_pipeline
    )


    advertiser_cur_task >> campaign_cur_task
    campaign_cur_task >> [clicks_cur_task, impression_cur_task]

    
