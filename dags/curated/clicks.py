# curated/advertiser.py
from utils.db_utils import execute_query , upsert_df
import pandas as pd
import logging

def extract_click(engine , last_run_time):
    query = f"""
        SELECT id as click_id, campaign_id ,created_at as click_timestamp
        FROM public.clicks 
        where created_at > '{last_run_time}'
    """
    logging.info("Extracting Clicks.")
    return execute_query(engine, query)

def transform_click(df, source):
    logging.info("Transforming Clicks data.")
    sat = df.drop_duplicates().copy()
    sat['record_source'] = source
    return {'sat': sat}

def load_click(engine, data):
    logging.info("Loading Clicks data to curated.")
    upsert_df(engine, data['sat'], 'sat_clicks', 'curated', ['click_id'])

def process_click(source_engine, curated_engine, source_name, last_run_time):
    try:
        df = extract_click(source_engine, last_run_time) 
        if df.empty:
            logging.warning("No clicks data extracted.")
            return
        data = transform_click(
            df,
            source_name
        )
        load_click(curated_engine, data)
        logging.info("clicks pipeline completed successfully.")
    except Exception as e:
        logging.error(f"clicks pipeline failed: {e}")
        raise
