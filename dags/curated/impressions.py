# curated/advertiser.py
from utils.db_utils import execute_query , upsert_df
import pandas as pd
import logging

def extract_impression(engine , last_run_time):
    query = f"""
        SELECT id as impression_id, campaign_id ,created_at as impression_timestamp
        FROM public.impressions 
        where created_at > '{last_run_time}'
    """
    logging.info("Extracting Impressions.")
    return execute_query(engine, query)

def transform_impression(df, source):
    logging.info("Transforming Impressions data.")
    sat = df.drop_duplicates().copy()
    sat['record_source'] = source
    return {'sat': sat}

def load_impression(engine, data):
    logging.info("Loading Impressions data to curated.")
    upsert_df(engine, data['sat'], 'sat_impressions', 'curated', ['impression_id'])

def process_impression(source_engine, curated_engine, source_name, last_run_time):
    try:
        df = extract_impression(source_engine, last_run_time) 
        if df.empty:
            logging.warning("No Impressions data extracted.")
            return
        data = transform_impression(
            df,
            source_name
        )
        load_impression(curated_engine, data)
        logging.info("Impressions pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Impressions pipeline failed: {e}")
        raise
