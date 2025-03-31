# curated/advertiser.py
from utils.db_utils import execute_query, insert_df , upsert_df
import pandas as pd
import logging

def extract_advertiser(engine , last_run_time):
    query = f"""
        SELECT id as advertiser_id, name as advertiser_name, created_at, updated_at 
        FROM public.advertiser 
        where updated_at >= '{last_run_time}'
    """
    logging.info("Extracting advertisers.")
    return execute_query(engine, query)

def transform_advertiser(df, source):
    logging.info("Transforming advertiser data.")
    hub = df[['advertiser_id']].drop_duplicates().copy()
    hub['record_source'] = source
    
    sat = df.copy()
    sat['record_source'] = source
    return {'hub': hub, 'sat': sat}


def load_advertiser(engine, data):
    logging.info("Loading advertiser data to curated.")
    insert_df(engine, data['hub'], 'hub_advertiser', 'curated')
    upsert_df(engine, data['sat'], 'sat_advertiser', 'curated', ['advertiser_id', 'load_date'])

def process_advertiser(source_engine, curated_engine, source_name, last_run_time):
    try:
        df = extract_advertiser(source_engine, last_run_time) 
        if df.empty:
            logging.warning("No advertiser data extracted.")
            return
        data = transform_advertiser(
            df,
            source_name
        )
        load_advertiser(curated_engine, data)
        logging.info("Advertiser pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Advertiser pipeline failed: {e}")
        raise
