# curated/advertiser.py
from utils.db_utils import execute_query, insert_df
import pandas as pd
import logging

def extract_advertiser(engine):
    query = "SELECT id as advertiser_id, name as advertiser_name, created_at, updated_at FROM RAW.advertiser"
    logging.info("Extracting advertisers.")
    return execute_query(engine, query)

def transform_advertiser(df, source):
    logging.info("Transforming advertiser data.")
    hub = df[['advertiser_id']].copy()
    hub['record_source'] = source
    
    sat = df.copy()
    sat['record_source'] = source
    return {'hub': hub, 'sat': sat}

def load_advertiser(engine, data):
    logging.info("Loading advertiser data to curated.")
    insert_df(engine, data['hub'], 'hub_advertiser', 'curated')
    insert_df(engine, data['sat'], 'sat_advertiser', 'curated')

def process_advertiser(source_engine, curated_engine, source_name):
    try:
        data = transform_advertiser(
            extract_advertiser(source_engine),
            source_name
        )
        load_advertiser(curated_engine, data)
        logging.info("Advertiser pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Advertiser pipeline failed: {e}")
        raise
