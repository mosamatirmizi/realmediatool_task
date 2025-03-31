# curated/advertiser.py
from utils.db_utils import execute_query, insert_df , upsert_df
import pandas as pd
import logging

def extract_campaign(engine , last_run_time):
    query = f"""
        SELECT id as campaign_id, name as campaign_name,bid as bid_amount, budget, start_date , end_date, advertiser_id, 
         created_at, updated_at 
        FROM public.campaign 
        where updated_at > '{last_run_time}'
    """
    logging.info("Extracting campaign.")
    return execute_query(engine, query)

def transform_campaign(df, source):
    logging.info("Transforming campaign data.")
    hub = df[['campaign_id']].drop_duplicates().copy()
    hub['record_source'] = source
    
    link = df[['campaign_id','advertiser_id','updated_at','created_at']].drop_duplicates().copy()
    link['record_source'] = source    

    sat = df.copy()
    sat['record_source'] = source
    return {'hub': hub, 'link':link ,'sat': sat}


def load_campaign(engine, data):
    logging.info("Loading campaign data to curated.")
    insert_df(engine, data['hub'], 'hub_campaign', 'curated')
    upsert_df(engine, data['sat'], 'sat_campaign', 'curated', ['campaign_id', 'advertiser_id','load_date']) 
    upsert_df(engine, data['link'], 'link_advertiser_campaign', 'curated', ['advertiser_id','campaign_id'])  

def process_campaign(source_engine, curated_engine, source_name, last_run_time):
    try:
        df = extract_campaign(source_engine, last_run_time) 
        if df.empty:
            logging.warning("No campaign data extracted.")
            return
        data = transform_campaign(
            df,
            source_name
        )
        load_campaign(curated_engine, data)
        logging.info("campaign pipeline completed successfully.")
    except Exception as e:
        logging.error(f"campaign pipeline failed: {e}")
        raise

