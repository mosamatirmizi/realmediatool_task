# utils/db_utils.py
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

@lru_cache(maxsize=None)
def get_engine(conn_str: str):
    try:
        engine = create_engine(conn_str, pool_pre_ping=True)
        logging.info("Database engine created and cached")
        return engine
    except SQLAlchemyError as e:
        logging.error(f"DB Connection Error: {e}")
        raise

def execute_query(engine, query: str) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except SQLAlchemyError as e:
        logging.error(f"Query Execution Failed: {e}")
        raise

def insert_df(engine, df: pd.DataFrame, table: str, schema: str):
    try:
        df.to_sql(table, engine, schema=schema, if_exists='append', 
                  index=False, method='multi', chunksize=5000)
        logging.info(f"Inserted {len(df)} rows into {schema}.{table}")
    except SQLAlchemyError as e:
        logging.error(f"Data Insertion Error: {e}")
        raise
