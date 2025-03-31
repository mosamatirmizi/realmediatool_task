
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import logging
from airflow.models import Variable
import logging
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Table, MetaData
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import ClickHouseError

def get_conn_string(var_name: str) -> str:
    try:
        conn_str = Variable.get(var_name)
        logging.info(f"Retrieved Airflow Variable: {var_name}")
        return str(conn_str)
    except KeyError as e:
        logging.error(f"Variable {var_name} not found: {e}")
        raise

logging.basicConfig(level=logging.INFO)


def get_ch_client():
    try:
        client = get_client(host='clickhouse', port=8123, username='default', password='your_password', database='rmt_db')
        logging.info("Connected to ClickHouse.")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to ClickHouse: {e}")
    raise
    

def write_ch_df(df: pd.DataFrame, table_name:str, db_name = str):
    try:
        client = get_ch_client()
        client.insert_df(table=table_name, df=df, database=db_name)
        logging.info(f"Successfully inserted {len(df)} rows into {db_name}.{table_name}")
    except ClickHouseError as e:
        logging.error(f"ClickHouse insert failed: {e}")
    except ValueError as e:
        logging.error(f"ValueError during insert_df (e.g., schema mismatch): {e}")
    except Exception as e:
        logging.error(f"Unexpected error during ClickHouse insert: {e}")
        raise

    
def get_engine():
    """Creates and caches a PostgreSQL SQLAlchemy engine."""
    try:
        conn_str = get_conn_string("db_conn_str")
        engine = create_engine(conn_str, pool_pre_ping=True)
        logging.info("database engine created and cached.")
        return engine
    except SQLAlchemyError as e:
        logging.error(f"Engine creation failed: {e}")
        raise

def execute_query(engine, query: str) -> pd.DataFrame:
    """Executes a SQL query against PostgreSQL and returns results as a DataFrame."""
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            logging.info(f"Executed query successfully.")
            return df
    except SQLAlchemyError as e:
        logging.error(f"Query execution failed: {e}")
        raise

def insert_df(engine, df: pd.DataFrame, table: str, schema: str = "public"):
    """Inserts DataFrame into a specified PostgreSQL table."""
    try:
        with engine.begin() as conn:
            df.to_sql(
                name=table,
                con=conn,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )
        logging.info(f"Inserted {len(df)} rows into {schema}.{table}")
    except SQLAlchemyError as e:
        logging.error(f"Data insertion error: {e}")
        raise

def upsert_df(engine, df, table_name, schema, unique_cols):
    metadata = MetaData(schema=schema)
    table = Table(table_name, metadata, autoload_with=engine)

    insert_stmt = insert(table).values(df.to_dict(orient='records'))
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=unique_cols,
        set_={col.name: col for col in insert_stmt.excluded if col.name not in unique_cols}
    )

    try:
        with engine.connect() as conn:
            conn.execute(update_stmt)
            logging.info(f"Successfully upserted data into {schema}.{table_name}")
    except SQLAlchemyError as e:
        logging.error(f"Failed to upsert data into {schema}.{table_name}: {e}")
        raise

