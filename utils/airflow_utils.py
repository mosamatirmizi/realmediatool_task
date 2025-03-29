# utils/airflow_utils.py
from airflow.models import Variable
import logging

def get_conn_string(var_name: str) -> str:
    try:
        conn_str = Variable.get(var_name)
        logging.info(f"Retrieved Airflow Variable: {var_name}")
        return conn_str
    except KeyError as e:
        logging.error(f"Variable {var_name} not found: {e}")
        raise



