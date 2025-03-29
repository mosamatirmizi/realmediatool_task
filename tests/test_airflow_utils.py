import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
from airflow.exceptions import AirflowException
from utils.airflow_utils import get_conn_string

class StringMismatch(Exception):
    pass

def test_variable_string_match():
    expected_conn = "postgresql://user:pass@localhost/db"
    actual_conn = get_conn_string("db_conn_str")

    if actual_conn != expected_conn:
        raise StringMismatch(f"Expected {expected_conn}, got {actual_conn}")

# def test_variable_not_exist_raises_airflow_exception():
#     with pytest.raises(AirflowException):
#         get_conn_string("db_conn_str")