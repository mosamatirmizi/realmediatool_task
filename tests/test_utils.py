import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_utils import  get_engine , insert_df , execute_query , get_conn_string
import pandas as pd
import pytest
from sqlalchemy import text


class StringMismatch(Exception):
    pass


def test_variable_string_match():
    expected_conn = "postgresql+psycopg2://postgres:postgres@localhost:5432/rmt_db"
    actual_conn = get_conn_string("db_conn_str")

    if actual_conn != expected_conn:
        raise StringMismatch(f"Expected {expected_conn}, got {actual_conn}")


@pytest.fixture(scope="module")
def db_engine():
    engine = get_engine()
    print(engine.dialect.name)
    return engine


def test_insert_df_success(db_engine):
    db_engine.execute(text("CREATE TABLE IF NOT EXISTS test_table (name varchar, value int)"))
    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "value": [100, 200]
    })
    print(db_engine.dialect.name)
    insert_df(db_engine, df, table="test_table")
    
    result_df = execute_query(db_engine, "SELECT * FROM test_table ORDER BY name")
    assert len(result_df) == 2
    assert list(result_df["name"]) == ["Alice", "Bob"]
    assert list(result_df["value"]) == [100, 200]

    db_engine.execute(text("DROP TABLE IF EXISTS test_table"))