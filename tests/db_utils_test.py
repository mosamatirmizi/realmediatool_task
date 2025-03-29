import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from utils.db_utils import get_engine, execute_query, insert_df


@patch('utils.db_utils.create_engine')
def test_get_engine_success(mock_create_engine):
    conn_str = "postgresql://postgres:postgres@localhost/rmt_db"
    engine_mock = MagicMock()
    mock_create_engine.return_value = engine_mock

    engine = get_engine(conn_str)

    mock_create_engine.assert_called_once_with(conn_str, pool_pre_ping=True)
    assert engine == engine_mock

@patch('utils.db_utils.create_engine', side_effect=SQLAlchemyError("Connection failed"))
def test_get_engine_failure(mock_create_engine):
    with pytest.raises(SQLAlchemyError):
        get_engine("invalid_connection")

@patch('utils.db_utils.pd.read_sql')
def test_execute_query_success(mock_read_sql):
    engine_mock = MagicMock()
    query = "SELECT * FROM test.T_TABLE"
    mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
    mock_read_sql.return_value = mock_df

    result_df = execute_query(engine_mock, query)

    mock_read_sql.assert_called_once()
    pd.testing.assert_frame_equal(result_df, mock_df)

@patch('utils.db_utils.pd.read_sql', side_effect=SQLAlchemyError("Query failed"))
def test_execute_query_failure(mock_read_sql):
    with pytest.raises(SQLAlchemyError):
        execute_query(MagicMock(), "SELECT * FROM invalid_table")

@patch('utils.db_utils.pd.DataFrame.to_sql')
def test_insert_df_success(mock_to_sql):
    engine_mock = MagicMock()
    df = pd.DataFrame({'col1': [1], 'col2': ['A']})

    insert_df(engine_mock, df, "T_TABLE", "test")

    mock_to_sql.assert_called_once_with(
        "T_TABLE", engine_mock, schema="test", if_exists='append', index=False, method='multi', chunksize=5000
    )

@patch('utils.db_utils.pd.DataFrame.to_sql', side_effect=SQLAlchemyError("Insertion failed"))
def test_insert_df_failure(mock_to_sql):
    with pytest.raises(SQLAlchemyError):
        insert_df(MagicMock(), pd.DataFrame({'col1': [1]}), "invalid_table", "test")
