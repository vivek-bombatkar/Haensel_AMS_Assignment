# test_pipeline.py
import pytest
import sqlite3
import pandas as pd
from unittest.mock import patch, MagicMock

# Import all functions from the pipeline
from src.data_extraction_Step2 import extract_customer_journeys
from src.data_transformation_Step3 import prepare_payload
from src.api_integration_Step3 import send_to_api
from src.data_loading_Step4 import load_api_results_to_db
from src.reporting_Step5 import populate_channel_reporting
from src.export_csv_Step6 import export_to_csv


# Mock Database Path
DB_PATH = 'test_challenge.db'

# Fixtures for setup and teardown
@pytest.fixture
def mock_db():
    conn = sqlite3.connect(DB_PATH)
    yield conn
    conn.close()

@pytest.fixture
def mock_journeys():
    return pd.DataFrame({
        'conv_id': ['1', '1', '2'],
        'session_id': ['s1', 's2', 's3'],
        'user_id': ['u1', 'u1', 'u2'],
        'event_date': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'event_time': ['12:00:00', '13:00:00', '14:00:00'],
        'channel_name': ['Email', 'Social', 'Search'],
        'holder_engagement': [1, 0, 1],
        'closer_engagement': [0, 1, 0],
        'impression_interaction': [1, 0, 1],
        'conv_date': ['2024-01-02', '2024-01-02', '2024-01-03'],
        'conv_time': ['15:00:00', '15:00:00', '16:00:00'],
        'revenue': [100.0, 100.0, 200.0],
    })

# Tests for db_setup.py
def test_connect_db(mock_db):
    conn = connect_db()
    assert isinstance(conn, sqlite3.Connection)
    conn.close()

def test_create_tables(mock_db):
    conn = mock_db
    create_tables(conn)
    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
    assert 'attribution_customer_journey' in tables['name'].values
    assert 'channel_reporting' in tables['name'].values

# Tests for data_extraction.py
@patch('src.db_setup.connect_db', return_value=sqlite3.connect(DB_PATH))
def test_extract_customer_journeys(mock_connect_db, mock_db):
    conn = mock_db
    conn.execute("""
        CREATE TABLE session_sources (
            session_id TEXT, user_id TEXT, event_date TEXT, event_time TEXT,
            channel_name TEXT, holder_engagement INTEGER, closer_engagement INTEGER,
            impression_interaction INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE conversions (
            conv_id TEXT, user_id TEXT, conv_date TEXT, conv_time TEXT, revenue REAL
        )
    """)
    conn.commit()

    df = extract_customer_journeys()
    assert isinstance(df, pd.DataFrame)

# Tests for data_transformation.py
def test_prepare_payload(mock_journeys):
    payload = prepare_payload(mock_journeys)
    assert isinstance(payload, dict)
    assert '1' in payload

def test_chunk_data(mock_journeys):
    payload = prepare_payload(mock_journeys)
    chunks = list(chunk_data(payload, 2))
    assert len(chunks) == 2

# Tests for api_integration.py
@patch('requests.post')
def test_send_to_api(mock_post, mock_journeys):
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {'data': [{'conv_id': '1', 'session_id': 's1', 'ihc': 0.5}]}
    payload = prepare_payload(mock_journeys)
    responses = send_to_api(payload)
    assert len(responses) > 0

# Tests for data_loading.py
@patch('src.db_setup.connect_db', return_value=sqlite3.connect(DB_PATH))
def test_load_api_results_to_db(mock_connect_db, mock_db):
    conn = mock_db
    conn.execute("""
        CREATE TABLE attribution_customer_journey (
            conv_id TEXT, session_id TEXT, ihc REAL
        )
    """)
    conn.commit()
    mock_responses = [{'data': [{'conv_id': '1', 'session_id': 's1', 'ihc': 0.5}]}]
    load_api_results_to_db(mock_responses)
    result = pd.read_sql_query("SELECT * FROM attribution_customer_journey;", conn)
    assert len(result) > 0

# Tests for reporting.py
@patch('src.db_setup.connect_db', return_value=sqlite3.connect(DB_PATH))
def test_populate_channel_reporting(mock_connect_db, mock_db):
    conn = mock_db
    conn.execute("""
        CREATE TABLE channel_reporting (
            channel_name TEXT, date TEXT, cost REAL, ihc REAL, ihc_revenue REAL
        )
    """)
    conn.commit()
    populate_channel_reporting()
    result = pd.read_sql_query("SELECT * FROM channel_reporting;", conn)
    assert len(result) >= 0

# Tests for export_csv.py
@patch('src.db_setup.connect_db', return_value=sqlite3.connect(DB_PATH))
def test_export_to_csv(mock_connect_db, mock_db):
    conn = mock_db
    conn.execute("""
        CREATE TABLE channel_reporting (
            channel_name TEXT, date TEXT, cost REAL, ihc REAL, ihc_revenue REAL
        )
    """)
    conn.commit()
    export_to_csv()
    assert os.path.exists('output/channel_reporting.csv')

# Cleanup after tests
def teardown_module(module):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
