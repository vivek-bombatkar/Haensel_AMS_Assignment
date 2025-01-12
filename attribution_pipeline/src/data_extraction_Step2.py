import pandas as pd
from db_setup_Step1 import connect_db

def extract_customer_journeys(start_date=None, end_date=None):
    """
    Extract customer journey data by querying session_sources and conversions
    with optional time-range filtering.
    """
    conn = connect_db()

    # Base query
    query = """
    SELECT ss.session_id, ss.user_id, ss.event_date, ss.event_time, ss.channel_name,
           ss.holder_engagement, ss.closer_engagement, ss.impression_interaction,
           c.conv_id, c.conv_date, c.conv_time, c.revenue
    FROM session_sources ss
    JOIN conversions c ON ss.user_id = c.user_id
    WHERE (ss.event_date || ' ' || ss.event_time) < (c.conv_date || ' ' || c.conv_time)
    """

    # Add time-range filtering
    if start_date:
        query += f" AND ss.event_date >= '{start_date}'"
    if end_date:
        query += f" AND ss.event_date <= '{end_date}'"

    df = pd.read_sql_query(query, conn)
    conn.close()
    print(f"Customer journeys extracted for range: {start_date} to {end_date}.")
    return df
