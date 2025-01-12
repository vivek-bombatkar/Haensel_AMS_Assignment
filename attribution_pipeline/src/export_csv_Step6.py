# Export channel_reporting table to CSV.

import pandas as pd
from db_setup import connect_db
from config import OUTPUT_CSV

def export_to_csv():
    """
    Export channel_reporting data to a CSV file.
    (Assignment Step 6: Export Data to CSV)
    """
    conn = connect_db()
    df = pd.read_sql_query("SELECT * FROM channel_reporting;", conn)
    df['CPO'] = df['cost'] / df['ihc']
    df['ROAS'] = df['ihc_revenue'] / df['cost']
    df.to_csv(OUTPUT_CSV, index=False)
    conn.close()
    print("Data exported to CSV successfully.")
