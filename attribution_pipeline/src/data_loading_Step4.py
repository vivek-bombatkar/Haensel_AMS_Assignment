# Loads API results into attribution_customer_journey table.

from db_setup import connect_db

def load_api_results_to_db(api_responses):
    conn = connect_db()
    for resp in api_responses:
        data = [(item['conv_id'], item['session_id'], item['ihc']) for item in resp['data']]
        conn.executemany(
            "INSERT INTO attribution_customer_journey (conv_id, session_id, ihc) VALUES (?, ?, ?)", data
        )
    conn.commit()
    conn.close()
    print("API results loaded into the database successfully.")
