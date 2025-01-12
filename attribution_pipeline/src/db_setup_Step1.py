import sqlite3
from config import DATABASE_PATH

def connect_db():
    """
    Establish a connection to the SQLite database.
    (Assignment Step 1: Query Data from DB)
    """
    conn = sqlite3.connect(DATABASE_PATH)
    print("Database connected successfully.")
    return conn

def create_tables(conn):
    """
    Create necessary tables if they don't exist.
    (Assignment Notes: Create attribution_customer_journey and channel_reporting tables)
    """
    queries = [
        """CREATE TABLE IF NOT EXISTS attribution_customer_journey (
            conv_id TEXT,
            session_id TEXT,
            ihc REAL
        )""",
        """CREATE TABLE IF NOT EXISTS channel_reporting (
            channel_name TEXT,
            date TEXT,
            cost REAL,
            ihc REAL,
            ihc_revenue REAL
        )""",
        """
        CREATE TABLE IF NOT EXISTS conversions (
                                    conv_id text NOT NULL,
                                    user_id text NOT NULL,
                                    conv_date text NOT NULL,
                                    conv_time text NOT NULL,
                                    revenue real NOT NULL,
                                    PRIMARY KEY(conv_id)
                                )
        """,
        """
        CREATE TABLE IF NOT EXISTS session_costs (
                                    session_id text NOT NULL,
                                    cost real,
                                    PRIMARY KEY(session_id)
                                )
        """,
        """
        CREATE TABLE IF NOT EXISTS session_sources (
                                    session_id text NOT NULL,
                                    user_id text NOT NULL,
                                    event_date text NOT NULL,
                                    event_time text NOT NULL,
                                    channel_name text NOT NULL,
                                    holder_engagement INTEGER NOT NULL,
                                    closer_engagement INTEGER NOT NULL,
                                    impression_interaction INTEGER NOT NULL,
                                    PRIMARY KEY(session_id)
                                )
        """
    ]
    for query in queries:
        conn.execute(query)
    conn.commit()
    print("Tables ensured successfully.")

if __name__ == '__main__':
    conn = connect_db()
    create_tables(conn)
    conn.close()
