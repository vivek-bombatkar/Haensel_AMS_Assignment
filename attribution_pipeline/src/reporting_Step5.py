# Populates the channel_reporting table.

from db_setup import connect_db

def populate_channel_reporting():
    """
    Aggregate and populate the channel_reporting table.
    (Assignment Step 5: Fill channel_reporting Table)
    """
    conn = connect_db()
    query = """
    INSERT INTO channel_reporting (channel_name, date, cost, ihc, ihc_revenue)
    SELECT ss.channel_name, ss.event_date, SUM(sc.cost) AS cost,
           SUM(acj.ihc) AS ihc,
           SUM(acj.ihc * c.revenue) AS ihc_revenue
    FROM session_sources ss
    JOIN session_costs sc ON ss.session_id = sc.session_id
    JOIN attribution_customer_journey acj ON ss.session_id = acj.session_id
    JOIN conversions c ON acj.conv_id = c.conv_id
    GROUP BY ss.channel_name, ss.event_date
    """
    conn.execute(query)
    conn.commit()
    conn.close()
    print("Channel reporting table populated successfully.")
