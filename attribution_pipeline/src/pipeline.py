# Orchestrates all pipeline steps.

from data_extraction_Step2 import extract_customer_journeys
from data_transformation_Step3 import prepare_payload
from api_integration_Step3 import send_to_api
from data_loading_Step4 import load_api_results_to_db
from reporting_Step5 import populate_channel_reporting
from export_csv_Step6 import export_to_csv

def run_pipeline():
    """
    Bonus Step: Orchestrates the pipeline steps with optional time-range filtering.
    """
    journeys = extract_customer_journeys(start_date, end_date)
    payload = prepare_payload(journeys)
    responses = send_to_api(payload)
    load_api_results_to_db(responses)
    populate_channel_reporting()
    export_to_csv()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Run the Attribution Pipeline")
    parser.add_argument('--start_date', type=str, help="Start date (YYYY-MM-DD) for filtering data", required=False)
    parser.add_argument('--end_date', type=str, help="End date (YYYY-MM-DD) for filtering data", required=False)

    args = parser.parse_args()
    run_pipeline(args.start_date, args.end_date)
