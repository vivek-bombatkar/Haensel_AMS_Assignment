# Sends data to IHC API and handles responses.

import requests
from config import API_URL, API_KEY
from data_transformation import chunk_data

def send_to_api(payload):
    """
    Send chunked payloads to the IHC API.
    (Assignment Step 3: Send Data to API)
    """
    headers = {"Authorization": f"Bearer {API_KEY}"}
    responses = []
    for chunk in chunk_data(payload, 10):
        response = requests.post(API_URL, headers=headers, json=chunk)
        if response.status_code == 200:
            responses.append(response.json())
        else:
            print(f"API Error: {response.status_code}")
    print("Data sent to API successfully.")
    return responses
