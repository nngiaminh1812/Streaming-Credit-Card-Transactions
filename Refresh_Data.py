import requests
import os
from dotenv import load_dotenv
import jwt 

from pathlib import Path
load_dotenv()
# Superset config
SUPERSET_BASE_URL = 'http://localhost:5000'
DATASET_ID = os.getenv('DATASET_ID')
print(DATASET_ID)
SUPERSET_USERNAME = os.getenv('SUPERSET_USERNAME')
SUPERSET_PASSWORD = os.getenv('SUPERSET_PASSWORD')
SUPERSET_SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY')

def refresh_data():
    # Authenticate with Superset
    auth_url = f"{SUPERSET_BASE_URL}/api/v1/security/login"
    auth_payload = {
        "username": SUPERSET_USERNAME,
        "password": SUPERSET_PASSWORD,
        "provider": "db",
        "refresh": True
    }
    auth_response = requests.post(auth_url, json=auth_payload)
    auth_response.raise_for_status()
    access_token = auth_response.json()['access_token']
    print(f"Access token: {access_token}") 

    # Decode the JWT to inspect the payload
    decoded_payload = jwt.decode(access_token, options={"verify_signature": False})
    # print(f"Decoded payload: {decoded_payload}") 

    # the `sub` claim is a string
    if isinstance(decoded_payload.get("sub"), int):
        decoded_payload["sub"] = str(decoded_payload["sub"])

    # Re-encode the JWT with the SECRET_KEY
    re_encoded_token = jwt.encode(decoded_payload, SUPERSET_SECRET_KEY, algorithm="HS256")
    # print(f"Re-encoded token: {re_encoded_token}")  

    # get the CSRF token
    csrf_url = f"{SUPERSET_BASE_URL}/api/v1/security/csrf_token/"
    headers = {
        'Authorization': f'Bearer {re_encoded_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    csrf_response = requests.get(csrf_url, headers=headers)
    print(f"CSRF Response: {csrf_response.status_code}, {csrf_response.text}")  
    csrf_response.raise_for_status()
    csrf_token = csrf_response.json()['result']
    print(f"CSRF Token: {csrf_token}")  

    # the CSRF token to the headers
    headers.update({'X-CSRFToken': csrf_token})

    # Refresh dataset
    refresh_dataset = f"{SUPERSET_BASE_URL}/api/v1/dataset/2/refresh"
    refresh_response = requests.put(refresh_dataset, headers=headers)
    if refresh_response.status_code == 200:
        print("Dataset refreshed successfully.")
    else:
        print(f"Failed to refresh dataset: {refresh_response.status_code}, {refresh_response.text}")

    headers = {
        'Authorization': f'Bearer {re_encoded_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }    
    # /api/v1/chart/{pk}
    # for chart_id in range(4, 8):
    #     refresh_chart = f"{SUPERSET_BASE_URL}/api/v1/chart/{chart_id}"
    #     chart_res = requests.put(refresh_chart, headers=headers)
    #     if chart_res.status_code == 200:
    #         print("Dataset refreshed successfully.")
    #     else:
    #         print(f"Failed to refresh charts: {chart_res.status_code}, {chart_res.text}")

    # /api/v1/dashboard/{pk}
    refresh_db = f"{SUPERSET_BASE_URL}/api/v1/dashboard/1"
    db_res = requests.put(refresh_db, headers=headers)
    if db_res.status_code == 200:
        print("Dataset refreshed successfully.")
    else:
        print(f"Failed to refresh dataset: {db_res.status_code}, {db_res.text}")


if __name__ == "__main__":
    refresh_data()