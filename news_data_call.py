from google.cloud import storage
from datetime import date, datetime, timedelta
import json
import requests
import os
import requests



def retreive_api_data(site,url,pages):
    objects = []
    for page in pages:
        payload = {
            "language": "en",
            "site": site,
            "page":page
        }
        headers = {
            "content-type": "application/json",
            "X-RapidAPI-Key": "65aeb55cc4msh0c34dd7544eb736p12dae3jsn39fcdba3cd57",
            "X-RapidAPI-Host": "newsnow.p.rapidapi.com"
        }
        response = requests.post(url, json=payload, headers=headers)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
        # Parse and print the response *****
            data = response.json()
        objects.append(data)
            

    return objects

def wrtie_json_to_gcs(bucket_name, blob_name, service_account_key_file, data):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("w") as f:
        json.dump(data, f)    