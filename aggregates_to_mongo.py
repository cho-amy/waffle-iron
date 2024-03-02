from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from google.cloud import storage
from news_data_call import *
from user_definition import *
from datetime import datetime



def read_json_from_gcs(bucket_name, blob_name, service_account_key_file):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data_string = blob.download_as_text()
    data = json.loads(data_string)
    return data


def gcs_to_mongob(uri,data,sources_name):


    client = MongoClient(uri, server_api=ServerApi('1'))

    client.admin.command('ping')
    # Retrieve news data from Google Cloud Storage
    db = client['news']
    collection = db[sources_name]
    collection.insert_many(data)