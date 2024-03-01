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


def gcs_to_mongob(uri, data, sourcename):


    uri = "mongodb+srv://waffleiron:zNkFSHTfTGroBXRq@waffleironcluster.7uzj7t2.mongodb.net/?retryWrites=true&w=majority&appName=WaffleIronCluster"


    client = MongoClient(uri, server_api=ServerApi('1'))


    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    
    # Retrieve news data from Google Cloud Storage

    news_data = read_json_from_gcs(bucket_name, data, service_account_key_file)
    
    db = client[sourcename]
    collection = db['news']
    collection.insert_many(news_data)
    
    print("News data inserted into MongoDB!")