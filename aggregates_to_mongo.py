from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from google.cloud import storage

from user_definition import *

from news_data_call import *


def retreive_news_data(service_account_key_file, bucket_name, blob_name):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    # print(storage_client)
    bucket = storage_client.bucket(bucket_name)
    # print(bucket)
    blob = bucket.blob(blob_name)
    # print(blob)
    json_str = blob.download_as_string().decode("utf8")
    # print(json_str)
    json_data = json.loads(json_str)
    return json_data

def gcs_to_mongob(blob_name):


    uri = "mongodb+srv://waffleiron:zNkFSHTfTGroBXRq@waffleironcluster.7uzj7t2.mongodb.net/?retryWrites=true&w=majority&appName=WaffleIronCluster"


    client = MongoClient(uri, server_api=ServerApi('1'))


    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    
    # Retrieve news data from Google Cloud Storage
    news_data = retreive_news_data(service_account_key_file, bucket_name, blob_name)
    
    db = client['2024-03-01_cnn']
    collection = db[blob_name]
    collection.insert_many(news_data)
    
    print("News data inserted into MongoDB!")
 # gcs_to_mongob("2024-03-01_cnn.json")