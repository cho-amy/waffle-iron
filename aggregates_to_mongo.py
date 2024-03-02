from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from google.cloud import storage
from news_data_call import *
<<<<<<< HEAD
=======
from user_definition import *
from datetime import datetime
>>>>>>> ba6da9c355c29ca5bc86359b22be3e87e8641c9e



def read_json_from_gcs(bucket_name, blob_name, service_account_key_file):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data_string = blob.download_as_text()
    data = json.loads(data_string)
    return data


def gcs_to_mongob(blob_name):


    uri = "mongodb+srv://waffleiron:zNkFSHTfTGroBXRq@waffleironcluster.7uzj7t2.mongodb.net/?retryWrites=true&w=majority&appName=WaffleIronCluster"


    client = MongoClient(uri, server_api=ServerApi('1'))


    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    
    # Retrieve news data from Google Cloud Storage
<<<<<<< HEAD
    news_data = retreive_news_data(service_account_key_file, bucket_name, blob_name)
    
    db = client['2024-03-01_cnn']
    collection = db[blob_name]
    collection.insert_many(news_data)
    
    print("News data inserted into MongoDB!")
 # gcs_to_mongob("2024-03-01_cnn.json")
=======
    news_sources = ["cnn", "foxnews"]

    for source in news_sources:
        current_date = datetime.now().strftime('%Y-%m-%d')
        str(current_date)
        blob_name = current_date + "_" + source
        news_data = read_json_from_gcs(bucket_name, blob_name, service_account_key_file)
        
        db = client[source]
        collection = db['news']
        collection.insert_many(news_data)
        
        print("News data inserted into MongoDB!")
>>>>>>> ba6da9c355c29ca5bc86359b22be3e87e8641c9e
