from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from google.cloud import storage

from user_definition import *

from news_data_call import *
from user_definition import *


<<<<<<< HEAD
def retreive_news_data(service_account_key_file, bucket_name, blob_name):
=======

def read_json_from_gcs(bucket_name, blob_name, service_account_key_file):
>>>>>>> 70862ca7df181682b231e08d500ddf6ef9a01ea4
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    # print(storage_client)
    bucket = storage_client.bucket(bucket_name)
    # print(bucket)
    blob = bucket.blob(blob_name)
<<<<<<< HEAD
    # print(blob)
    json_str = blob.download_as_string().decode("utf8")
    # print(json_str)
    json_data = json.loads(json_str)
    return json_data
def airflow_to_mongob()
    url = "https://newsnow.p.rapidapi.com/newsv2_top_news_site"
    site = "https://www.foxnews.com/politics"
=======
    data_string = blob.download_as_text()
    data = json.loads(data_string)
    return data
>>>>>>> 70862ca7df181682b231e08d500ddf6ef9a01ea4

    data = retreive_api_data(site, url,3)


    uri = "mongodb+srv://waffleiron:zNkFSHTfTGroBXRq@waffleironcluster.7uzj7t2.mongodb.net/?retryWrites=true&w=majority&appName=WaffleIronCluster"


    client = MongoClient(uri, server_api=ServerApi('1'))


    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    
    # Retrieve news data from Google Cloud Storage
    news_data = retreive_news_data(service_account_key_file, bucket_name,"waffle.json")
    
    # Load the news data into MongoDB
    # Assuming there's a collection named 'news' in your MongoDB database
    db = client['Trial_test']
    collection = db['news']
    collection.insert_many(news_data)
    
    print("News data inserted into MongoDB!")