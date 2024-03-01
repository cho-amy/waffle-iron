import json
from google.cloud import storage
from mongodb import *
from pyspark.sql import Row, SparkSession

from user_definition import *

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from google.cloud import storage

from user_definition import *

from news_data_call import *
from user_definition import *



def retreive_news_data(service_account_key_file,bucket_name,blob_name):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    json_str = blob.download_as_string().decode("utf8")
    json_data = json.loads(json_str)
    return json_data

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from google.cloud import storage

from user_definition import *

from news_data_call import *
from user_definition import *

url = "https://newsnow.p.rapidapi.com/newsv2_top_news_site"
site = "https://www.foxnews.com/politics"

data = retreive_api_data(site,url,3)

uri = "mongodb+srv://waffleiron:zNkFSHTfTGroBXRq@waffleironcluster.7uzj7t2.mongodb.net/?retryWrites=true&w=majority&appName=WaffleIronCluster"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    
    # Retrieve news data from Google Cloud Storage
    news_data = retreive_news_data(service_account_key_file, bucket_name,"waffle.json")
    
    # Load the news data into MongoDB
    # Assuming there's a collection named 'news' in your MongoDB database
    db = client['your_database_name']
    collection = db['news']
    collection.insert_many(news_data)
    
    print("News data inserted into MongoDB!")
    
except Exception as e:
    print(e)