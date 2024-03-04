from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError
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
    
    
def data_from_mongob(uri,sources_name):
    client = MongoClient(uri, server_api=ServerApi('1'))

    client.admin.command('ping')
    # Retrieve news data from Google Cloud Storage
    db = client['news']
    collection = db[sources_name]
    return collection



def deduplicate_and_aggregate(uri,sources_name):

    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client['news']
    collection = db[sources_name]
    
    # Define the aggregation pipeline
    pipeline = [
        {
            "$group": {
                "_id": "$url",
                "uniqueDoc": {"$first": "$$ROOT"}
            }
        },
        {
            "$replaceRoot": {"newRoot": "$uniqueDoc"}
        },
        {
            "$addFields": {
                "convertedDate": {"$toDate": "$date"}
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$convertedDate"}
                },
                "count": {"$sum": 1},
                "articles": {"$push": "$$ROOT"}
            }
        },
        {
            "$merge": {
                "into": f"{sources_name}-date",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]
    
    try:
        # Execute the aggregation pipeline
        result = collection.aggregate(pipeline)
        # Since aggregate() is lazy, using a loop or similar method to initiate execution might be necessary
        # For example, converting it to a list (if the result set is not too large) or iterating over it
        for _ in result:
            pass
        print("Aggregation pipeline executed successfully.")
        return result
    except PyMongoError as e:
        print(f"An error occurred: {e}")

    