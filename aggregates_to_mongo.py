from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError
import json
from google.cloud import storage
from news_data_call import *
from user_definition import *
from datetime import datetime



def read_json_from_gcs(bucket_name, blob_name, service_account_key_file):
    """
    Reads a JSON file from a specified Google Cloud Storage (GCS) bucket.

    Parameters:
    - bucket_name: Name of the GCS bucket.
    - blob_name: Name of the blob (file) in the bucket.
    - service_account_key_file: Path to the service account key file for authentication.

    Returns:
    - data: The JSON data read from the blob as a Python dictionary.
    """
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data_string = blob.download_as_text()
    data = json.loads(data_string)
    return data


def gcs_to_mongob(uri,data,sources_name):
    """
    Inserts data into a MongoDB collection from Google Cloud Storage.

    Parameters:
    - uri: MongoDB URI for connecting to the database.
    - data: The data to be inserted into the MongoDB collection.
    - sources_name: The name of the MongoDB collection.

    The function establishes a connection to MongoDB, selects the specified collection,
    and inserts the data. It also performs a basic ping to the database to ensure the connection is successful.
    """

    client = MongoClient(uri, server_api=ServerApi('1'))

    client.admin.command('ping')
    # Retrieve news data from Google Cloud Storage
    db = client['news']
    collection = db[sources_name]
    collection.insert_many(data)
    
    
def data_from_mongob(uri,sources_name):
    """
    Retrieves a MongoDB collection.

    Parameters:
    - uri: MongoDB URI for connecting to the database.
    - sources_name: The name of the MongoDB collection to retrieve.

    Returns:
    - collection: The MongoDB collection specified by sources_name.
    """
    client = MongoClient(uri, server_api=ServerApi('1'))

    client.admin.command('ping')
    # Retrieve news data from Google Cloud Storage
    db = client['news']
    collection = db[sources_name]
    return collection



def deduplicate_and_aggregate(uri, sources_name):
    """
    Deduplicates data within a MongoDB collection and aggregates it by date.

    Parameters:
    - uri: MongoDB URI for connecting to the database.
    - sources_name: The name of the MongoDB collection to process.

    This function defines and executes an aggregation pipeline that:
    1. Groups documents by URL to remove duplicates.
    2. Replaces the root of each document with the unique document found.
    3. Converts the date field to a date object and adds it to each document.
    4. Groups documents by the converted date, counting documents and pushing them into an array.
    5. Merges the results into a new collection, replacing or inserting documents as necessary.

    It handles exceptions related to the aggregation and prints an error message if one occurs.
    """
    
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
            "$project": {
                "videos": 0,  # Exclude the 'video' field
                "images": 0,  # Exclude the 'image' field
                "publisher": 0  # Exclude the 'publisher' field
            }
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

    
