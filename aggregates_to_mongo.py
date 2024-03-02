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
    return collection


def mongod_to_spark(data, sources_name):
    data = list(data.find({}))

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("MongoDB to DataFrame") \
        .getOrCreate()
    csv_file = f"{sources_name}.csv"
    headers = data[0].keys()
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
    
    # Write column headers
        writer.writeheader()
    
    # Write data rows
        for row in data:
            writer.writerow(row)



    dfs = pd.read_csv(f"{sources_name}.csv")
    dfs = dfs.drop(columns=["videos", "images"])
    dfs["publisher"] = dfs["publisher"].apply(lambda x: eval(x)['title'])
    
    
    schema = StructType([
        StructField("_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("top_image", StringType(), True),
        StructField("url", StringType(), True),
        StructField("date", StringType(), True),
        StructField("short_description", StringType(), True),
        StructField("text", StringType(), True),
        StructField("publisher", StringType(), True)
        ])

    df = spark.createDataFrame(dfs, schema=schema)
    # df.show()
    return df