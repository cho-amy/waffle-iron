import json
from google.cloud import storage
from mongodb import *
from pyspark.sql import Row, SparkSession

from user_definition import *



def retreive_news_data(service_account_key_file,bucket_name,blob_name):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    json_str = blob.download_as_string().decode("utf8")
    json_data = json.loads(json_str)
    return json_data