from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from google.cloud import storage
from news_data_call import *
from user_definition import *
from datetime import datetime
import csv
import pandas as pd


def mongod_to_spark(data, sources_name):
    data = list(data.find({}))

    # Create a Spark session
    spark = SparkSession.builder.appName("MongoDB to DataFrame").getOrCreate()
    csv_file = os.path.join(
        os.environ["HOME"], f"airflow/dags/ml_data/{sources_name}.csv"
    )
    headers = data[0].keys()
    with open(csv_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)

        # Write column headers
        writer.writeheader()

        # Write data rows
        for row in data:
            writer.writerow(row)


def create_df(sources_name):
    dfs = pd.read_csv(
        os.path.join(os.environ["HOME"], f"airflow/dags/ml_data/{sources_name}.csv")
    )
    dfs = dfs.drop(columns=["videos", "images"])
    dfs["publisher"] = dfs["publisher"].apply(lambda x: eval(x)["title"])

    schema = StructType(
        [
            StructField("_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("top_image", StringType(), True),
            StructField("url", StringType(), True),
            StructField("date", StringType(), True),
            StructField("short_description", StringType(), True),
            StructField("text", StringType(), True),
            StructField("publisher", StringType(), True),
        ]
    )

    df = spark.createDataFrame(dfs, schema=schema)
    # df.show()
    return df
