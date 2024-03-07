from datetime import date, timedelta
import os
import airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.types import *

import csv
import pandas as pd
from ml_pipline.ml_pip import *
from ml_pipline.user_definition import *
from ml_pipline.API_gcs import *
from datetime import datetime
from ml_pipline.aggregates_to_mongo import *

# Stage 1 Function:  load data from news APIs, preprocess it, and write it to Google Cloud Storage
def load_data():
    for url in news.keys():
        data = retreive_api_data(url, api, pages)
        data = preprocess(data)
        blob = f"{today}_{news[url]}.json"
        write_json_to_gcs(bucket_name, blob, service_account_key_file, data)
# Stage 2 Function: transfer data from GCS to MongoDB, with a check to skip empty data files
def to_mongo(): ### parse data would fail if data file is empty in 
    for url in news.keys():
        source_name = news[url]  ###cnn or fox 
        blob = f"{today}_{news[url]}.json"
        news_data = read_json_from_gcs(bucket_name, blob, service_account_key_file)
        ### parse data would fail if data file is empty, insert check point here
        if not news_data or news_data == {}:
            print(f"Data for {blob} is empty, skipping...")
            continue  # Skip the rest of the loop and move to the next item
        gcs_to_mongob(uri,news_data,source_name)
def run_aggregate():
    for url in news.keys():
        source_name = news[url]
        deduplicate_and_aggregate(uri,source_name)
    
# Stage 3 Function: parse data from MongoDB and prepare it for machine learning tasks
def data_parse_from_mongo():
    for url in news.keys():
        source_name = news[url]  ###cnn or fox 
        data = data_from_mongob(uri,source_name)
        mongod_to_spark(data,source_name)
        
        
        
    
# Define the DAG for scheduling tasks with Airflow
with DAG(
    dag_id="waffle",
    schedule="@daily",
    start_date=datetime(2024, 2, 28), # Start date for the DAG
    catchup=False # Prevent backfilling of past dates
) as dag:
    # Define tasks using PythonOperator to call Python functions
    API = PythonOperator(task_id="APi_call",
                         python_callable=load_data,
                         dag=dag)
    mongo = PythonOperator(task_id="upload_data_to_mongo",
                         python_callable=to_mongo,
                         dag=dag)
    aggegate = PythonOperator(task_id="aggregate_data_mongo",
                         python_callable=run_aggregate,
                         dag=dag)
    ML_data = PythonOperator(task_id="parse_data_for_ml",
                         python_callable=data_parse_from_mongo,
                         dag=dag)
    # Define task dependencies using the bitshift operators
    API
    API >> mongo >> aggegate >> ML_data #API task runs first, followed by mongo, then ML_data

