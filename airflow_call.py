from datetime import date, timedelta
import os
import airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from user_definition import *
from news_data_call import *
from datetime import datetime
from aggregates_to_mongo import *

def load_data():
    for url in news.keys():
        data = retreive_api_data(url, api, pages)
        data = preprocess(data)
        blob = f"{today}_{news[url]}.json"
        write_json_to_gcs(bucket_name, blob, service_account_key_file, data)

def to_mongo():
    for url in news.keys():
        source_name = news[url]  ###cnn or fox 
        blob = f"{today}_{news[url]}.json"
        news_data = read_json_from_gcs(bucket_name, blob, service_account_key_file)
        gcs_to_mongob(uri,news_data,source_name)
with DAG(
    dag_id="waffle",
    schedule="@daily",
    start_date=datetime(2024, 2, 28),
    catchup=False
) as dag:
    API = PythonOperator(task_id="APi_call",
                         python_callable=load_data,
                         dag=dag)
    mongo = PythonOperator(task_id="to_mongo",
                         python_callable=to_mongo,
                         dag=dag)
    API
<<<<<<< HEAD
    API >> mongo
=======
    API >> mongo
    
>>>>>>> e50dad6b665a5ef40fe8bc91c53fce911412d1e0
