from datetime import date, timedelta
import os

import pandas as pd
import datetime
import requests
import json
import airflow
from airflow import DAG
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
SQLALCHEMY_SILENCE_UBER_WARNING=1
def create_dir(parent_dir, directory):
    path = os.path.join(parent_dir, directory)
    os.makedirs(path, exist_ok=True)


def retrieve_text(site ="https://www.foxnews.com/politics" ,pages  = 3):
    url = "https://newsnow.p.rapidapi.com/newsv2_top_news_site"
    article_texts = []
    for page in pages:
        payload = {
            "language": "en",
            "site": site,
            "page":page

        }
        headers = {
            "content-type": "application/json",
            "X-RapidAPI-Key": "65aeb55cc4msh0c34dd7544eb736p12dae3jsn39fcdba3cd57",
            "X-RapidAPI-Host": "newsnow.p.rapidapi.com"
        }
        response = requests.post(url, json=payload, headers=headers)
        # Check if the request was successful (status code 200)
        print(response.status_code)
        if response.status_code == 200:
        # Parse and print the response *****
            data = response.json()
        print(data)
        print(f"News from page {page}:")
        for news_item in data["news"]:
            print(news_item["title"])
            print(news_item["url"])
            print(news_item["text"])
            print("-" * 50)
            # Append the text of the article to the list
            article_texts.append(news_item["text"])
    else:
        print(f"Error fetching page {page}: {response.status_code}")
    # File path for the JSON file
    json_file_path = "newssleep1.json"
    # Convert the list of article texts to JSON format
    json_data = json.dumps(data, indent=4)
    # Write the JSON data to a file
    with open(json_file_path, "w") as json_file:
        json_file.write(json_data)
    print(f"JSON data has been written to {json_file_path}")
    
    

with DAG(
    dag_id="msds697-task2",
    schedule= "@daly",
    start_date=datetime(2024, 2, 28),
    catchup=False
) as dag:
    API = PythonOperator(task_id = "APi_call",
                                                  python_callable = retrieve_text,
                                                  dag=dag)


    API

