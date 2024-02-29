from google.cloud import storage
from datetime import date, datetime, timedelta
import json
import requests
import os
import requests
import requests
import re
import json
from bs4 import BeautifulSoup



def retreive_api_data(site,url,pages):
    objects = []
    for page in range(1,pages+1):
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
        if response.status_code == 200:
        # Parse and print the response *****
            data = response.json()
        objects.append(data)
            

    return objects

def wrtie_json_to_gcs(bucket_name, blob_name, service_account_key_file, data):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("w") as f:
        json.dump(data, f)
        




def scrape_cnn_articles():
    # URL
    url = 'https://www.cnn.com'

    # Send an HTTP GET request to the URL
    response = requests.get(url)

    # Parse the HTML content of the response using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links on the page
    links = [link.get('href') for link in soup.find_all('a')]

    # Filter URLs to those starting with '/2024'
    filtered_urls = ["https://www.cnn.com" + url for url in links if url and url.startswith('/20')]

    articles = []

    for link in filtered_urls:
        
        r = requests.get(link)

        # Extract date from the URL
        date_pattern = r'/(\d{4}/\d{2}/\d{2})/'
        date_match = re.search(date_pattern, link)
        date = date_match.group(1) if date_match else None

        # Extract HTML content
        html_content = r.content.decode('utf-8')

        # Extract article body
        body_pattern = r'"articleBody"\s*:\s*"([^"]+)"'
        body_match = re.search(body_pattern, html_content)
        body = body_match.group(1) if body_match else None

        # Extract article headline
        header_pattern = r'"headline"\s*:\s*"([^"]+)"'
        header_match = re.search(header_pattern, html_content)
        header = header_match.group(1) if header_match else None

        # Extract article author
        author_pattern = r'"author"\s*:\s*"([^"]+)"'
        author_match = re.search(author_pattern, html_content)
        author = author_match.group(1) if author_match else None

        article = {
            "URL": link,
            "Date": date,
            "Header": header,
            "Author": author,
            "Text": body
        }

        articles.append(article)
    return articles

def scrape_fox_articles():
    # URL
    url = 'https://www.foxnews.com/'

    # Send an HTTP GET request to the URL
    response = requests.get(url)

    # Parse the HTML content of the response using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links on the page
    links = [link.get('href') for link in soup.find_all('a')]

    # Filter URLs to those starting with '/2024'
    filtered_urls = [url for url in links if url and url.startswith('https://www.foxnews.com/')]

    articles = []

    for link in filtered_urls:
        
        r = requests.get(link)
        
        soup = BeautifulSoup(r.text, 'html.parser')

        # Extract date from the URL
        #time
        date = soup.find('time')
        date = date.text.strip() if date else None

        # Extract HTML content
        html_content = r.content.decode('utf-8')

        # Extract article body
        body_pattern = r'"articleBody"\s*:\s*"([^"]+)"'
        body_match = re.search(body_pattern, html_content)
        body = body_match.group(1) if body_match else None

        title = soup.find('title')
        title = title.text.strip() if title else None
        title

        author_pattern = r'"name":([^"]+)"'
        author_match = re.search(body_pattern, html_content)
        author = body_match.group(1) if body_match else None

        article = {
            "URL": link,
            "Date": date,
            "Header": title,
            "Author": author,
            "Text": body
        }

        articles.append(article)

    # Write the extracted data to a JSON file
    return articles