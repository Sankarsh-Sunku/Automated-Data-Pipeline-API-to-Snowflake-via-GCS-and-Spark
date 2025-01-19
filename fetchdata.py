import requests
import pandas as pd
import datetime
import uuid
import os
from datetime import date
from google.cloud import storage

def upload_to_gcs(bucket,path, file):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(path)
    blob.upload_from_filename(file)
    print(f"File {file} uploader to {path}")

def fetch_data_from_api():
    today = date.today()
    apiKey = "*******************************"
    base_url = "https://newsapi.org/v2/top-headlines?country=us&category=business&apiKey={}"
    # base_url = "https://newsapi.org/v2/everything?q={}&from={}&to={}&sortBy=popularity&apiKey={}&language=en"
    start_date_value = str(today - datetime.timedelta(days=1))
    end_date_value = str(today)

    df = pd.DataFrame(columns=['newsTitle', 'timestamp', 'url_source', 'content', 'source', 'author', 'urlToImage'])

    # url_extractor = base_url.format("tesla", start_date_value, end_date_value, apiKey)
    url_extractor = base_url.format(apiKey)
    r = requests.get(url_extractor)
    data = r.json()

    for i in data['articles']:
        newsTitle = i['title']
        timestamp = i['publishedAt']
        url_source = i['url']
        source = i['source']['name']
        author = i['author']
        urlToImage = i['urlToImage']
        partial_content = i['content'] if i['content'] is not None else ""

        if len(partial_content) >= 200:
            trimmed_part = partial_content[:199]
        if '.' in partial_content:
            trimmed_part = partial_content[:partial_content.rindex('.')]
        else:
            trimmed_part = partial_content

        new_row = pd.DataFrame({
            'newsTitle': [newsTitle],
            'timestamp': [timestamp],
            'url_source': [url_source],
            'content': [partial_content],
            'source': [source],
            'author': [author],
            'urlToImage': [urlToImage]
        })

        df = pd.concat([df, new_row], ignore_index=True)

    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f'run_{current_time}.parquet'
    # print(df)

    print("Current Working Directory:", os.getcwd())

    df.to_parquet(filename)

    bucket_name = 'snowflake_projects_sa'
    destination_blob_name = f'news_data_analysis/parquet_files/{filename}'
    upload_to_gcs(bucket_name,destination_blob_name,filename)

    os.remove(filename)
