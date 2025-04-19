import os
from googleapiclient.discovery import build
from google.cloud import pubsub_v1
import json
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta,timezone
import os
from googleapiclient.discovery import build
from google.cloud import pubsub_v1
import json
import time

from google.cloud import bigquery

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 7),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    
}


YOUTUBE_API_KEY  = os.getenv('YOUTUBE_API_KEY')
PROJECT_ID = "aesthetic-nova-454803-r7"
DATASET_ID = "youtube_trending_dataset"

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

with DAG(
    'ER3_youtube_channels_stats',  # Name of the DAG
    default_args=default_args,
    schedule='0 */12 * * *',  #4 hours
    tags=['youtube'],
    catchup=False # Tag the DAG
) as dag:
   
    def get_channel_id_by_name(channel_name):
        request = youtube.search().list(
            part="snippet",
            q=channel_name,  
            type="channel",  
            maxResults=1      
        )
        response = request.execute()
        
        # Print the channel ID and other details
        if response['items']:
            channel_id = response['items'][0]['id']['channelId']
            channel_title = response['items'][0]['snippet']['channelTitle']
            print(f"Channel Name: {channel_title}")
            print(f"Channel ID: {channel_id}")
            return channel_id
        else:
            print("No channel found with that name.")
            return None
    
    
    @task
    def fetch_channel_stats(channel_names):
        """
        Fetches the latest videos from a given YouTube channel.
        """
        channels = []
        channel_ids = []
        for channel_name in channel_names:
            channel_id = get_channel_id_by_name(channel_name)
            channel_ids.append(channel_id)
        for i in channel_ids:
            channel_ids_str = ",".join(channel_ids)
    
    # Call the API to get details about these channels
        request = youtube.channels().list(
            part="snippet,statistics",
            id=channel_ids_str
        )
        response = request.execute()

        current_time_millis = int(datetime.now().timestamp())
 
            
        for item in response["items"]:
            channel_stats = {
                "channelId": item["id"],
                "channelName": item["snippet"]["title"],
                "description": item["snippet"]["description"],
                "publishedAt": item["snippet"]["publishedAt"],
                "subscriberCount": item["statistics"].get("subscriberCount"),
                "viewCount": item["statistics"].get("viewCount"),
                "videoCount": item["statistics"].get("videoCount"),
                "retrievedAt": current_time_millis 
            }

            channels.append(channel_stats)
        return channels

    @task
    def insert_channel_data(channels: list[dict]):
        """
        Inserts data into Channels and Channel_Statistics tables from the fetched results.
        """
        client = bigquery.Client(project=PROJECT_ID)

        # Prepare data
        channels_table_data = []
        stats_table_data = []

        for item in channels:
            # Static info
            channels_table_data.append({
                "channelId": item["channelId"],
                "channelName": item["channelName"],
                "description": item["description"],
                "publishedAt": item["publishedAt"],
                "retrievedAt": item["retrievedAt"]
            })

            # Time-variant stats
            stats_table_data.append({
                "channelId": item["channelId"],
                "subscriberCount": int(item["subscriberCount"]),
                "viewCount": int(item["viewCount"]),
                "videoCount": int(item["videoCount"]),
                "retrievedAt": item["retrievedAt"]
            })

        # Insert into Channels
        channels_table_id = f"{PROJECT_ID}.{DATASET_ID}.Channels"
        errors_1 = client.insert_rows_json(channels_table_id, channels_table_data)

        # Insert into Channel_Statistics
        stats_table_id = f"{PROJECT_ID}.{DATASET_ID}.Channels_Statistics"
        errors_2 = client.insert_rows_json(stats_table_id, stats_table_data)

        if not errors_1:
            print("✅ Inserted into Channels table")
        else:
            print("❌ Errors inserting into Channels table:", errors_1)

        if not errors_2:
            print("✅ Inserted into Channel_Statistics table")
        else:
            print("❌ Errors inserting into Channel_Statistics table:", errors_2)

    channels_data1 = fetch_channel_stats(["Jackson Wang","Ariana Grande","MrBeast","IShowSpeed","JianHao Tan"])
    insert_channel_data(channels_data1)

