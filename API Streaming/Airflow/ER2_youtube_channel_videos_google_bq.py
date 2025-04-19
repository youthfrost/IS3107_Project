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
    "retry_delay": timedelta(minutes=10)
}


YOUTUBE_API_KEY  = os.getenv('YOUTUBE_API_KEY')
PROJECT_ID = "aesthetic-nova-454803-r7"
DATASET_ID = "youtube_trending_dataset"

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

with DAG(
    'ER3_youtube_channel_videos',  # Name of the DAG
    default_args=default_args,
    schedule='0 */4 * * *',  #4 hours
    tags=['youtube'],
    catchup=False   # Tag the DAG
) as dag:
   
    def get_channel_id_by_name(channel_name):
        request = youtube.search().list(
            part="snippet",
            q=channel_name,  # Jackson Wang's name to search for
            type="channel",  # Only search for channels
            maxResults=1      # Get the first result (assuming it's his channel)
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



    def fetch_video_stats(video_ids,channel_id):
        videos = []
        for video_id in video_ids:
            response = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
                ).execute()
            video  =  response["items"][0]
            current_time_millis = int(datetime.now().timestamp())
            publishedAt_str = video["snippet"]["publishedAt"]
            dt = datetime.strptime(publishedAt_str, "%Y-%m-%dT%H:%M:%SZ")
            dt = dt.replace(tzinfo=timezone.utc)
            timestamp_seconds = int(dt.timestamp())
            video_data = {
                "videoId": video["id"],
                "title": video["snippet"]["title"],
                "publishedAt": timestamp_seconds,
                "channelName": video["snippet"]["channelTitle"],
                "viewCount": video["statistics"].get("viewCount", "0"),
                "likeCount": video["statistics"].get("likeCount", "0"),
                "commentCount": video["statistics"].get("commentCount", "0"),
                "tags": video["snippet"].get("tags", []),
                "categoryId": video["snippet"]["categoryId"],
                "retrievedAt": current_time_millis 
            }
            
            videos.append(video_data) 
            #if we convert to byte here will get Xcom error
            #videos.append(video_data)
        return videos
    
    @task
    def fetch_latest_videos(channel_name, max_results):
        """
        Fetches the latest videos from a given YouTube channel.
        """
        channel_id = get_channel_id_by_name(channel_name)
        print("searchinggg")
        search_request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            maxResults=max_results,
            order="date",
            type="video"
        )
        search_response = search_request.execute()
        print("search_response")

        # Extract video IDs
        video_ids = [item["id"]["videoId"] for item in search_response["items"]]
        print(video_ids)
        videos = fetch_video_stats(video_ids,channel_id)
        return videos


    @task
    def insert_into_bigquery(videos_data):
        """Insert video and related data into multiple BigQuery tables."""
        client = bigquery.Client(project=PROJECT_ID)

        videos_table_id = f"{PROJECT_ID}.{DATASET_ID}.Videos"
        video_stats_table_id = f"{PROJECT_ID}.{DATASET_ID}.Video_Statistics"

        videos = []
        video_stats = []

        for video in videos_data:

            # Videos
            videos.append({
                "videoId": video["videoId"],
                "channelName": video["channelName"],
                "title": video["title"],
                "publishedAt": video["publishedAt"],
                "categoryId": video["categoryId"],
                "tags": video["tags"],
                "retrievedAt": video["retrievedAt"]
            })

            # Video stats
            video_stats.append({
                "videoId": video["videoId"],
                "viewCount": int(video["viewCount"]),
                "likeCount": int(video["likeCount"]),
                "commentCount": int(video["commentCount"]),
                "retrievedAt": video["retrievedAt"]
            })

        # Insert into BigQuery
        def insert_rows( rows, name):
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{name}"
            if rows:
                errors = client.insert_rows_json(table_id, list(rows.values()) if isinstance(rows, dict) else rows)
                if not errors:
                    print(f"✅ Inserted into {name}")
                else:
                    print(f"❌ Errors inserting into {name}:", errors)

        insert_rows( videos, "Videos")
        insert_rows( video_stats, "Videos_Statistics")


    videos_data = fetch_latest_videos("Jackson Wang",5)
    insert_into_bigquery(videos_data)
    videos_data = fetch_latest_videos("Ariana Grande",5)
    insert_into_bigquery(videos_data)
