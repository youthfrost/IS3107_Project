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
    "retry_delay": timedelta(minutes=15)
}

YOUTUBE_API_KEY  = os.getenv('YOUTUBE_API_KEY')
PROJECT_ID = "aesthetic-nova-454803-r7"

DATASET_ID = "youtube_trending_dataset"

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

with DAG(
    'ER3_trending_daily_google_bq',  # Name of the DAG
    default_args=default_args,
    schedule='0 */11 * * *',  
    tags=['youtube'],
    catchup=False  # Tag the DAG
) as dag:

    @task
    def fetch_music_trending():
        """Fetch trending music videos and return data for Pub/Sub"""
        request = youtube.videos().list(
            part="snippet,statistics",
            chart="mostPopular",
            videoCategoryId="10",  # Music category
            regionCode="US",
            maxResults=10
        )
        response = request.execute()
        print("Successfully got YouTube API")

        current_time_millis = int(datetime.now().timestamp())
        videos = []

        for video in response["items"]:
            publishedAt_str = video["snippet"]["publishedAt"]
            dt = datetime.strptime(publishedAt_str, "%Y-%m-%dT%H:%M:%SZ")
        
            # Replace with UTC timezone info
            dt = dt.replace(tzinfo=timezone.utc)
            # Convert to Unix timestamp in seconds (for BigQuery TIMESTAMP)
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
            print(json.dumps(video_data, indent=2))

            # Add the video data to the list as JSON encoded bytes
            videos.append(video_data) 
            #if we convert to byte here will get Xcom error
            #videos.append(video_data)
        return videos
    
    @task
    def insert_into_bigquery(videos_data):
        """Insert video and related data into multiple BigQuery tables."""
        client = bigquery.Client(project=PROJECT_ID)

        videos_table_id = f"{PROJECT_ID}.{DATASET_ID}.Videos"
        video_stats_table_id = f"{PROJECT_ID}.{DATASET_ID}.Video_Statistics"
        trending_table_id = f"{PROJECT_ID}.{DATASET_ID}.Trending"

        videos = []
        video_stats = []
        trending = []

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
            trending.append({
                "videoId": video["videoId"],
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
        insert_rows( trending, "Trending")
    videos = fetch_music_trending()
    insert_into_bigquery(videos)
