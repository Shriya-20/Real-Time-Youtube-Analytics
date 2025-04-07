
"""

#testing the api

from googleapiclient.discovery import build

API_KEY = "AIzaSyDii0e5q-U-YI_bqRghuaxpq1BGX0J20Zg"
youtube = build('youtube', 'v3', developerKey=API_KEY)

# part="snippet,statistics": This specifies that the response should include both the video snippet (title, description, thumbnails, etc.) and statistics (like views, likes, dislikes, etc.).
# chart="mostPopular": This specifies that the videos should be retrieved based on their popularity.
# regionCode="IN": This specifies that the videos should be retrieved from India.
#  function then executes the request and returns the response from the API
def get_trending_videos():
    request = youtube.videos().list(
        part="snippet,statistics",
        chart="mostPopular",
        regionCode="IN",
        maxResults=10
    )
    return request.execute()

# Test the API
print(get_trending_videos()['items'][0]['snippet']['title'])

"""

import os
import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

# Configuration
API_KEY = "AIzaSyDii0e5q-U-YI_bqRghuaxpq1BGX0J20Zg"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "trending_videos"
REGION_CODE = "IN"
MAX_RESULTS = 50  # Max allowed by YouTube API

def get_trending_videos():
    """Fetch trending videos from YouTube API for India region"""
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": REGION_CODE,
        "maxResults": MAX_RESULTS,
        "key": API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching trending videos: {e}")
        return None

def process_video_data(video_data):
    """Process and transform video data into structured format"""
    processed_videos = []
    
    for item in video_data.get("items", []):
        video = {
            "video_id": item.get("id"),
            "title": item.get("snippet", {}).get("title"),
            "channel_id": item.get("snippet", {}).get("channelId"),
            "channel_title": item.get("snippet", {}).get("channelTitle"),
            "category_id": item.get("snippet", {}).get("categoryId"),
            "publish_time": item.get("snippet", {}).get("publishedAt"),
            "view_count": int(item.get("statistics", {}).get("viewCount", 0)),
            "like_count": int(item.get("statistics", {}).get("likeCount", 0)),
            "comment_count": int(item.get("statistics", {}).get("commentCount", 0)),
            "duration": item.get("contentDetails", {}).get("duration"),
            "tags": item.get("snippet", {}).get("tags", []),
            "thumbnail_url": item.get("snippet", {}).get("thumbnails", {}).get("high", {}).get("url"),
            "fetch_time": datetime.now().isoformat()
        }
        processed_videos.append(video)
    
    return processed_videos

def send_to_kafka(producer, videos):
    """Send processed video data to Kafka topic"""
    for video in videos:
        try:
            producer.send(
                KAFKA_TOPIC,
                value=json.dumps(video).encode('utf-8'),
                key=video["video_id"].encode('utf-8')
            )
            print(f"Sent video {video['video_id']} to Kafka")
        except Exception as e:
            print(f"Error sending to Kafka: {e}")

def main():
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks='all',
        retries=3
    )
    
    try:
        # Fetch trending videos
        video_data = get_trending_videos()
        if not video_data:
            print("No data received from YouTube API")
            return
        
        # Process video data
        processed_videos = process_video_data(video_data)
        print(f"Processed {len(processed_videos)} trending videos")
        
        # Send to Kafka
        send_to_kafka(producer, processed_videos)
        
    finally:
        # Clean up
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()