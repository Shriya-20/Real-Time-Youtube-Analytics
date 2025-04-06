<<<<<<< HEAD

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
=======

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
>>>>>>> b5c6f8ab4f97b85eaebc06331846aeaff61a9892
print(get_trending_videos()['items'][1]['snippet']['title'])