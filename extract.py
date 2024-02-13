import re
import csv
import boto3
import pandas as pd
import googleapiclient.discovery
from googleapiclient.errors import HttpError
from decouple import config


AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY  = config("AWS_SECRET_ACCESS_KEY")

api_key = config("api_key")
youtube_url = "https://www.youtube.com/watch?v=nHkKJ87FS6s&ab_channel=MarquesBrownlee"
csv_filename = "extracted_youtube_data.csv"

fieldnames = ['Video ID', 'Title', 'Like Count', 'Comments Count', 'Comment Text']

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def extract_video_info(youtube_url, api_key):
    # Extract video ID from the YouTube URL
    video_id_match = re.search(r"(?<=v=)[^&]+", youtube_url)
    video_id = video_id_match.group() if video_id_match else None
  
    if video_id:
        # Initialize the YouTube Data API client
        youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

        try:
            # Get video details
            video_response = youtube.videos().list(
                part="snippet,statistics",
                id=video_id
            ).execute()

            # Extract relevant information
            video_info = video_response["items"][0]["snippet"]
            statistics = video_response["items"][0]["statistics"]

            title = video_info["title"]
            like_count = statistics.get("likeCount", 0)
            comments_count = statistics.get("commentCount", 0)

            # Extract video comments
            comments_info = []
            next_page_token = None

            while True:
                comments_response = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    textFormat="plainText",
                    maxResults=100,      # Maximum number of comments per page
                    pageToken = next_page_token,
                    ).execute()

                for item in comments_response["items"]:
                    comment_info = item["snippet"]["topLevelComment"]["snippet"]
                    comment_text = comment_info["textDisplay"]
                    comments_info.append({
                        "comment_text": comment_text
                    })
                # Retrieve the next page token from the response
                next_page_token = comments_response.get('nextPageToken')
                # Break the loop if there are no more pages or a maximum of 500 comments are retrieved
                if not next_page_token or len(comments_info) >= 5000:
                    print(f"\nExtracted Comments Count : {len(comments_info)}")
                    break

            return {
                "video_id": video_id,
                "title": title,
                "like_count": like_count,
                "comments_count": comments_count,
                "comments": comments_info
            }

        except HttpError as e:
            print(f"An error occurred: {e}")
    else:
        print("Invalid YouTube URL. Please provide a valid URL.")

# def save_to_csv(video_info, csv_filename, s3_bucket , s3_key):
#     with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

#         writer.writeheader()

#         for comment in video_info["comments"]:
#             writer.writerow({
#                 'Video ID': video_info["video_id"],
#                 'Title': video_info["title"],
#                 'Like Count': video_info["like_count"],
#                 'Comments Count': video_info["comments_count"],
#                 'Comment Text': comment["comment_text"]
#             })
        

def save_to_csv(video_info, csv_filename):
    data = {
        'Video ID': [video_info["video_id"]] * len(video_info["comments"]),
        'Title': [video_info["title"]] * len(video_info["comments"]),
        'Like Count': [video_info["like_count"]] * len(video_info["comments"]),
        'Comments Count': [video_info["comments_count"]] * len(video_info["comments"]),
        'Comment Text': [comment["comment_text"] for comment in video_info["comments"]]
    }
    df = pd.DataFrame(data)
    # df.to_csv(csv_filename, index=False, encoding='utf-8')
    df.to_csv(f"s3://dipesh-airflow-youtube-bucket/{csv_filename}", index=False, storage_options={
        "key":AWS_ACCESS_KEY_ID,
        "secret" : AWS_SECRET_ACCESS_KEY,
    })


def main():
    video_info = extract_video_info(youtube_url, api_key)
    
    # csv_filename = "airflow/dags/youtube_etl/extracted_youtube_data.csv"
    csv_filename = "extracted_youtube_data.csv"

    s3_bucket = "dipesh-airflow-youtube-bucket"
    s3_key = "s3://dipesh-airflow-youtube-bucket/extracted_youtube_data.csv"

    # Save data to CSV
    saved_data = save_to_csv(video_info, csv_filename)


    if video_info:
        print("Video ID:", video_info["video_id"])
        print("Title:", video_info["title"])
        print("Like Count:", video_info["like_count"])
        print("Comments Count:", video_info["comments_count"])

        # print("\nComments:")
        # for comment in video_info["comments"]:
        #     print("\nComment Text:", comment["comment_text"])
        
        print(f"\nData saved to {csv_filename} in S3 bucket.")
        print("Task Completed")
        return saved_data

# if __name__ == "__main__":
saved_data = main()
