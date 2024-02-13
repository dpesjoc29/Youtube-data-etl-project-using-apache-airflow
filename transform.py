import re
import pandas as pd
import emoji
import boto3
from decouple import config


AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY  = config("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

def clean_comment(comment):
    # Remove emojis
    # comment = re.sub("[🤣|🤭|😁|❤️|👍|🏴|😣|😠|💪|🙏|😢|🤩|🔥|😭|💯|🏆|😂|💁|🌾|😎|♥|🤷‍♂]+", '', comment)

    # Remove emojis
    # comment = ''.join(c for c in comment if c not in emoji.UNICODE_EMOJI_ALIAS)

    # Convert emojis to text representations and remove them
    comment = emoji.demojize(comment)
    comment = re.sub(":.*?:", "", comment)

    # Remove numbers
    comment = re.sub("[0-9]+", "", comment)

    # Remove special characters
    comment = re.sub(r"[\(\-\”\“\#\!\/\«\»\&\:\@\)\*\.\$\!\?\,\%\"]+", " ", comment)

    # Remove newline characters
    comment = re.sub("\n", " ", comment)

    # Remove additional characters
    comment = re.sub(r'[\'|🇵🇰|\;|\！]+', '', comment)

    # Convert to lowercase
    comment = comment.lower()

    return comment

def transform_and_load_data():
    input_file = r'extracted_youtube_data.csv'
    output_file = r'transformed_youtube_data.csv'

    # Read CSV and transform data
    df = pd.read_csv(input_file)

    # Apply the clean_comment function to the 'Comment Text' column
    df['Comment Text'] = df['Comment Text'].apply(clean_comment)

    # Save the transformed data to a new CSV file
    df.to_csv(output_file, index=False)
    print(f"Transformed data saved to {output_file} in S3 bucket.")
    df.to_csv(f"s3://dipesh-airflow-youtube-bucket/{output_file}", index=False, storage_options={
        "key":AWS_ACCESS_KEY_ID,
        "secret" : AWS_SECRET_ACCESS_KEY,
    })
    # print()

    print("Task Completed. Data is transformed successfully.")

transform_and_load_data()

