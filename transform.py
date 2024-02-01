import re
import pandas as pd
import emoji


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

    print(f"Transformed data saved to {output_file}.")
    print("Task Completed. Data is transformed successfully.")

transform_and_load_data()

