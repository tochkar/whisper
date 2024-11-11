import os
import json
import boto3
from dotenv import load_dotenv

# Load environment variables from a .env file, if you have one
load_dotenv()

# Initialize AWS S3 resources
bucket = os.environ['BUCKET']
s3 = boto3.client(
    's3',
    region_name='eu-west-1',
    aws_access_key_id=os.environ['ACCESS_KEY'],
    aws_secret_access_key=os.environ['SECRET_KEY'],
    aws_session_token=os.environ['SESSION_TOKEN']
)


def upload_file_to_s3(file_path, bucket_name, object_key=None):
    """
    Upload a file to an S3 bucket.
    
    :param file_path: Path to the file you want to upload.
        :param bucket_name: Name of the S3 bucket.
        :param object_key: S3 object name. If not specified, file_path's basename is used.
        :return: None
        """


try:
    if object_key is None:
        object_key = os.path.basename(file_path)

    print(f"Uploading {file_path} to {bucket_name} with key {object_key}...")

    s3.upload_file(file_path, bucket_name, object_key)

    print(f"File {file_path} uploaded successfully as {object_key}")
except Exception as e:
    print(f"Failed to upload file: {e}")

# Example usage
local_file_path = 'rows.csv'  # Replace with your local file path
bucket_name = bucket  # The bucket name from your environment variables
s3_object_key = 'your/key/in/s3.ext'  # Replace with the desired key name in S3

upload_file_to_s3(local_file_path, bucket_name, s3_object_key)
