import os
import re
import json
import boto3
from dotenv import load_dotenv
from filelock import FileLock, Timeout
import whisperx_transcribe as wt

# Load environment variables
load_dotenv()

# Initialize AWS S3 resources
bucket = os.environ['BUCKET']
s3 = boto3.client('s3', region_name='us-east-1')
s3_res = boto3.resource('s3')


def list_s3_files():
    """Lists MP3 files available in the S3 bucket."""
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix='input/'):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.mp3'):
                    files.append(obj['Key'])
        break
    return files

def process_file(file):
    """Processes a single MP3 file for transcription."""
    print("Processing", file)
    model_type_needed = 'large-v3'
    language = 'ru'
    local_file_path = 'audio/' + os.path.basename(file)

    try:
        s3.download_file(bucket, file, local_file_path)
    except Exception as e:
        print(f"Failed to download file {file}: {e}. Skipping.")
        return

    # print(f"Using model {model_type_needed} and language {language}")
    # transcription = wt.transcribe(local_file_path, model_type_needed, language=language)
    #
    # # Save the transcription result to the output path in the S3 bucket
    # output_path = f'output/{os.path.basename(file)}.txt'
    # s3_res.Object(bucket, output_path).put(Body=json.dumps(transcription))
    #
    # # Optionally, delete the input file after processing if needed
    # s3_res.Object(bucket, file).delete()

def main():
    """Main function to process MP3 files using Whisper."""
    for file in list_s3_files():
        process_file(file)
    print("Done")

if __name__ == "__main__":
    try:
        with FileLock("/tmp/transcribe.lock", timeout=3):
            main()
    except Timeout:
        print("Another instance of this script is running. Exiting.")
