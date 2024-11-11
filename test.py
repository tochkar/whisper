import csv
import os
import json
import boto3
from dotenv import load_dotenv
from filelock import FileLock, Timeout
import whisperx_transcribe as wt
from openai import OpenAI

# Load environment variables
load_dotenv()

# Initialize AWS S3 resources
bucket = os.environ['BUCKET']
s3 = boto3.client('s3', region_name='eu-west-1', aws_access_key_id=os.environ['ACCESS_KEY'],
                  aws_secret_access_key=os.environ['SECRET_KEY'], aws_session_token=os.environ['SESSION_TOKEN'])

def list_s3_files():
    """Lists up to 5 MP3 files available in the S3 bucket."""
    print(1)
    files = []
    files_needed = 100  # Number of files to retrieve
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix='in/2024/08/06'):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.mp3'):
                    files.append(obj['Key'])
                    if len(files) >= files_needed:
                        break  # Stop if we've collected the desired number of MP3s
        if len(files) >= files_needed:
            break  # Ensure to exit if the limit is reached across pages
    print(files)
    return files
   

def process_file(file, rows, csv_filename):
    """Processes a single MP3 file and updates the corresponding row in the CSV if phone matches."""
    print("Processing", file)
    model_type_needed = 'large-v2'
    language = 'ru'
    local_file_path = 'audio/' + os.path.basename(file)
    try:
        s3.download_file(bucket, file, local_file_path)
    except Exception as e:
        print(f"Failed to download file {file}: {e}. Skipping.")
        return
    try:
        print(f"Using model {model_type_needed} and language {language}")
        transcription = wt.transcribe(local_file_path, model_type_needed, language=language)
        transcript = json.loads(json.dumps(transcription, ensure_ascii=False))
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            api_key = input("Enter your OpenAI API key: ")
        client = OpenAI(api_key=api_key)
        phrases = ' '.join(segment['phrase'] for segment in transcript)
        # Prepare the request to OpenAI
        response = client.chat.completions.create(
            model='gpt-4o',
            messages=[
                {"role": "system",
                 "content": ("You are to extract only the pick-up address from the transcript. "
                             "The text is in Russian. "
                             "Your task is to identify the street and house number accurately..")},
                {"role": "user", "content": f"The following is a series of phrases from a transcript:\n{phrases}"}
            ],
            temperature=0,
        )
        # Extract the pick-up address from GPT response
        gpt_response = response.choices[0].message.content.strip()
        # Extract phone number from the filename
        basename = os.path.basename(file)
        parts = basename.split('_')
        phone = parts[3].strip()  # Assuming phone is the fourth part
        
        # Check and replace phone number format if needed
        if phone.startswith('80'):
            phone = '375' + phone[2:]

        # Update the CSV row if phone matches
        data_updated = False
        for row in rows:
            if row[1] == phone:
                row.append(gpt_response)
                data_updated = True
                break  # Assuming only one match per file

        if data_updated:
            with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerows(rows)
            print("CSV file updated with new data.")
    except Exception as e:
        print(f"Failed to process file {file}: {e}")
    finally:
        # Attempt to remove the local file regardless of success
        try:
            os.remove(local_file_path)
            print(f"Removed local file: {local_file_path}")
        except Exception as e:
            print(f"Failed to remove file {local_file_path}: {e}")

def main():
    """Main function to process a specific MP3 file using Whisper and update the CSV."""
    csv_filename = 'rows.csv'
    rows = []
    
    # Read existing CSV rows
    try:
        with open(csv_filename, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            rows = list(reader)
    except Exception as e:
        print(f"Could not read CSV file {csv_filename}: {e}")
        return
    
    for file in list_s3_files():
        process_file(file, rows, csv_filename)
    
    print("Done")

if __name__ == "__main__":
    try:
        with FileLock("/tmp/transcribe.lock", timeout=3):
            main()
    except Timeout:
        print("Another instance of this script is running. Exiting.")
