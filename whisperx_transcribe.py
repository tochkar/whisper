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
    """Returns a specific MP3 file from the S3 bucket."""
    return ['in/2024/08/07/2024-08-07_00-00-45_135_80293353525_2_9d735cbc-0d63-47b1-aa75-902c7de32202.mp3']

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
                             "The text is in Russian. Streets and house numbers in Minsk may be recognized with errors. "
                             "Try to correct the mistakes based on Minsk street names and house numbers. "
                             "Your task is to identify the street and house number accurately.")},
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
