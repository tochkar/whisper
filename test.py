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

def get_phone_numbers_from_csv(csv_filename, limit=None):
    """Extract phone numbers from column B of the CSV file and remove the '375' prefix, limiting to a specified number of entries."""
    phone_numbers = []
    try:
        with open(csv_filename, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header, if present
            for row in reader:
                if len(row) > 1 and row[1].startswith('375'):
                    phone_numbers.append(row[1][3:])  # Remove the '375' prefix
                if limit and len(phone_numbers) >= limit:
                    break
    except Exception as e:
        print(f"Failed to read CSV file {csv_filename}: {e}")
    return phone_numbers

def list_all_files_in_s3(bucket_name, prefix):
    """List all MP3 files within and beyond a given S3 prefix."""
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.mp3'):
                    files.append(obj['Key'])
    return files

def process_file(file, phone, rows, csv_filename):
    """Process the MP3 file and update the corresponding row in the CSV."""
    print("Processing file", file)
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
        client = OpenAI(api_key=api_key)
        phrases = ' '.join(segment['phrase'] for segment in transcript)
        
        # Prepare the request to OpenAI
        response = client.chat.completions.create(
            model='gpt-4o',
            messages=[
                {"role": "system",
                 "content": ("Extract only the pick-up address from the transcript. "
                             "The text is in Russian. "
                             "Identify the street and house number accurately.")},
                {"role": "user", "content": f"The following is a series of phrases from a transcript:\n{phrases}"}
            ],
            temperature=0,
        )
        
        # Extract the pick-up address from the GPT response
        gpt_response = response.choices[0].message.content.strip()
        
        # Update the CSV row if the phone matches and column G is empty
        data_updated = False
        for row in rows:
            if len(row) > 1 and row[1] == '375' + phone and (len(row) < 7 or not row[6]):
                while len(row) <= 6:  # Ensure there are enough columns
                    row.append('')
                row[6] = gpt_response
                data_updated = True
                break
        
        if data_updated:
            with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerows(rows)
            print("CSV file updated with new data.")
    
    except Exception as e:
        print(f"Failed to process file {file}: {e}")
    
    finally:
        # Attempt to remove the local file regardless of how processing went
        try:
            os.remove(local_file_path)
            print(f"Removed local file: {local_file_path}")
        except Exception as e:
            print(f"Failed to remove file {local_file_path}: {e}")

def main():
    """Main function to process MP3 files using Whisper and update the CSV."""
    csv_filename = 'rows.csv'
    rows = []
    number_of_phones_to_process = 10  # Set the desired number of phone numbers to process
    
    # Read existing CSV rows
    try:
        with open(csv_filename, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            rows = list(reader)
    except Exception as e:
        print(f"Failed to read CSV file {csv_filename}: {e}")
        return
    
    phone_numbers = get_phone_numbers_from_csv(csv_filename, limit=number_of_phones_to_process)
    all_s3_files = list_all_files_in_s3(bucket, 'in/2024/08/')

    for phone in phone_numbers:
        for s3_file in all_s3_files:
            if phone in s3_file:
                process_file(s3_file, phone, rows, csv_filename)
                break  # Stop searching if the file with this phone number is processed
    
    print("Done")

if __name__ == "__main__":
    try:
        with FileLock("/tmp/transcribe.lock", timeout=3):
            main()
    except Timeout:
        print("Another instance of this script is running. Exiting.")
