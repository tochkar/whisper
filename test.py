import csv
import os
import re
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
s3_res = boto3.resource('s3')


def list_s3_files():
    print(1)
    """Lists MP3 files available in the S3 bucket."""
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix='in/2024/08/01'):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.mp3'):
                    files.append(obj['Key'])
                break
        break
    print(files)
    return files


def process_file(file):
    """Processes a single MP3 file for transcription."""
    print("Processing", file)
    model_type_needed = 'large-v2'
    language = 'ru'
    local_file_path = 'audio/' + os.path.basename(file)

    try:
        s3.download_file(bucket, file, local_file_path)
    except Exception as e:
        print(f"Failed to download file {file}: {e}. Skipping.")
        return

    print(f"Using model {model_type_needed} and language {language}")
    transcription = wt.transcribe(local_file_path, model_type_needed, language=language)

    json_output = json.dumps(transcription, ensure_ascii=False, indent=4)

    # Print the JSON output
    print(json_output)

    transcript = json.loads(json_output)

    api_key = os.environ['OPENAI_API_KEY']

    if not api_key:
        api_key = input("Enter your OpenAI API key: ")

    client = OpenAI(api_key=api_key)

    phrases = ' '.join(segment['phrase'] for segment in transcript)

    # Prepare the request to OpenAI
    response = client.chat.completions.create(
        model='gpt-4o',
        messages=[
            {"role": "system",
             "content": "You are to analyze a transcript and extract key features with labels. Language: Russian. When you recognise any type of location try to recognise name of Minsk city street correctly. In input text location can be with mistake. Respond in Markdown. Write notes in russian language in the of response must be only text in format :  имя_клиента;адрес_посадки;подъезд;адрес_назначения;стоимость;детское_кресло;отправлено_ли_такси"},
            {"role": "user", "content": f"The following is a series of phrases from a transcript:\n{phrases}"}
        ],
        temperature=0,
    )

    # Print out the response from GPT-4
    print(response.choices[0].message.content)

    output = response.choices[0].message.content.strip()
    data = output.split(';')

    # CSV output headers
    headers = [
        "имя_клиента",
        "адрес_посадки",
        "подъезд",
        "адрес_назначения",
        "стоимость",
        "детское_кресло",
        "отправлено_такси"
    ]

    # Create a CSV file in the output directory
    csv_filename = 'output/transcript_data.csv'

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(csv_filename), exist_ok=True)

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';')

        # Write the header
        writer.writerow(headers)

        # Write the data row
        writer.writerow(data)

    # Print confirmation
    print(f"Data written to CSV file: {csv_filename}")


# Save the transcription result to the output path in the S3 bucket
# output_path = f'output/{os.path.basename(file)}.txt'
# s3_res.Object(bucket, output_path).put(Body=json.dumps(transcription))

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
