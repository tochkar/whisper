import csv
import os
import json
import boto3
from dotenv import load_dotenv

bucket = os.environ['BUCKET']
s3 = boto3.client('s3', region_name='eu-west-1', aws_access_key_id=os.environ['ACCESS_KEY'],
                  aws_secret_access_key=os.environ['SECRET_KEY'], aws_session_token=os.environ['SESSION_TOKEN'])


s3.download_file(bucket, 'order.csv', 'rows')
