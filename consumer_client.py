import os
import json
import base64
import boto3
import csv
from io import StringIO
from datetime import datetime
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    region_name='us-east-1',
    aws_access_key_id=os.getenv("AWS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET")
)
BUCKET = os.getenv("Bucket")

