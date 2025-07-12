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

def upload_to_s3_csv(batch_data, batch_num):
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=batch_data[0].keys())
    writer.writeheader()
    writer.writerows(batch_data)

    now = datetime.utcnow()
    folder = now.strftime('%Y/%m/%d')
    filename = f"{folder}/stocks_batch_{batch_num}.csv"

    s3.put_object(
        Bucket=BUCKET,
        Key=filename,
        Body=csv_buffer.getvalue()
    )
    print(f"Uploaded batch {batch_num} to {filename}")


def start_consumer():
    consumer = Consumer({
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv("KAFKA_API_KEY"),
        'sasl.password': os.getenv("KAFKA_API_SECRET"),
        'group.id': 'finance-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['finance1'])

