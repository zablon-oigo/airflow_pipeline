import boto3
import os
from dotenv import load_dotenv

load_dotenv()

AWS_KEY_ID = os.getenv("AWS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET")
Bucket = os.getenv("Bucket")

def create_bucket():
    try:
        s3 = boto3.client(
            's3',
            region_name='us-east-1',
            aws_access_key_id=AWS_KEY_ID,
            aws_secret_access_key=AWS_SECRET
        )

        response = s3.create_bucket(Bucket=Bucket)

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return f"Bucket '{Bucket}' created successfully."
        else:
            return f"Failed to create bucket '{Bucket}'."
    except Exception as e:
        return f"An error occurred: {e}"

if __name__ == "__main__":
    print(create_bucket())
