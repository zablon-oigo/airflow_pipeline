import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")

def load_csv_from_s3_to_snowflake():
    ctx = None
    cs = None
    try:
        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cs = ctx.cursor()

        print("Checking files in S3 stage...")
        cs.execute(f"LIST @{SNOWFLAKE_STAGE}")
        for row in cs.fetchall():
            print(row[0])

        print(f"\nLoading data into {SNOWFLAKE_TABLE}...")
        cs.execute(f"""
            COPY INTO {SNOWFLAKE_TABLE}
            FROM @{SNOWFLAKE_STAGE}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
            PATTERN='.*\\.csv'
            ON_ERROR = 'CONTINUE'
        """)
        print("Data loaded successfully.")

