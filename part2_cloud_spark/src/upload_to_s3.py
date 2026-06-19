import os
import boto3
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent/".env")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

local_file = r"C:\Users\Lakshay Sopra\financial-etl-pipeline\part1_pandas\data\raw\financials.csv"

bucket_name = "financial-etl-pipeline-lakshay"

s3_key = "raw/financials.csv"

s3.upload_file(
    local_file,
    bucket_name,
    s3_key
)

print("file uploaded successfully!")