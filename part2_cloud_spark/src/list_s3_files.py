import os
import boto3
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
   )

response = s3.list_objects_v2(
    Bucket = "financial-etl-pipeline-lakshay"
)

for obj in response.get("Contents",[]):
    print(obj["Key"])