import os

from dotenv import load_dotenv

load_dotenv()


# AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")
RAW_PREFIX = os.getenv("RAW_PREFIX")
PROCESSED_PREFIX = os.getenv("PROCESSED_PREFIX")
LOG_PREFIX = os.getenv("LOG_PREFIX")
ARCHIVE_PREFIX = os.getenv("ARCHIVE_PREFIX")
RAW_FILE = os.getenv("RAW_FILE")

# Spark
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME")

# Redshift
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL")
