from pyspark.sql import DataFrame

from config import ( AWS_BUCKET, RAW_PREFIX, RAW_FILE )

def extract_from_s3(spark) -> DataFrame:
    
    s3_path = (f"s3a://{AWS_BUCKET}/{RAW_PREFIX}{RAW_FILE}")

    df = ( spark.read
           .option("header", True) 
           .option("inferSchema", True) 
           .csv(s3_path) ) 

    return df

