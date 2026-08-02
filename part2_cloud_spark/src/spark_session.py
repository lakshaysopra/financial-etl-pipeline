from pyspark.sql import SparkSession

from config import (
     SPARK_APP_NAME,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION
)

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)

        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2"
        )


        .config(
            "spark.hadoop.fs.s3a.access.key",
            AWS_ACCESS_KEY_ID
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            AWS_SECRET_ACCESS_KEY
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{AWS_REGION}.amazonaws.com"
        )

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .getOrCreate()
    )

    return spark
