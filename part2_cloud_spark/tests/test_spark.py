from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Financial ETL Pipeline")
    .getOrCreate()
)

print("Spark Started Successfully!")
print(spark)

spark.stop()