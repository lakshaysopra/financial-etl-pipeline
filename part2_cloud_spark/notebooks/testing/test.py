from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Version Check")
    .getOrCreate()
)

print("Spark Version :", spark.version)
print("SparkContext Version :", spark.sparkContext.version)

jvm = spark.sparkContext._jvm

print("Hadoop Version :", jvm.org.apache.hadoop.util.VersionInfo.getVersion())

spark.stop()
