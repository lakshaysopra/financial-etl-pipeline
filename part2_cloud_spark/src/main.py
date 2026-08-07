from spark_session import create_spark_session
from extract.extract_s3 import extract_from_s3
from transform.explore_data import explore_data
from transform.transform_data import transform_data
from transform.kpi_transformation import calculate_kpis
from validation.validate_data import validate_dataset
from load.write_parquet import write_parquet

from utils.logger import logger

def main():
    #step 1
    spark = create_spark_session()

    #step 2
    df = extract_from_s3(spark)

    #step 3 creating cache
    logger.info("Caching extracted DataFrame...")
    df.cache()

    #step 4 materialize the cache
    logger.info("Materializing cache...")
    df.count()

    # Step 5 - Exploration
    explore_data(df)

    # Step 6 - Cleaning + transformation
    df = transform_data(df)

    # Step 7 - KPI calculations
    df = calculate_kpis(df)
    

    # Step 8 - Validation phase
    # validate_dataset(df)

    # Step 9 - laod parquet to s3

    write_parquet(df)

    # # Review
    # df.show(100, truncate=False)
    
    #step 10
    logger.info("Stopping Spark session...")
    spark.stop()


if __name__ =="__main__":
    main()

