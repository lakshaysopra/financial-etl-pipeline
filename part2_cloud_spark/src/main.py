import time

from spark_session import create_spark_session
from extract.extract_s3 import extract_from_s3
from transform.explore_data import explore_data
from transform.transform_data import transform_data
from transform.kpi_transformation import calculate_kpis
from validation.validate_data import validate_dataset
from load.write_parquet import write_parquet

from utils.logger import logger
from utils.stage import stage


def main():
    run_start = time.monotonic()
    logger.info("PIPELINE START | pipeline=financial_etl")

    #step 1
    spark = create_spark_session()

    #step 2
    with stage("extract"):
        df = extract_from_s3(spark)

        #step 3 creating cache
        logger.info("Caching extracted DataFrame...")
        df.cache()

        #step 4 materialize the cache
        logger.info("Materializing cache...")
        rows_in = df.count()
    logger.info("EXTRACT OK | rows=%d | cols=%d", rows_in, len(df.columns))

    # Step 5 - Exploration
    with stage("explore"):
        explore_data(df)

    # Step 6 - Cleaning + transformation
    with stage("transform"):
        df = transform_data(df)
        rows_after_transform = df.count()
    logger.info("TRANSFORM OK | rows_in=%d | rows_out=%d | dropped=%d",
                rows_in, rows_after_transform, rows_in - rows_after_transform)

    # Step 7 - KPI calculations
    with stage("kpi"):
        df = calculate_kpis(df)

    # creating cache for validation
    df.cache()
    df.count()

    # Step 8 - Validation phase
    with stage("validation"):
        validate_dataset(df)

    # Step 9 - load parquet to s3
    with stage("load"):
        write_parquet(df)
    logger.info("LOAD OK | rows=%d | mode=overwrite", rows_after_transform)

    # # Review
    # df.show(100, truncate=False)

    logger.info("PIPELINE SUCCESS | rows_read=%d | rows_written=%d | duration_s=%.1f",
                rows_in, rows_after_transform, time.monotonic() - run_start)

    #step 10
    logger.info("Stopping Spark session...")
    spark.stop()


if __name__ =="__main__":
    main()