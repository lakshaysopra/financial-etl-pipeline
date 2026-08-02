from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, count

from utils.logger import logger


def show_shape(df):
    rows = df.count()
    cols = len(df.columns)

    logger.info("=" * 60)
    logger.info("DATASET SHAPE")
    logger.info(f"Rows    : {rows}")
    logger.info(f"Columns : {cols}")


def show_schema(df):
    logger.info("=" * 60)
    logger.info("DATASET SCHEMA")
    df.printSchema()


def show_columns(df):
    logger.info("=" * 60)
    logger.info("COLUMN NAMES")

    for col in df.columns:
        logger.info(col)


def analyze_nulls(df):
   
    logger.info("=" * 60)
    logger.info("NULL VALUE ANALYSIS")
    logger.info("Displaying null counts for each column")
    null_values_df = df.select([
        count(
            when(col(column).isNull(), column)
        ).alias(column)

        for column in df.columns
    ])

    null_values_df.show(truncate=False)
       


def analyze_duplicate_rows(df):
    logger.info("=" * 60)
    logger.info("DUPLICATE ROW ANALYSIS")

    total_rows = df.count()

    distinct_rows = df.distinct().count()

    duplicates = total_rows - distinct_rows

    logger.info(f"Duplicate Rows : {duplicates}")


def analyze_companies(df):
    logger.info("=" * 60)
    logger.info("COMPANY ANALYSIS")

    companies = df.select("company_name").distinct().count()

    logger.info(f"Unique Companies : {companies}")


def analyze_metrics(df):
    logger.info("=" * 60)
    logger.info("METRIC ANALYSIS")

    metrics = df.select("Unnamed: 0").distinct()

    metrics.show(truncate=False)


def analyze_grain(df):
    logger.info("=" * 60)
    logger.info("DATA GRAIN")

    df.select(
        "company_name",
        "Unnamed: 0"
    ).show(25, truncate=False)


def analyze_company_metric_count(df):
    logger.info("=" * 60)
    logger.info("METRICS PER COMPANY")

    (
        df.groupBy("company_name")
          .count()
          .groupBy("count")
          .count()
          .show()
    )


############################################################
# Main Exploration Function
############################################################

def explore_data(df):

    show_shape(df)

    show_schema(df)

    show_columns(df)

    analyze_nulls(df)

    analyze_duplicate_rows(df)

    analyze_companies(df)

    analyze_metrics(df)

    analyze_grain(df)

    analyze_company_metric_count(df)