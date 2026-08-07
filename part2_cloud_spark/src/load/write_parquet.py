from config.config import (
    AWS_BUCKET,
    PROCESSED_PREFIX
)

from utils.logger import logger


def write_parquet(df):

    processed_data_path = (f"s3a://{AWS_BUCKET}/{PROCESSED_PREFIX}company_financials/")

    logger.info("=" * 60)
    logger.info("WRITING PARQUET FILES TO AMAZON S3")
    logger.info("=" * 60)

    try:

        (
            df.write
              .mode("overwrite")
              .parquet(processed_data_path)
        )

        logger.info(
            f"Parquet files written successfully to {processed_data_path}"
        )

        return processed_data_path

    except Exception :

        logger.exception(
            "Failed to write Parquet files."
        )

        raise 