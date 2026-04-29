# Imports
import argparse
import os
import sys
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import logging_utils

# Ensure BASE_APP_DIR is set
BASE_APP_DIR = os.getenv("BASE_APP_DIR")
if not BASE_APP_DIR:
    raise EnvironmentError("BASE_APP_DIR environment variable is not set.")

# Parse parameters from env config file
load_dotenv(os.path.join(BASE_APP_DIR, "config/config.env"))

# Resolve variables
BASE_LOG_DIR = os.getenv("BASE_LOG_DIR")
BASE_OUTPUT_DIR = os.getenv("BASE_OUTPUT_DIR")
KMS_CONFIDENTIAL_FPE_PATH = os.getenv("KMS_CONFIDENTIAL_FPE_PATH")
MAPPING_TABLE_PATH = os.getenv("MAPPING_TABLE_PATH")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Ensure required environment variables are set
if not all(
    [
        BASE_LOG_DIR,
        BASE_OUTPUT_DIR,
        KMS_CONFIDENTIAL_FPE_PATH,
        MAPPING_TABLE_PATH,
        WEBHOOK_URL,
    ]
):
    raise EnvironmentError(
        "One or more required environment variables are not set in config.env."
    )

# Replace placeholder in MAPPING_TABLE_PATH
MAPPING_TABLE_PATH = MAPPING_TABLE_PATH.replace("${BASE_OUTPUT_DIR}", BASE_OUTPUT_DIR)

# Setup logger
logger = logging_utils.setup_logger(
    "mapping_csv_to_parquet_incremental",
    os.path.join(BASE_LOG_DIR, "mapping_csv_to_parquet_incremental.log"),
)


def convert_csv_to_parquet(input_file, output_dir, debug):
    """Convert CSV to Parquet with incremental updates."""
    try:
        if debug:
            logging_utils.log_message(logger, "INFO", "Initializing Spark session")

        # Initialize Spark session
        spark = SparkSession.builder.appName("CSV to Parquet Incremental").getOrCreate()
        if debug:
            logging_utils.log_message(logger, "INFO", "Spark session initialized")

        if debug:
            # Verify Spark configuration
            logging_utils.log_message(logger, "INFO", "Spark Configuration:")
            for item in spark.sparkContext.getConf().getAll():
                logging_utils.log_message(logger, "INFO", str(item))

        # Read the CSV file
        logging_utils.log_message(logger, "INFO", f"Reading CSV file: {input_file}")
        new_df = spark.read.csv(input_file, header=True, inferSchema=False)

        # Ensure the columns are treated as strings
        new_df = new_df.withColumn("CTRT_NMB", new_df["CTRT_NMB"].cast("string"))
        new_df = new_df.withColumn(
            "CONTRACT_UID", new_df["CONTRACT_UID"].cast("string")
        )

        # Remove duplicates from the new DataFrame
        new_df = new_df.dropDuplicates(["CTRT_NMB"])

        # Check if the output directory exists and contains Parquet files
        if os.path.exists(output_dir) and any(f.endswith(".parquet") for f in os.listdir(output_dir)):
            logging_utils.log_message(
                logger, "INFO", f"Reading existing Parquet files: {output_dir}"
            )
            existing_df = spark.read.parquet(output_dir)

            # Filter new records that are not in the existing DataFrame
            logging_utils.log_message(
                logger,
                "INFO",
                "Filtering new records that are not in the existing DataFrame",
            )
            new_records_df = new_df.join(existing_df, on=["CTRT_NMB"], how="left_anti")

            # Combine existing records with new records
            combined_df = existing_df.union(new_records_df)

            # Write the combined DataFrame to Parquet, effectively compacting it
            logging_utils.log_message(
                logger, "INFO", "Writing combined DataFrame to Parquet"
            )
            combined_df.repartition(24).write.mode("overwrite").parquet(output_dir)
        else:
            logging_utils.log_message(
                logger, "INFO", "No existing Parquet files found. Writing new data."
            )
            new_df.repartition(24).write.parquet(output_dir, mode="overwrite")

        logging_utils.log_message(
            logger, "INFO", f"Conversion complete. Output directory: {output_dir}"
        )
    except Exception as e:
        error_message = f"Unexpected error during conversion: {str(e)}"
        logging_utils.log_message(logger, "ERROR", error_message)
        sys.exit(1)
    finally:
        if "spark" in locals():
            # Stop the Spark session
            logging_utils.log_message(logger, "INFO", "Stopping Spark session")
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert CSV to Parquet with incremental updates"
    )
    parser.add_argument("input_file", type=str, help="Path to the input CSV file")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    convert_csv_to_parquet(
        args.input_file, output_dir=MAPPING_TABLE_PATH, debug=args.debug
    )
