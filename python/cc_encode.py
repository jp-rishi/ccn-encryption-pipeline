# Imports
import argparse
import json
import os
import re
import shutil
import sys
import time
import logging
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
from urllib3.util import Retry
import hvac
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    arrays_zip,
    coalesce,
    col,
    collect_list,
    concat,
    concat_ws,
    explode,
    expr,
    length,
    lit,
    monotonically_increasing_id,
    regexp_extract_all,
    regexp_replace,
    row_number,
    udf,
    when,
)
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window
import logging_utils

# Ensure BASE_APP_DIR is set. PATH of the folder containing all the scripts for this workflow.
BASE_APP_DIR = os.getenv("BASE_APP_DIR")
if not BASE_APP_DIR:
    raise EnvironmentError("BASE_APP_DIR environment variable is not set.")

# Parse parameters from env config file
load_dotenv(os.path.join(BASE_APP_DIR, "config/config.env"))

# Resolve variables
BASE_LOG_DIR = os.getenv("BASE_LOG_DIR")
BASE_OUTPUT_DIR = os.getenv("BASE_OUTPUT_DIR")
BASE_TEMP_DIR = os.getenv("BASE_TEMP_DIR")
KMS_CONFIDENTIAL_FPE_PATH = os.getenv("KMS_CONFIDENTIAL_FPE_PATH")
MAPPING_TABLE_PATH = os.getenv("MAPPING_TABLE_PATH")

# Ensure required environment variables are set
required_env_vars = [
    BASE_LOG_DIR,
    BASE_OUTPUT_DIR,
    BASE_TEMP_DIR,
    KMS_CONFIDENTIAL_FPE_PATH,
    MAPPING_TABLE_PATH
]
if not all(required_env_vars):
    raise EnvironmentError(
        "One or more required environment variables are not set in config.env."
    )

# Replace placeholder in MAPPING_TABLE_PATH
MAPPING_TABLE_PATH = MAPPING_TABLE_PATH.replace("${BASE_OUTPUT_DIR}", BASE_OUTPUT_DIR)

# Define the regex pattern for extracting credit card numbers
CCN_PATTERN = (
    r"(?<!\d)(0{0,2}\d{14,16}|0{0,2}\d{4}[-\s]\d{4}[-\s]\d{4}[-\s]\d{2,4})(?!\d)"
)
PATTERN = f"(^|.+?)({CCN_PATTERN}|$)"

# Define a constant for the window specification
WINDOW_ORDER = 1

# Initialize a default logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("default")

def requests_retry_session(
    retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None
):
    """Function to retry session"""
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Function for logging detected cc numbers in a dedicated file
def log_error(message, detected_ccnum_log_file):
    """
    Log a message to the detected ccnum log file.
    """
    os.makedirs(os.path.dirname(detected_ccnum_log_file), exist_ok=True)
    with open(detected_ccnum_log_file, mode="a", encoding="utf-8") as log_file:
        log_file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def load_vault_config(config_path):
    """Load Vault configuration from JSON file"""
    with open(config_path, encoding="utf-8") as f:
        vault_config = json.load(f)
        return {
            "url": vault_config["url"],
            "role_id": vault_config["role_id"],
            "secret_id": vault_config["secret_id"],
            "verify_ssl": vault_config.get("verify_ssl", "true").lower() == "true",
        }

# Function for authenticating with Hashicorp Vault and generating token
def authenticate_vault(vault_url, vault_role_id, vault_secret_id, verify_ssl=True):
    """
    Authenticate with Vault using AppRole and return the client token.
    """
    auth_url = f"{vault_url}/v1/auth/approle/login"
    auth_payload = {"role_id": vault_role_id, "secret_id": vault_secret_id}
    try:
        session = requests_retry_session()
        auth_response = session.post(auth_url, json=auth_payload, verify=verify_ssl)
        auth_response.raise_for_status()
        return auth_response.json()["auth"]["client_token"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to authenticate with Vault:  {e}")
        sys.exit(1)

# Function for encoding 16 digit numbers using Hashicorp Vault's FPE transform method
def batch_transform_credit_card_numbers(client, numbers, debug):
    """
    Transform a batch of credit card numbers using Vault's transform secrets engine.
    """
    try:
        chunk_size = 100000
        encoded_numbers = []

        for i in range(0, len(numbers), chunk_size):
            chunk = numbers[i : i + chunk_size]
            # if debug:
            #     logger.info(f"Sending chunk to Vault for encoding: {chunk}")
            response = client.secrets.transform.encode(
                role_name="transform-role-fpe",
                transformation="contract-num-fpe",
                batch_input=[{"value": number} for number in chunk],
            )
            # if debug:
            #     logger.info(f"Received response from Vault: {response}")
            if "data" in response and "batch_results" in response["data"]:
                for number, item in zip(chunk, response["data"]["batch_results"]):
                    if "encoded_value" in item:
                        encoded_numbers.append(item["encoded_value"])
                    else:
                        logger.error(
                            f"Missing 'encoded_value' for input number: {number}, response item: {item}"
                        )
                        raise ValueError(
                            f"Missing 'encoded_value' for input number: {number}"
                        )
            else:
                logger.error(f"Unexpected response format: {response}")
                raise ValueError("Unexpected response format from Vault")
        return encoded_numbers
    except hvac.exceptions.VaultError as e:
        logger.error(f"Vault error: {e}")
        raise
    except KeyError as e:
        logger.error(f"KeyError: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception: {e}")
        raise


# Utility Functions
def first_element(array):
    """
    Return the first element of an array, or None if the array is empty.
    """
    if array:
        return array[0]
    return None


def luhn_checksum(card_number):
    """
    Function for validating numbers using Luhn's Algorithm.
    https://en.wikipedia.org/wiki/Luhn_algorithm
    Our main use case is to detect 14-16 digit valid credit card numbers in our data
    but Luhn's Algorithm also validates other numbers such as IMEI numbers,
    National Provider Identifier numbers in the United States, Social Security numbers etc.
    """
    if not card_number.isdigit():
        return False
    digits = [int(d) for d in card_number]
    odd_digits = digits[-1::-2]
    even_digits = digits[-2::-2]
    checksum = sum(odd_digits)
    for d in even_digits:
        checksum += sum([int(d) for d in str(d * 2)])
    return checksum % 10 == 0


# Main Processing Functions
def check_for_raw_cc_numbers(df, skip_check_columns, detected_ccnum_log_file):
    """Function to check presence of valid credit card numbers in our data"""
    logger.info("Checking the final df for raw credit card numbers.")
    columns_to_check = [
        column for column in df.columns if column not in skip_check_columns
    ]

    # UDF to validate credit card numbers using the Luhn algorithm
    validate_udf = udf(lambda x: luhn_checksum(x) if x else False, BooleanType())

    detected_columns = set()

    for column in columns_to_check:
        try:
            start_col_check_time = time.time()

            # Extract patterns and text parts into columns
            df_reg = df.select(
                "*",  # Select all original columns
                regexp_extract_all(col(column), lit(PATTERN), 0).alias("match"),
                regexp_extract_all(col(column), lit(PATTERN), 1).alias("other_text"),
                regexp_extract_all(col(column), lit(PATTERN), 2).alias(
                    "extracted_number"
                ),
            ).withColumn(
                "zipped", arrays_zip(col("other_text"), col("extracted_number"))
            )

            # Define window specification for order_id order by constant one
            window_spec_order = Window.orderBy(lit(WINDOW_ORDER))
            # Explode the zipped list into separate rows for easier processing
            df_exploded = df_reg.select(
                "*", explode("zipped").alias("exploded")
            ).select(
                "*",
                col("exploded.other_text").alias("exploded_other_text"),
                col("exploded.extracted_number").alias("number_raw"),
                regexp_replace(col("exploded.extracted_number"), r"[-\s]", "").alias(
                    "cleaned_number"
                ),
                row_number().over(window_spec_order).alias(
                    "order_id"
                ),  # Add order_id to preserve the order
            )

            # Filter out numbers that are not 14 to 16 digits long
            df_exploded = df_exploded.filter(
                length(col("cleaned_number")).between(14, 16)
            )

            # Validate the cleaned matches
            df_exploded = df_exploded.withColumn(
                "is_valid_ccn", validate_udf(col("cleaned_number"))
            )
            df_valid_ccn = df_exploded.filter(col("is_valid_ccn"))

            if df_valid_ccn.count() > 0:
                detected_rows = df_valid_ccn.collect()
                for row in detected_rows:
                    detected_number = row["number_raw"]
                    log_message = f"Detected raw credit card number in column '{column}': {detected_number}\nRow: {row.asDict()}"
                    log_error(log_message, detected_ccnum_log_file)
                    detected_columns.add(column)
            else:
                logger.info(
                    f"No raw credit card numbers detected in column: {column}"
                )

            end_col_check_time = time.time()
            logger.info(
                f"Total time taken to check {column}: {end_col_check_time - start_col_check_time} seconds."
            )
        except Exception as e:
            logger.error(f"Error checking column {column} for raw CCNs: {e}")
            raise

    if detected_columns:
        error_message = (
            f"Raw credit card numbers found in columns: {', '.join(detected_columns)}"
        )
        logger.error(error_message)
        raise ValueError(error_message)
    else:
        logger.info("No raw credit card numbers detected in the DataFrame.")


def encode_free_text_column(spark, df, column_name, client, mapping_df, debug):
    """
    Function for Extracting and Encoding 14, 15 and 16 digit valid credit card numbers
    in the free text columns in our data
    """
    try:
        logger.info(f"Processing Free Text column: {column_name}")
        start_free_text_col_time = time.time()

        # Extract patterns and text parts into columns
        df_reg = df.select(
            "unique_id",
            column_name,
            regexp_extract_all(col(column_name), lit(PATTERN), 0).alias("match"),
            regexp_extract_all(col(column_name), lit(PATTERN), 1).alias("other_text"),
            regexp_extract_all(col(column_name), lit(PATTERN), 2).alias("number"),
        ).withColumn("zipped", arrays_zip(col("other_text"), col("number")))

        if debug:
            logger.info(f"=== df_reg for {column_name} ===")
            df_reg.show(truncate=False)
            df_reg.printSchema()
            logger.info(f"Row count after extraction: {df_reg.count()}")

        # Define window specification for order_id partitioned by unique_id and order by constant one
        window_spec_order = Window.partitionBy("unique_id").orderBy(lit(WINDOW_ORDER))

        # Explode the zipped list into separate rows for easier processing
        df_exploded = df_reg.select(
            "unique_id", column_name, explode("zipped").alias("exploded")
        ).select(
            "unique_id",
            column_name,
            col("exploded.other_text"),
            col("exploded.number").alias("number_raw"),
            regexp_replace(col("exploded.number"), r"[-\s]", "").alias("number"),
            row_number().over(window_spec_order).alias(
                "order_id"
            ),  # Add order_id to preserve the order
        )

        if debug:
            logger.info(f"=== df_exploded for {column_name} ===")
            df_exploded.show(truncate=False)
            df_exploded.printSchema()
            logger.info(f"Row count after explosion: {df_exploded.count()}")

        start_luhn_validate_time = time.time()
        # UDF to check if the number is valid using the Luhn checksum
        validate_udf = udf(lambda x: luhn_checksum(x) if x else False, BooleanType())
        df_validated = df_exploded.withColumn("is_valid", validate_udf(col("number")))
        end_luhn_validate_time = time.time()
        logger.info(
            f"Time taken to validate extracted numbers using luhn: {end_luhn_validate_time - start_luhn_validate_time} seconds."
        )

        if debug:
            logger.info(f"=== df_validated for {column_name} ===")
            df_validated.show(truncate=False)
            df_validated.printSchema()
            logger.info(f"Row count after validation: {df_validated.count()}")

        # Filter out invalid numbers before joining with the mapping DataFrame
        df_valid_only = df_validated.filter(col("is_valid"))

        if debug:
            logger.info(f"=== df_valid_only for {column_name} ===")
            df_valid_only.show(truncate=False)
            df_valid_only.printSchema()
            logger.info(
                f"Row count after filtering invalid numbers: {df_valid_only.count()}"
            )

        # Pad numbers to 16 digits
        df_valid_only = df_valid_only.withColumn(
            "padded_number", expr("lpad(number, 16, '0')")
        )

        if debug:
            logger.info(
                f"=== df_valid_only with padded numbers for {column_name} ==="
            )
            df_valid_only.show(truncate=False)
            df_valid_only.printSchema()
            logger.info(f"Row count after padding numbers: {df_valid_only.count()}")

        df_encoded = df_valid_only.join(
            mapping_df, df_valid_only.padded_number == mapping_df.CTRT_NMB, how="left"
        ).select(
            "unique_id",
            column_name,
            "number",
            "number_raw",
            "is_valid",
            "other_text",
            "order_id",
            "padded_number",
            coalesce("CONTRACT_UID", lit("")).alias("encoded"),
        )

        if debug:
            logger.info(f"=== df_encoded for {column_name} ===")
            df_encoded.show(truncate=False)
            df_encoded.printSchema()
            logger.info(
                f"Row count after encoding using mapping table: {df_encoded.count()}"
            )

        # Handle unmatched credit card numbers using the vault (for valid numbers only)
        unmatched_df = (
            df_encoded.filter(col("encoded") == "").select("padded_number").distinct()
        )
        unmatched_ccn_count = unmatched_df.count()
        unmatched_ccns = [row["padded_number"] for row in unmatched_df.collect()]

        if unmatched_ccns:
            valid_unmatched_ccns = [
                ccn for ccn in unmatched_ccns if ccn.isdigit() and len(ccn) == 16
            ]
            if valid_unmatched_ccns:
                # start time for vault tracking
                start_vault_time = time.time()

                # send a list of numbers to encode using vault
                encoded_ccns = batch_transform_credit_card_numbers(
                    client, valid_unmatched_ccns, debug
                )

                # Check if the count of numbers sent to the vault is equal to the returned encoded values
                if len(valid_unmatched_ccns) != len(encoded_ccns):
                    logger.error(
                        f"Mismatch in the count of valid numbers sent to the vault and returned encoded values. Sent: {len(valid_unmatched_ccns)}, Returned: {len(encoded_ccns)}"
                    )
                    raise ValueError(
                        "Count of valid numbers sent to the vault does not equals to returned encoded values"
                    )
                else:
                    # end_vault_time
                    end_vault_time = time.time()
                    logger.info(
                        f"Total Time taken to encode {unmatched_ccn_count} unmatched numbers using vault: {end_vault_time - start_vault_time} seconds."
                    )

                new_mappings = [
                    (ccn, encoded_ccn)
                    for ccn, encoded_ccn in zip(valid_unmatched_ccns, encoded_ccns)
                ]
                new_mappings_df = spark.createDataFrame(
                    new_mappings, ["CTRT_NMB", "CONTRACT_UID"]
                ).dropDuplicates(["CTRT_NMB"])

                # Validate the mapping table for completeness and correctness
                invalid_mappings = new_mappings_df.filter(
                    new_mappings_df["CONTRACT_UID"].isNull()
                ).count()
                if invalid_mappings > 0:
                    error_message = f"Mapping table contains {invalid_mappings} invalid entries with missing CONTRACT_UID."
                    logger.error(error_message)
                    raise ValueError(error_message)
                else:
                    new_mappings_df.write.parquet(MAPPING_TABLE_PATH, mode="append")
                    mapping_df = (
                        mapping_df.union(new_mappings_df)
                        .dropDuplicates(["CTRT_NMB"])
                        .cache()
                    )
                    new_mappings_df.write.parquet(MAPPING_TABLE_PATH, mode="append")

                mapping_df = (
                    mapping_df.union(new_mappings_df)
                    .dropDuplicates(["CTRT_NMB"])
                    .cache()
                )

                # Update the DataFrame with newly encoded values
                df_encoded = df_encoded.join(
                    mapping_df,
                    df_encoded.padded_number == mapping_df.CTRT_NMB,
                    "left_outer",
                ).withColumn("encoded", coalesce(col("CONTRACT_UID"), col("encoded")))
                if debug:
                    logger.info(f"=== df_vault_encoded for {column_name} ===")
                    df_encoded.show(truncate=False)
                    df_encoded.printSchema()
                    logger.info(
                        f"Row count after vault encoding: {df_encoded.count()}"
                    )

        # Combine valid and invalid numbers back together
        df_combined = df_validated.join(
            df_encoded,
            [
                "unique_id",
                column_name,
                "number",
                "number_raw",
                "is_valid",
                "other_text",
                "order_id",
            ],
            "left_outer",
        ).select(
            "unique_id",
            column_name,
            "other_text",
            "number_raw",
            "is_valid",
            coalesce(col("encoded"), lit("")).alias("encoded"),
            "order_id",
        )

        # Create a single column for both valid and invalid numbers: valid numbers are encoded, invalid remain raw
        df_concat = df_combined.withColumn(
            "concat_org", concat(col("other_text"), col("number_raw"))
        ).withColumn(
            "concat_encoded",
            concat(
                col("other_text"),
                when(
                    col("is_valid") & (col("encoded") != ""), col("encoded")
                ).otherwise(col("number_raw")),
            ),
        )

        if debug:
            logger.info(f"=== df_concat for {column_name} ===")
            df_concat.show(truncate=False)
            df_concat.printSchema()
            logger.info(f"Row count after concatenation: {df_concat.count()}")

        # Sort the DataFrame by order_id before aggregation
        df_sorted = df_concat.orderBy("unique_id", "order_id")

        # Final aggregation: preserve the order of text and numbers
        df_out = df_sorted.groupBy("unique_id", column_name).agg(
            concat_ws("", collect_list("concat_org")).alias("text_org_parsed"),
            concat_ws("", collect_list("concat_encoded")).alias(
                f"text_encoded_{column_name}"
            ),
        )

        if debug:
            logger.info(f"=== df_out for {column_name} ===")
            df_out.show(truncate=False)
            df_out.printSchema()
            logger.info(f"Row count after aggregation: {df_out.count()}")

        end_free_text_col_time = time.time()
        logger.info(
            f"Total Time taken to process free text col {column_name}: {end_free_text_col_time - start_free_text_col_time} seconds."
        )
        return df_out
    except Exception as e:
        logger.error(f"Error processing column {column_name}: {e}")
        raise


def encode_free_text_columns(spark, df, client, mapping_df, free_text_columns, debug):
    """
    Function for Orchestrating the Extraction and Encoding of 14, 15 and 16 digit valid credit card numbers in the free text columns
    """
    dfs = [
        encode_free_text_column(spark, df, col_name, client, mapping_df, debug)
        for col_name in free_text_columns
    ]

    # Combine the results
    df_final = dfs[0]
    for df_temp in dfs[1:]:
        df_final = df_final.join(df_temp, on="unique_id", how="outer")

    # Select only the encoded columns and rename them to the original column names
    select_exprs = ["unique_id"] + [
        col(f"text_encoded_{free_text_columns[i]}").alias(free_text_columns[i])
        for i in range(len(free_text_columns))
    ]
    df_final = df_final.select(*select_exprs)

    if debug:
        logger.info("=== df_final after combining free text columns ===")
        df_final.show(truncate=False)
        df_final.printSchema()
        logger.info(
            f"Row count after combining free text columns: {df_final.count()}"
        )

    return df_final

# Process credit card columns.
def encode_cc_columns(df, cc_columns, mapping_df, client, spark, debug):
    """
    Function for encoding fixed length (len=16) columns containing only credit card numbers
    """
    for cc_column in cc_columns:
        # start time for cc col encode
        start_cc_col_encode_time = time.time()
        logger.info(f"Encoding CC column: {cc_column}")

        padded_cc_column = f"{cc_column}_padded"

        # Pad and clean the credit card number
        df = df.withColumn(
            padded_cc_column,
            expr(f"lpad(regexp_replace({cc_column}, '\\\\D', ''), 16, '0')"),
        )

        # Create a flag for valid numbers
        df = df.withColumn(
            "is_valid_ccn",
            (col(padded_cc_column).isNotNull())
            & (length(col(padded_cc_column)) == 16)
            & (col(padded_cc_column).rlike("^[0-9]+$")),
        )

        # Alias the DataFrames before joining
        df = df.alias("df")
        mapping_df = mapping_df.alias("mapping_df")

        # Join with mapping_df for encoding
        df = df.join(
            mapping_df, df[padded_cc_column] == mapping_df["CTRT_NMB"], how="left_outer"
        ).select("df.*", col("mapping_df.CONTRACT_UID").alias("mapped_CONTRACT_UID"))

        # If no mapping, attempt to encode using Vault
        unmatched_df = (
            df.filter(df["is_valid_ccn"] & df["mapped_CONTRACT_UID"].isNull())
            .select(padded_cc_column)
            .distinct()
        )
        unmatched_ccn_count = unmatched_df.count()
        unmatched_ccns = [row[padded_cc_column] for row in unmatched_df.collect()]

        # if debug:
        #    logger.info(f"Unmatched CCNs to be sent to Vault: {unmatched_ccns}")

        if unmatched_ccns:
            # start time for vault tracking
            start_vault_time = time.time()

            # send a list of numbers to encode using vault
            encoded_ccns = batch_transform_credit_card_numbers(
                client, unmatched_ccns, debug
            )

            # end_vault_time
            end_vault_time = time.time()
            logger.info(
                f"Total Time taken to encode {unmatched_ccn_count} unmatched numbers using vault: {cc_column} : {end_vault_time - start_vault_time} seconds."
            )

            new_mappings = [
                (ccn, encoded_ccn)
                for ccn, encoded_ccn in zip(unmatched_ccns, encoded_ccns)
            ]
            new_mappings_df = spark.createDataFrame(
                new_mappings, ["CTRT_NMB", "CONTRACT_UID"]
            ).dropDuplicates(["CTRT_NMB"])
            # Validate the mapping table for completeness and correctness
            invalid_mappings = new_mappings_df.filter(
                new_mappings_df["CONTRACT_UID"].isNull()
            ).count()
            if invalid_mappings > 0:
                error_message = f"Mapping table contains {invalid_mappings} invalid entries with missing CONTRACT_UID."
                logger.error(error_message)
                raise ValueError(error_message)
            else:
                new_mappings_df.write.parquet(MAPPING_TABLE_PATH, mode="append")
                mapping_df = (
                    mapping_df.union(new_mappings_df)
                    .dropDuplicates(["CTRT_NMB"])
                    .cache()
                )

            # Alias the DataFrames before joining
            df = df.alias("df")
            df = df.drop("mapped_CONTRACT_UID")
            mapping_df = mapping_df.alias("mapping_df")

            df = df.join(
                mapping_df, df[padded_cc_column] == mapping_df["CTRT_NMB"], "left_outer"
            ).select(
                "df.*", col("mapping_df.CONTRACT_UID").alias("mapped_CONTRACT_UID")
            )

        # Replace the original CC column with the encoded value
        df = df.withColumn(
            cc_column,
            when(
                col("is_valid_ccn") & col("mapped_CONTRACT_UID").isNotNull(),
                col("mapped_CONTRACT_UID"),
            ).otherwise(col(cc_column)),
        )
        df = df.drop(padded_cc_column, "mapped_CONTRACT_UID", "is_valid_ccn")

        end_cc_col_encode_time = time.time()
        logger.info(
            f"Total Time taken to encode CC column: {cc_column} : {end_cc_col_encode_time - start_cc_col_encode_time} seconds."
        )

    return df


def check_invalid_cc_numbers(df, cc_columns):
    """
    Check invalid credit card numbers > 16 digits in cc columns
    """
    invalid_columns = []
    for cc_column in cc_columns:
        invalid_cc_count = df.filter(length(col(cc_column)) > 16).count()
        if invalid_cc_count > 0:
            invalid_columns.append((cc_column, invalid_cc_count))

    if invalid_columns:
        error_messages = [
            f"Column {col} contains {count} entries with more than 16 digits."
            for col, count in invalid_columns
        ]
        error_message = " | ".join(error_messages)
        logger.error(error_message)
        raise ValueError(error_message)


def process_file(
    input_file,
    cc_columns,
    free_text_columns,
    skip_check_columns,
    detected_ccnum_log_file,
    delimiter,
    debug,
):
    """
    Function for processing the input file: Detect -> Encode -> Check -> Output parquet
    """
    spark = None
    try:
        spark = SparkSession.builder.appName("CCEncode").getOrCreate()

        # Register the luhn_checksum function as a UDF
        luhn_checksum_udf = udf(luhn_checksum, BooleanType())
        spark.udf.register("luhn_checksum", luhn_checksum_udf)

        df = spark.read.csv(input_file, header=True, sep=delimiter, escape="\"")

        temp_df_dir = os.path.join(
                BASE_TEMP_DIR, os.path.splitext(os.path.basename(input_file))[0]
            )

        if cc_columns:
            # Check invalid credit card numbers > 16 digits in cc columns
            check_invalid_cc_numbers(df, cc_columns)

        # Check initial count
        initial_count = df.count()
        logger.info(f"Initial row count: {initial_count}")

        # Log the columns to verify correct parsing
        # logger.info(f"Columns in DataFrame: {df.columns}")

        df = df.alias("df")

        # Identify columns for encoding
        all_columns = df.columns
        non_cc_columns = [
            col
            for col in all_columns
            if col not in cc_columns and col not in free_text_columns
        ]

        # Load Vault configuration
        vault_config = load_vault_config(KMS_CONFIDENTIAL_FPE_PATH)

        # Prepare Vault for encoding
        vault_token = authenticate_vault(
            vault_config["url"],
            vault_config["role_id"],
            vault_config["secret_id"],
            vault_config["verify_ssl"],
        )
        client = hvac.Client(
            url=vault_config["url"],
            token=vault_token,
            verify=vault_config["verify_ssl"],
        )

        # Read the mapping table for replacing credit card numbers with encoded values
        mapping_df = spark.read.parquet(MAPPING_TABLE_PATH)
        mapping_df = mapping_df.withColumn(
            "CTRT_NMB", mapping_df["CTRT_NMB"].cast("string")
        )
        mapping_df = mapping_df.withColumn(
            "CONTRACT_UID", mapping_df["CONTRACT_UID"].cast("string")
        )
        mapping_df = mapping_df.dropDuplicates(["CTRT_NMB"])

        # Validate the mapping table for completeness and correctness
        invalid_mappings = mapping_df.filter(
            mapping_df["CONTRACT_UID"].isNull()
        ).count()
        if invalid_mappings > 0:
            error_message = f"Mapping table contains {invalid_mappings} invalid entries with missing CONTRACT_UID."
            logger.error(error_message)
            raise ValueError(error_message)

        # Process credit card columns (cc_columns)
        if cc_columns:
            # start cc col processing
            start_cc_col_encode_time = time.time()

            # call function to encode cc columns
            df = encode_cc_columns(df, cc_columns, mapping_df, client, spark, debug)

            # end cc col processing
            end_cc_col_encode_time = time.time()
            logger.info(
                f"Total Time taken to encode all CC columns: {end_cc_col_encode_time - start_cc_col_encode_time} seconds."
            )

            if debug:
                # Debug: Check row count after processing CC columns
                cc_processed_count = df.count()
                logger.info(
                    f"Row count after processing CC columns: {cc_processed_count}"
                )

        # Process free text columns (free_text_columns)
        if free_text_columns:
            # start free text col processing
            start_free_text_col_encode_time = time.time()

            # add unique id column using spark's monotonically_increasing_id func (not sequential) 
            # in order to join after encode function.
            df_with_unique_id = df.withColumn("unique_id", monotonically_increasing_id())
            # We can also do it like below using row_number and Window functions but since it is sequential 
            # it reshuffles the data to put it order which in turn affects the performance for large datasets.
            # window_spec = Window.orderBy(lit(1))
            # df_with_unique_id = df.withColumn("unique_id", row_number().over(window_spec))
            
            # Write the DataFrame with unique_id column to temporary directory
            df_with_unique_id.write.mode("overwrite").parquet(temp_df_dir)

            # Read the saved temp data with unique_id column into dataframe
            df_with_unique_id = spark.read.parquet(temp_df_dir)

            # send list of free text columns for encoding
            df_encoded = encode_free_text_columns(
                spark, df_with_unique_id, client, mapping_df, free_text_columns, debug
            )

            df_encoded = df_encoded.alias("df_encoded")
            df = df_with_unique_id.alias("df")

            # end free text col processing
            end_free_text_col_encode_time = time.time()
            logger.info(
                f"Total Time taken to encode all Free Text columns: {end_free_text_col_encode_time - start_free_text_col_encode_time} seconds."
            )

            # Select columns from df and df_encoded to avoid ambiguity
            df_columns = [col("df." + column) for column in df.columns]
            df_encoded_columns = [
                col("df_encoded." + column).alias(column + "_encoded")
                for column in free_text_columns
            ]
            df_encoded_columns.append(col("df_encoded.unique_id"))

            if debug:
                # Debug: Check row counts before join
                logger.info(f"Row count before join: {df.count()}")
                logger.info(f"Encoded row count: {df_encoded.count()}")

            df = df.join(
                df_encoded.select(df_encoded_columns),
                df["unique_id"] == df_encoded["unique_id"],
                how="left",
            ).drop(df_encoded["unique_id"])

            if debug:
                # Debug: Check row count after join
                logger.info(f"Row count after join: {df.count()}")

            # Rename encoded columns back to their original names
            for column in free_text_columns:
                df = df.withColumn(column, col(column + "_encoded")).drop(
                    column + "_encoded"
                )

        final_count = df.count()
        logger.info(f"Final row count: {final_count}")

        # Merge non-encoded columns back into the final DataFrame
        final_columns = []
        for column in all_columns:
            if column in non_cc_columns:
                final_columns.append(column)  # Add non-encoded columns
            elif column in cc_columns:
                final_columns.append(column)  # Add processed CC columns
            elif column in free_text_columns:
                final_columns.append(column)  # Add processed free text columns

        df_final = df.select(final_columns)

        if initial_count == final_count:
            # start credit card number check for all columns except skip check columns
            start_cc_check_time = time.time()

            # Check for raw credit card numbers before saving
            check_for_raw_cc_numbers(
                df_final, skip_check_columns, detected_ccnum_log_file
            )

            # end credit card number check time
            end_cc_check_time = time.time()
            logger.info(
                f"Total Time taken to Check all target columns: {end_cc_check_time - start_cc_check_time} seconds."
            )

            # Save the output as parquet
            output_dir = os.path.join(
                BASE_OUTPUT_DIR, os.path.splitext(os.path.basename(input_file))[0]
            )
            df_final.write.parquet(output_dir, mode="overwrite")
        else:
            error_message = f"Initial Count: {initial_count} is not equal to Final Count: {final_count}"
            logger.error(error_message)
            raise ValueError(error_message)

    except Exception as e:
        error_message = f"Exception: {str(e)}"
        logger.error(f"File: {input_file}\n{error_message}")
        sys.exit(1)
    finally:
        if os.path.exists(temp_df_dir):
            shutil.rmtree(temp_df_dir)
        if spark:
            spark.stop()


def extract_base_table_name(filename):
    """
    Function to extract base table name from filename
    """
    # Remove the file extension
    base_name = os.path.splitext(filename)[0]

    # Remove numeric suffix followed by date suffix if present
    base_name = re.sub(r"_\d+_\d{8}$", "", base_name)

    # Remove date suffix if present
    base_name = re.sub(r"_\d{8}$", "", base_name)

    return base_name


# Main function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CC Mask Script")
    parser.add_argument("input_file", type=str, help="Path to the input CSV file")
    parser.add_argument(
        "input_file_config", type=str, help="Path to the configuration JSON file"
    )
    args = parser.parse_args()

    # Start File Processing
    start_file_processing_time = time.time()

    # Extract the base filename without suffix and extension
    input_file = args.input_file
    base_filename = os.path.splitext(os.path.basename(input_file))[0]

    # Setup logger for this specific file
    logger = logging_utils.setup_logger(
        "cc_encode", f"{BASE_LOG_DIR}/{base_filename}/{base_filename}.log"
    )

    # Read the configuration file
    with open(args.input_file_config, mode="r", encoding="utf-8") as f:
        config = json.load(f)

    # Extract the base table name
    base_table_name = extract_base_table_name(base_filename)

    # Log the extracted base table name
    logger.info(f"Extracted base table name: {base_table_name}")

    # Get the credit card columns and free-text columns for the base table name
    cc_columns = config.get(base_table_name, {}).get("cc_columns", [])
    free_text_columns = config.get(base_table_name, {}).get("free_text_columns", [])
    skip_check_columns = config.get(base_table_name, {}).get("skip_check_columns", [])
    delimiter = config.get(base_table_name, {}).get("delimiter", ",")

    # Log the delimiter being used
    logger.info(f"Using delimiter: '{delimiter}' for file: {input_file}")

    detected_ccnum_log_file = f"{BASE_LOG_DIR}/{base_filename}_detected_ccnum.log"

    process_file(
        input_file,
        cc_columns,
        free_text_columns,
        skip_check_columns,
        detected_ccnum_log_file,
        delimiter,
        debug=False,
    )

    end_file_processing_time = time.time()
    logger.info(
        f"Total Time taken to process the file: {end_file_processing_time - start_file_processing_time} seconds."
    )
