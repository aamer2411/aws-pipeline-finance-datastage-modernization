################################################################################
# common_utils.py
#
# Purpose:
#   Shared utility module for NON-IPORT AWS Glue ETL jobs.
#   This script is imported by other Glue jobs for internal processing,
#   fixed-width file parsing, S3 file operations, and DataFrame output.
#
# Covers:
#   - Spark / GlueContext initialization (module-level singletons)
#   - S3 file utilities: get latest file (pattern-based and prefix-based),
#     delete files from S3
#   - Fixed-width data splitting with Decimal/Integer type casting
#   - KMS-encrypted output storage to S3 (env-aware: eng / test / prod)
#   - Dependent output storage (flat CSV, pipe-delimited)
#   - Structured output storage (date-partitioned folder paths)
#   - Dummy SOT / SOP / SOC DataFrames (used as empty schema templates
#     when source data is unavailable, ensuring downstream jobs don't fail)
#
# Systems Used:
#   - AWS Glue (PySpark)
#   - Amazon S3 (via boto3 and AWS KMS encryption)
#
# NOTE:
#   Logic has NOT been changed. Only formatting, indentation,
#   and clarifying comments were added.
################################################################################

import sys
import time
import re
import datetime
import hashlib

import boto3
import pandas as pd

from datetime import datetime, timedelta, date
from botocore.exceptions import ClientError

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, substring, concat_ws, trim, explode,
    row_number, monotonically_increasing_id
)
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.window import Window
from awsglue.context import GlueContext


################################################################################
# SECTION 1 — Global Spark Configuration
################################################################################

# Increase Spark optimizer iterations for complex multi-join query plans.
# Default is 100; raising to 500 prevents optimizer timeout on wide-column joins
# common in fixed-width mainframe transformations.
SparkConf().set('spark.sql.optimizer.maxIterations', '500')

# Module-level S3 client — shared across all functions to avoid re-initializing on each call
s3_client = boto3.client('s3')

# Capture today's date at module load time for use in date-partitioned output paths
today = datetime.date.today()
year  = str(today.year)  # Used in output_store() subfolder name construction

# Max driver result size — set high (5G) to support large DataFrame collects during toPandas()
# If OOM errors occur, reduce this value or rethink the toPandas() approach
spark_driver_maxResultSize = '5G'


################################################################################
# SECTION 2 — Spark / GlueContext Initialization
################################################################################

# Module-level Spark + Glue singletons shared by all functions.
# SparkContext.getOrCreate() is safe to call multiple times — returns existing context
# if already initialized (important in Glue where context is pre-created by the framework)
sc           = SparkContext.getOrCreate()
glueContext  = GlueContext(sc)
spark        = glueContext.spark_session


def spark_session():
    """
    Returns the active SparkSession for use by importing job scripts.

    Why a function instead of just exposing the module-level `spark` var:
        Allows importing scripts to get a fresh session reference without needing
        to know how initialization was done — provides a clean access pattern.

    Returns:
        SparkSession: Active Glue SparkSession
    """
    sc           = SparkContext.getOrCreate()
    glueContext  = GlueContext(sc)
    spark        = glueContext.spark_session
    return spark


def glue_Context():
    """
    Returns the Glue logger for use by this module and importing scripts.

    Used for structured logging via glueContext.get_logger() rather than raw print().
    Glue logger output appears in CloudWatch Logs under the job's log group.

    Returns:
        logger: AWS Glue logger instance
    """
    sc          = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    logger      = glueContext.get_logger()
    return logger


# Module-level logger — used by delete_s3_file() and any function that needs CloudWatch logging
logger = glue_Context()


################################################################################
# SECTION 3 — S3 File Utilities
################################################################################

def get_latest_file(bucket_name: str, prefix_path: str, filename_pattern: str):
    """
    Finds the most recently modified S3 file matching a regex filename pattern.

    Used when multiple files with similar names exist (e.g., daily drops with dates in the name)
    and the job must always pick up the freshest one.

    Args:
        bucket_name      : S3 bucket name. Leading 's3://' is stripped automatically.
        prefix_path      : S3 key prefix to list objects under (e.g., 'Incoming/')
        filename_pattern : Regex pattern to match against the full object key
                           (e.g., r'SOT_.*\\.csv' to match any SOT file)

    Returns:
        str: Filename (last segment of the S3 key) of the most recently modified match,
             or None if no matching files found.

    Note:
        Uses list_objects_v2 without pagination — may miss files if the bucket prefix
        contains more than 1,000 objects. Use a paginator if that becomes a concern.
    """
    s3 = boto3.client('s3')

    # Strip 's3://' prefix if provided — list_objects_v2 requires a bare bucket name
    bucket_name = bucket_name.replace('s3://', '')

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix_path)

    if "Contents" not in response:
        print("No Files found at the specified path.")
        return None

    # Filter to only files matching the regex pattern
    matching_files = [
        obj for obj in response["Contents"]
        if re.search(filename_pattern, obj['Key'])
    ]

    if not matching_files:
        return None

    # Sort descending by LastModified and take the first (most recent)
    latest_file = sorted(matching_files, key=lambda x: x['LastModified'], reverse=True)[0]

    # Return only the filename portion (strip path prefix)
    return latest_file['Key'].split('/')[-1]


def get_latest_file_from_s3(bucket_name, prefix):
    """
    Finds the most recently modified S3 object under the 'Incoming/<prefix>' path.

    Variant of get_latest_file() that hardcodes the 'Incoming/' base path —
    used by jobs that always pull source files from the standard Incoming folder.

    Args:
        bucket_name : S3 bucket name (bare, no 's3://' prefix)
        prefix      : File name prefix to filter by (e.g., 'SOT_DAILY')

    Returns:
        str: Filename with 'Incoming/' stripped, or None if not found.
    """
    s3_client = boto3.client('s3')

    # Always look inside the 'Incoming/' folder for inbound source files
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix='Incoming/' + prefix
    )

    if 'Contents' not in response:
        print(f"No files found with prefix '{prefix}'")
        return None

    latest_file        = None
    latest_modified_time = None

    # Iterate all matching objects to find the most recently modified
    for obj in response['Contents']:
        if latest_modified_time is None or obj['LastModified'] > latest_modified_time:
            latest_modified_time = obj['LastModified']
            latest_file          = obj['Key']

    if latest_file:
        # Strip the 'Incoming/' path prefix — callers only need the filename
        latest_filename = latest_file.replace('Incoming/', '')
        print(f"Latest file: {latest_filename}")
        return latest_filename
    else:
        print(f"No files found with prefix '{prefix}'")
        return None


def delete_s3_file(bucket_name, path, key):
    """
    Deletes a single file from S3 after processing is complete.

    Used by jobs to clean up intermediate/temp files from the Intermediate folder
    after the final transformation script has run successfully. This keeps the
    Intermediate S3 prefix clean and prevents reprocessing stale files.

    Args:
        bucket_name : S3 bucket name
        path        : Folder path (e.g., 'Intermediate/DailyLoad')
        key         : Filename to delete (e.g., 'SOT_20250115.csv')

    Returns:
        bool: True on success, False on ClientError.
    """
    s3_client = boto3.client('s3')
    file_key  = f"{path}/{key}"

    try:
        s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        logger.info(f"Successfully deleted {file_key} from {bucket_name}")
        return True
    except ClientError as e:
        logger.info(f"Error deleting file: {e}")
        return False


################################################################################
# SECTION 4 — Fixed-Width Data Splitting
################################################################################

def data_split(df, n, datatype, columns):
    """
    Parses a fixed-width column ('all_column_values') into individual named columns,
    then casts each to its declared datatype.

    Background:
        Mainframe-generated fixed-width files are ingested as a single raw string column
        ('all_column_values'). This function uses field-width metadata to extract each
        field using PySpark's substring() at the correct byte offsets.

    Field width encoding (the 'n' parameter):
        - Simple int    → field occupies exactly n characters (e.g., 10 → chars 1-10)
        - [total, scale]→ Decimal field: total digits, 'scale' of which are after the decimal.
          A dot is inserted: first (total-scale) chars = integer part, last scale chars = decimal.

    Example:
        n       = [5, 3, [8, 2]]
        columns = ['CODE', 'NAME', 'AMOUNT']
        → 'CODE'   = chars 1-5
        → 'NAME'   = chars 6-8
        → 'AMOUNT' = chars 9-16, reformatted as 'XXXXXX.XX'

    Datatype casting:
        After all columns are extracted as strings, each column is cast to its declared type.
        Supported types: 'Decimal' (with optional precision/scale), 'Integer'.
        All other types are left as strings (no cast applied).

    Args:
        df       : Spark DataFrame with a single column 'all_column_values' (raw fixed-width rows)
        n        : List of field widths. int for simple fields, [total, scale] for Decimal fields.
        datatype : List of type names matching each column in 'columns'
                   (e.g., ['Integer', 'String', 'Decimal'])
        columns  : List of output column names to create

    Returns:
        DataFrame: New DataFrame with all individual columns extracted and typed.
                   The 'all_column_values' raw column is dropped from the output.
    """
    start = 1   # Fixed-width parsing is 1-indexed in PySpark's substring()
    end   = 0

    # --- Step 1: Extract all fields from the raw fixed-width string column ---
    for i in range(len(n)):
        if not isinstance(n[i], list):
            # Simple field: extract exactly n[i] characters starting at 'start'
            end = n[i]
            df  = df.withColumn(columns[i], substring('all_column_values', start, end))
            start = start + end

        else:
            # Decimal field: [total_length, decimal_scale]
            # Split into integer_part and decimal_part, joined with a '.'
            # Example: n[i]=[8,2], raw='00123456' → '001234.56'
            end = n[i][0]
            df  = df.withColumn(
                columns[i],
                concat_ws(
                    '.',
                    # Integer part: first (total - scale) characters
                    substring('all_column_values', start, n[i][0] - n[i][1]),
                    # Decimal part: last 'scale' characters
                    substring('all_column_values', (start + n[i][0] - n[i][1]), n[i][1])
                )
            )
            start = start + end

    # Drop the raw fixed-width column — all fields are now individual columns
    df         = df.drop('all_column_values')
    pysparkdf  = df

    # --- Step 2: Cast each column to its declared type ---
    for i in range(len(datatype)):

        if datatype[i] == 'Decimal':
            if isinstance(n[i], list):
                # Precision = total digits, scale = decimal digits
                # Example: [8,2] → DECIMAL(8,2)
                decimal_type = f"Decimal({n[i][0]},{n[i][1]})"
                pysparkdf = pysparkdf.withColumn(
                    pysparkdf.columns[i],
                    col(pysparkdf.columns[i]).cast(decimal_type)
                )
            else:
                # No precision specified — cast to plain Decimal
                pysparkdf = pysparkdf.withColumn(
                    pysparkdf.columns[i],
                    col(pysparkdf.columns[i]).cast('Decimal')
                )

        elif datatype[i] == 'Integer':
            if isinstance(n[i], list):
                # Parameterized Integer — unusual, but handled defensively
                integer_type = f"Integer({n[i][0]},{n[i][1]})"
                pysparkdf = pysparkdf.withColumn(
                    pysparkdf.columns[i],
                    col(pysparkdf.columns[i]).cast(integer_type)
                )
            else:
                pysparkdf = pysparkdf.withColumn(
                    pysparkdf.columns[i],
                    col(pysparkdf.columns[i]).cast('Integer')
                )

        # Note: 'String' and all other types are not cast — extracted value is kept as-is

    return pysparkdf


################################################################################
# SECTION 5 — Output Storage Functions
################################################################################

def dependent_output_store(datadf, split_no, file_name, extension,
                            target_path, sub_folder, folder_name):
    """
    Writes a DataFrame to S3 as a PIPE-DELIMITED CSV with KMS encryption.

    Used by jobs that write intermediate/dependent output — files that will be
    consumed by downstream transformation steps (hence 'dependent').

    File naming convention:
        <sub_folder>/<file_name><folder_name>.<extension>
        Example: Intermediate/SOT_20250115.csv

    S3 path is flat (no date-partitioned subfolders) — see output_store() for
    the partitioned variant used for final output files.

    Env detection:
        Uses substring matching on target_path (e.g., 'eng', 'test', 'prod')
        to select the correct KMS key ARN for server-side encryption.

    KMS Key ARNs by environment:
        eng  : arn:aws:kms:us-east-1:<account-id-eng>:key/<kms-key-id-eng>
        test : arn:aws:kms:us-east-1:<account-id-test>:key/<kms-key-id-test>
        prod : arn:aws:kms:us-east-1:<account-id-prod>:key/<kms-key-id-prod>

    Args:
        datadf      : Spark DataFrame to write
        split_no    : Intended split size (currently unused — splitting logic is commented out;
                      the full DataFrame is always written as one file)
        file_name   : Output file name prefix (e.g., 'SOT_DAILY_')
        extension   : File extension without dot (e.g., 'csv')
        target_path : S3 bucket name (env-aware substring: contains 'eng', 'test', or 'prod')
        sub_folder  : S3 folder within the bucket (e.g., 'Intermediate')
        folder_name : Date string appended to filename (e.g., '230417' → YYMMDD)

    NOTE:
        toPandas() collects all data to the driver — only suitable for small-to-medium DataFrames.
        For large DataFrames, use Spark's native write.csv() with partitioning instead.
    """
    target_path    = str(target_path)
    folder_name    = str(folder_name)   # Date string used in the output filename (e.g., '230417')
    file_name_date = folder_name        # Appended to file_name to create date-stamped output

    # Get total row count to determine batch size
    # NOTE: split_no-based chunking is currently disabled (see commented-out block below)
    df_totalcount = datadf.count()
    each_len      = int(df_totalcount)

    # --- Chunking logic (DISABLED — currently writes full DataFrame as one file) ---
    # Uncomment to re-enable split-based output (useful for large files):
    #
    # n_splits = int(df_totalcount / split_no)
    # if split_no < df_totalcount:
    #     each_len = split_no
    # else:
    #     each_len = int(df_totalcount)
    # if (each_len * n_splits < df_totalcount):
    #     n_splits += 1

    # Add a sequential row index — used to limit output to 'each_len' rows
    # monotonically_increasing_id() + row_number() over Window gives a stable 1-based index
    copy_df = datadf
    copy_df = copy_df.withColumn(
        'indexing',
        row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

    # Build the output filename: <file_name><date>.<extension>
    # e.g., 'SOT_DAILY_230417.csv'
    output_key = f"{sub_folder}/{file_name}{file_name_date}.{extension}"

    # --- Write to S3 with environment-specific KMS key ---
    # toPandas() → local /tmp CSV → boto3 upload with KMS SSE
    # The 'eng', 'test', 'prod' check uses substring matching on the target_path bucket name.

    if 'eng' in target_path:
        temp_df = copy_df.limit(each_len)
        temp_df = temp_df.drop(col('indexing'))
        # Pipe-delimited (sep='|') — matches the downstream job's expected delimiter
        temp_df.toPandas().to_csv('temp.csv', index=False, sep='|')
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            'temp.csv',
            target_path,
            output_key,
            ExtraArgs={
                "ServerSideEncryption": "aws:kms",
                # eng KMS key ARN
                "SSEKMSKeyId": "arn:aws:kms:us-east-1:<account-id-eng>:key/<kms-key-id-eng>"
            }
        )

    elif 'test' in target_path:
        temp_df = copy_df.limit(each_len)
        temp_df = temp_df.drop(col('indexing'))
        temp_df.toPandas().to_csv('temp.csv', index=False, sep='|')
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            'temp.csv',
            target_path,
            output_key,
            ExtraArgs={
                "ServerSideEncryption": "aws:kms",
                # test/SAT KMS key ARN
                "SSEKMSKeyId": "arn:aws:kms:us-east-1:<account-id-test>:key/<kms-key-id-test>"
            }
        )

    elif 'prod' in target_path:
        temp_df = copy_df.limit(each_len)
        temp_df = temp_df.drop(col('indexing'))
        temp_df.toPandas().to_csv('temp.csv', index=False, sep='|')
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            'temp.csv',
            target_path,
            output_key,
            ExtraArgs={
                "ServerSideEncryption": "aws:kms",
                # prod KMS key ARN
                "SSEKMSKeyId": "arn:aws:kms:us-east-1:<account-id-prod>:key/<kms-key-id-prod>"
            }
        )


def output_store(datadf, file_name, extension, target_path, sub_folder, folder_name):
    """
    Writes a DataFrame to S3 using a DATE-PARTITIONED folder structure with KMS encryption.

    Folder structure pattern:
        <sub_folder>/<YY + MM>/<DD>/<HH>/<file_name><date_str>.<extension>

    Where the date subfolders are derived from folder_name (YYMMDD string):
        subfoldername1 = current year[:2] + folder_name[0:2]  → e.g., '2023' → '2023'
        subfoldername2 = folder_name[2:4]                     → e.g., '04'
        subfoldername3 = folder_name[4:6]                     → e.g., '17'

    Example full path:
        Outgoing/2023/04/17/SOT_DAILY_230417.csv

    This partitioned layout makes it easy for downstream consumers (e.g., Athena,
    Redshift Spectrum) to scan only the relevant date partition.

    Difference from dependent_output_store():
        - output_store uses date-partitioned subfolder hierarchy (final output)
        - dependent_output_store uses flat subfolder (intermediate temp files)
        - output_store writes comma-separated CSV (default); dependent uses pipe-separated

    Args:
        datadf      : Spark DataFrame to write
        file_name   : Output file name prefix
        extension   : File extension (e.g., 'csv', 'txt')
        target_path : S3 bucket name (env-aware: 'eng', 'test', or 'prod')
        sub_folder  : Top-level folder within the bucket (e.g., 'Outgoing')
        folder_name : 6-digit date string YYMMDD (e.g., '230417')
    """
    target_path    = str(target_path)
    folder_name    = str(folder_name)
    file_name_date = folder_name

    # Build date-partitioned folder names from the 6-digit date string
    # year[:2] + folder_name[0:2] → e.g., '20' + '23' = '2023'
    subfoldername1 = year[:2] + folder_name[0:2]   # Full 4-digit year (e.g., '2023')
    subfoldername2 = folder_name[2:4]               # Month (e.g., '04')
    subfoldername3 = folder_name[4:6]               # Day   (e.g., '17')

    # Add stable sequential index for row ordering before toPandas()
    copy_df = datadf
    copy_df = copy_df.withColumn(
        'indexing',
        row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

    # n_splits placeholder (currently unused — full DataFrame written in one file)
    n_splits   = 1
    df_total   = datadf.count()
    each_len   = int(df_total)

    # Build the full S3 key with date-partitioned path
    # Format: sub_folder/YYYY/MM/DD/file_nameYYMMDD.extension
    partitioned_key = '/'.join([
        sub_folder, subfoldername1, subfoldername2, subfoldername3,
        f"{file_name}{file_name_date}.{extension}"
    ])

    if 'eng' in target_path:
        temp_df = copy_df.limit(each_len)
        temp_df = temp_df.drop(col('indexing'))
        temp_df.toPandas().to_csv('temp.csv', index=False)  # Comma-separated (no sep= override)
        s3_client = boto3.client('s3')
        if n_splits != (n_splits - 1):  # Always true — guard retained from original logic
            s3_client.upload_file(
                'temp.csv',
                target_path,
                partitioned_key,
                ExtraArgs={
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": (
                        "arn:aws:kms:us-east-1:<account-id-eng>:key/"
                        "<kms-key-id-eng>"
                    )
                }
            )

    elif 'test' in target_path:
        temp_df = copy_df.limit(each_len)
        temp_df = temp_df.drop(col('indexing'))
        temp_df.toPandas().to_csv('temp.csv', index=False)
        s3_client = boto3.client('s3')
        if n_splits != (n_splits - 1):
            s3_client.upload_file(
                'temp.csv',
                target_path,
                partitioned_key,
                ExtraArgs={
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": (
                        "arn:aws:kms:us-east-1:<account-id-test>:key/"
                        "<kms-key-id-test>"
                    )
                }
            )

    elif 'prod' in target_path:
        temp_df = copy_df.limit(each_len)
        temp_df = temp_df.drop(col('indexing'))
        temp_df.toPandas().to_csv('temp.csv', index=False)
        s3_client = boto3.client('s3')
        if n_splits != (n_splits - 1):
            s3_client.upload_file(
                'temp.csv',
                target_path,
                partitioned_key,
                ExtraArgs={
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": (
                        "arn:aws:kms:us-east-1:<account-id-prod>:key/"
                        "<kms-key-id-prod>"
                    )
                }
            )


################################################################################
# SECTION 6 — Dummy Schema DataFrames (SOT / SOP / SOC)
#
# These functions return empty Spark DataFrames that exactly mirror the expected
# schema of the SOT (Statement of Transactions), SOP (Statement of Position),
# and SOC (Statement of Cash) feed files.
#
# Purpose:
#   Downstream transformation jobs always do a UNION or JOIN against these schemas.
#   When no source data arrives (e.g., holiday, empty feed, upstream delay),
#   these dummy frames ensure the job produces a structurally valid (but empty)
#   output rather than failing with a schema mismatch.
#
# Pattern:
#   - merge_row_generator  : all-empty dict  → df1 (represents the "merge" feed)
#   - funnel_row_generator : column-name dict → df2 (represents the "funnel" mapping)
#   Both are created as pandas DataFrames first, then converted to PySpark.
################################################################################

def dummy_SOT():
    """
    Returns dummy (empty schema) DataFrames for the SOT (Statement of Transactions) feed.

    Returns:
        (df1, df2):
            df1 → All-empty row SOT merge DataFrame (used when no SOT source data available)
            df2 → SOT funnel mapping DataFrame (column name → column name, used for schema alignment)

    Column groups in SOT:
        - Core trade fields: RECTYPEID, SIDE, PAGENO, CONTIND, DESTINATIONID, etc.
        - Reference fields: OURREF, THEIRREF, SUBACC, FROMDATE, TODATE
        - Security fields: SECURITY, SECURITYDESCRIPTION, OPBAL, CLBAL, NOOFPARTS
        - Settlement: CONTRACTUAL_SETTLEMENT_DATE, SENDERS_REFERENCE, etc.
        - Extended attributes (XAMT1-10, XCCY1-10, XDATE1-10, XFLAG1-20, XSTR1-19):
          Generic overflow columns for non-standard fields
    """
    # merge_row_generator: All values empty string — represents an "empty" record
    # used when the source SOT file did not arrive or contains no data
    merge_row_generator = {
        "RECTYPEID": [""], "SIDE": [""], "PAGENO": [""], "CONTIND": [""],
        "DESTINATIONID": [""], "TERMINALID": [""],
        "OURREF": [""], "THEIRREF": [""], "SUBACC": [""], "FROMDATE": [""], "TODATE": [""],
        "SECURITY": [""], "SECURITYDESCRIPTION": [""], "OPBAL": [""], "CLBAL": [""],
        "NOOFPARTS": [""],
        "SNDRTORCVR": [""], "SOURCE_OF_PRICE": [""], "PRICE_QUOTATION_DATE": [""],
        "TRADE_DATE": [""], "CASH_VALUE_DATE": [""],
        "CONTRACTUAL_SETTLEMENT_DATE": [""], "SENDERS_REFERENCE": [""],
        "MESSAGE_FUNCTION": [""], "COPY_DUPLICATE_FLAG": [""], "PREPARATION_DATE": [""],
        "RELATED_REFERENCE": [""], "STAMP_DUTY_EXCEPTION": [""], "ASSET_TYPE": [""],
        "DEAL_QUANTITY": [""], "REFERENCE": [""],
        "SENDER_TO_RECEIVER_INFORMATION": [""], "SOURCE_CODE": [""],
        "CATEGORY_CODE": [""], "TRANSACTION_CODE": [""], "NET_PROCEEDS": [""],
        "NET_PROCEEDS_CURRENCY": [""], "BUYERS_SAFEKEEPING_ACCOUNT": [""],
        "PLACE_OF_SETTLEMENT": [""], "RECEIVERS_CUSTODIAN": [""],
        "DEAL_AMOUNT": [""], "DEAL_CURRENCY": [""], "PRICE": [""], "PRICE_CURRENCY": [""],
        "ACCRUED_INTEREST": [""], "ACCRUED_INTEREST_CURRENCY": [""],
        "ACCRUED_DAYS": [""], "EXCHANGE_RATE": [""], "PLACE_OF_TRADE": [""],
        "NEXT_COUPON": [""], "EXECUTING_BROKER": [""],
        "BENEFICIARY_OF_SECURITIES": [""], "RECEIVER_OR_DELIVERER_OF_SECURITIES": [""],
        "BENEFICIARY": [""], "PLACE_OF_SAFEKEEPING_CODE": [""],
        "PLACE_OF_SAFEKEEPING": [""],
        "DELIVERERS_INSTRUCTING_PARTY": [""], "SIGN": [""], "ALTERNATE_ID": [""],
        "AUDIT_STATUS": [""], "BENEFICIAL_OWNERSHIP_OVERRIDE": [""],
        "CHARGES_FEES": [""], "CHARGES_FEES_CURRENCY": [""], "CODE": [""],
        "COMPLETE_DELTA": [""], "CONDITION_INDICATOR": [""],
        "CONSOLIDATED": [""], "CORPORATE_ACTION_EVENT_INDICATOR": [""],
        "DEAL_PRICE": [""], "DEAL_PRICE_CURRENCY": [""],
        "EXECUTING_BROKERS_AMOUNT": [""], "EXECUTING_BROKERS_CURRENCY": [""],
        "FORM_OF_SECURITIES": [""], "NET_AMOUNT": [""],
        "OTHER_AMOUNT": [""], "OTHER_AMOUNT_CURRENCY": [""],
        "PARTY_CAPACITY": [""], "PAYMENT_INDICATOR": [""], "POSITION_BASIS": [""],
        "PROBLEM_DATE": [""], "PROCESSING_INDICATOR": [""],
        "REGISTRATION_OVERRIDE": [""], "REPORTING_INDICATOR": [""],
        "STAMP_DUTY": [""], "STAMP_DUTY_CURRENCY": [""], "STATEMENT_FREQUENCY": [""],
        "ACCRUED_AMOUNT": [""], "ACCRUED_AMOUNT_BASE": [""],
        "ACCRUED_AMOUNT_BASE_CURRENCY": [""], "ACCRUED_AMOUNT_CURRENCY": [""],
        "AMOUNT": [""], "CURRENCY": [""],
        "AMOUNT_BASE": [""], "AMOUNT_BASE_CURRENCY": [""], "BOOK": [""],
        "COST_CURRENCY": [""], "DEAL_PRICE_BASE": [""], "DEAL_PRICE_BASE_CURRENCY": [""],
        "EXCHANGE": [""], "INSTRUMENT_CLASS": [""], "INSTRUMENT_SUB_CLASS": [""],
        "LINK_UNLINK_TYPE": [""], "PRICE_BASE": [""], "PRICE_BASE_CURRENCY": [""],
        "REFERENCE_1": [""], "REFERENCE_2": [""], "REFERENCE_3": [""],
        "REFERENCE_4": [""], "REFERENCE_4_OVERFLOW": [""], "SOURCE_SYSTEM": [""],
        # Extended generic fields — used for non-standard/overflow data from source systems
        "XAMT1": [""], "XAMT2": [""], "XAMT3": [""], "XAMT4": [""], "XAMT5": [""],
        "XAMT6": [""], "XAMT7": [""], "XAMT8": [""], "XAMT9": [""], "XAMT10": [""],
        "XCCY1": [""], "XCCY2": [""], "XCCY3": [""], "XCCY4": [""], "XCCY5": [""],
        "XCCY6": [""], "XCCY7": [""], "XCCY8": [""], "XCCY9": [""], "XCCY10": [""],
        "XDATE1": [""], "XDATE2": [""], "XDATE3": [""], "XDATE4": [""], "XDATE5": [""],
        "XDATE6": [""], "XDATE7": [""], "XDATE8": [""], "XDATE9": [""], "XDATE10": [""],
        "XFLAG1": [""], "XFLAG2": [""], "XFLAG3": [""], "XFLAG4": [""], "XFLAG5": [""],
        "XFLAG6": [""], "XFLAG7": [""], "XFLAG8": [""], "XFLAG9": [""], "XFLAG10": [""],
        "XFLAG11": [""], "XFLAG12": [""], "XFLAG13": [""], "XFLAG14": [""], "XFLAG15": [""],
        "XFLAG16": [""], "XFLAG17": [""], "XFLAG18": [""], "XFLAG19": [""], "XFLAG20": [""],
        "XSTR1": [""], "XSTR2": [""], "XSTR3": [""], "XSTR4": [""], "XSTR5": [""],
        "XSTR6": [""], "XSTR7": [""], "XSTR8": [""], "XSTR9": [""], "XSTR10": [""],
        "XSTR11": [""], "XSTR12": [""], "XSTR13": [""], "XSTR14": [""], "XSTR15": [""],
        "XSTR16": [""], "XSTR17": [""], "XSTR18": [""], "XSTR19": [""]
    }

    # funnel_row_generator: each key maps to itself — used for column name alignment
    # in downstream funnel/mapping logic (column value = column name)
    funnel_row_generator = {
        "SIDE": ["SIDE"], "PAGENO": ["PAGENO"], "CONTIND": ["CONTIND"],
        "DESTINATIONID": ["DESTINATIONID"],
        "THEIRREF": ["THEIRREF"], "SUBACC": ["SUBACC"],
        "FROMDATE": ["FROMDATE"], "TODATE": ["TODATE"],
        "SECURITY": ["SECURITY"],
        "OPBAL": ["OPBAL"], "CLBAL": ["CLBAL"], "NOOFPARTS": ["NOOFPARTS"],
        "SNDRTORCVR": ["SNDRTORCVR"], "SOURCE_OF_PRICE": ["SOURCE_OF_PRICE"],
        "PRICE_QUOTATION_DATE": ["PRICE_QUOTATION_DATE"],
        "TRADE_DATE": ["TRADE_DATE"], "CASH_VALUE_DATE": ["CASH_VALUE_DATE"],
        "CONTRACTUAL_SETTLEMENT_DATE": ["CONTRACTUAL_SETTLEMENT_DATE"],
        "SENDERS_REFERENCE": ["SENDERS_REFERENCE"],
        "MESSAGE_FUNCTION": ["MESSAGE_FUNCTION"],
        "COPY_DUPLICATE_FLAG": ["COPY_DUPLICATE_FLAG"],
        "PREPARATION_DATE": ["PREPARATION_DATE"],
        "RELATED_REFERENCE": ["RELATED_REFERENCE"],
        "STAMP_DUTY_EXCEPTION": ["STAMP_DUTY_EXCEPTION"],
        "ASSET_TYPE": ["ASSET_TYPE"], "DEAL_QUANTITY": ["DEAL_QUANTITY"],
        "REFERENCE": ["REFERENCE"],
        "SENDER_TO_RECEIVER_INFORMATION": ["SENDER_TO_RECEIVER_INFORMATION"],
        "SOURCE_CODE": ["SOURCE_CODE"], "CATEGORY_CODE": ["CATEGORY_CODE"],
        "NET_PROCEEDS": ["NET_PROCEEDS"],
        "NET_PROCEEDS_CURRENCY": ["NET_PROCEEDS_CURRENCY"],
        "BUYERS_SAFEKEEPING_ACCOUNT": ["BUYERS_SAFEKEEPING_ACCOUNT"],
        "PLACE_OF_SETTLEMENT": ["PLACE_OF_SETTLEMENT"],
        "RECEIVERS_CUSTODIAN": ["RECEIVERS_CUSTODIAN"],
        "DEAL_AMOUNT": ["DEAL_AMOUNT"], "DEAL_CURRENCY": ["DEAL_CURRENCY"],
        "PRICE": ["PRICE"], "PRICE_CURRENCY": ["PRICE_CURRENCY"],
        "ACCRUED_INTEREST_CURRENCY": ["ACCRUED_INTEREST_CURRENCY"],
        "ACCRUED_DAYS": ["ACCRUED_DAYS"], "EXCHANGE_RATE": ["EXCHANGE_RATE"],
        "EXECUTING_BROKER": ["EXECUTING_BROKER"],
        "BENEFICIARY_OF_SECURITIES": ["BENEFICIARY_OF_SECURITIES"],
        "BENEFICIARY": ["BENEFICIARY"],
        "PLACE_OF_SAFEKEEPING_CODE": ["PLACE_OF_SAFEKEEPING_CODE"],
        "PLACE_OF_SAFEKEEPING": ["PLACE_OF_SAFEKEEPING"],
        "AUDIT_STATUS": ["AUDIT_STATUS"],
        "BENEFICIAL_OWNERSHIP_OVERRIDE": ["BENEFICIAL_OWNERSHIP_OVERRIDE"],
        "CHARGES_FEES": ["CHARGES_FEES"],
        "CONDITION_INDICATOR": ["CONDITION_INDICATOR"],
        "CONSOLIDATED": ["CONSOLIDATED"],
        "CORPORATE_ACTION_EVENT_INDICATOR": ["CORPORATE_ACTION_EVENT_INDICATOR"],
        "DEAL_PRICE": ["DEAL_PRICE"],
        "EXECUTING_BROKERS_CURRENCY": ["EXECUTING_BROKERS_CURRENCY"],
        "FORM_OF_SECURITIES": ["FORM_OF_SECURITIES"],
        "OTHER_AMOUNT_CURRENCY": ["OTHER_AMOUNT_CURRENCY"],
        "PARTY_CAPACITY": ["PARTY_CAPACITY"], "PAYMENT_INDICATOR": ["PAYMENT_INDICATOR"],
        "POSITION_BASIS": ["POSITION_BASIS"],
        "PROBLEM_DATE": ["PROBLEM_DATE"], "PROCESSING_INDICATOR": ["PROCESSING_INDICATOR"],
        "REGISTRATION_OVERRIDE": ["REGISTRATION_OVERRIDE"],
        "STAMP_DUTY": ["STAMP_DUTY"], "STAMP_DUTY_CURRENCY": ["STAMP_DUTY_CURRENCY"],
        "STATEMENT_FREQUENCY": ["STATEMENT_FREQUENCY"],
        "ACCRUED_AMOUNT_BASE": ["ACCRUED_AMOUNT_BASE"],
        "ACCRUED_AMOUNT_BASE_CURRENCY": ["ACCRUED_AMOUNT_BASE_CURRENCY"],
        "ACCRUED_AMOUNT_CURRENCY": ["ACCRUED_AMOUNT_CURRENCY"],
        "AMOUNT": ["AMOUNT"], "CURRENCY": ["CURRENCY"],
        "AMOUNT_BASE": ["AMOUNT_BASE"], "AMOUNT_BASE_CURRENCY": ["AMOUNT_BASE_CURRENCY"],
        "BOOK": ["BOOK"], "COST_CURRENCY": ["COST_CURRENCY"],
        "DEAL_PRICE_BASE_CURRENCY": ["DEAL_PRICE_BASE_CURRENCY"],
        "EXCHANGE": ["EXCHANGE"], "INSTRUMENT_CLASS": ["INSTRUMENT_CLASS"],
        "INSTRUMENT_SUB_CLASS": ["INSTRUMENT_SUB_CLASS"],
        "LINK_UNLINK_TYPE": ["LINK_UNLINK_TYPE"], "PRICE_BASE": ["PRICE_BASE"],
        "PRICE_BASE_CURRENCY": ["PRICE_BASE_CURRENCY"],
        "REFERENCE_1": ["REFERENCE_1"], "REFERENCE_2": ["REFERENCE_2"],
        "REFERENCE_3": ["REFERENCE_3"], "REFERENCE_4": ["REFERENCE_4"],
        "REFERENCE_4_OVERFLOW": ["REFERENCE_4_OVERFLOW"],
        "SOURCE_SYSTEM": ["SOURCE_SYSTEM"],
        "XAMT1": ["XAMT1"], "XAMT2": ["XAMT2"], "XAMT3": ["XAMT3"],
        "XAMT4": ["XAMT4"], "XAMT5": ["XAMT5"], "XAMT6": ["XAMT6"],
        "XAMT7": ["XAMT7"], "XAMT8": ["XAMT8"], "XAMT9": ["XAMT9"], "XAMT10": ["XAMT10"],
        "XCCY1": ["XCCY1"], "XCCY2": ["XCCY2"], "XCCY3": ["XCCY3"],
        "XCCY4": ["XCCY4"], "XCCY5": ["XCCY5"], "XCCY6": ["XCCY6"],
        "XCCY7": ["XCCY7"], "XCCY8": ["XCCY8"], "XCCY9": ["XCCY9"], "XCCY10": ["XCCY10"],
        "XDATE1": ["XDATE1"], "XDATE2": ["XDATE2"], "XDATE3": ["XDATE3"],
        "XDATE4": ["XDATE4"], "XDATE5": ["XDATE5"], "XDATE6": ["XDATE6"],
        "XDATE7": ["XDATE7"], "XDATE8": ["XDATE8"], "XDATE9": ["XDATE9"], "XDATE10": ["XDATE10"],
        "XFLAG1": ["XFLAG1"], "XFLAG2": ["XFLAG2"], "XFLAG3": ["XFLAG3"],
        "XFLAG4": ["XFLAG4"], "XFLAG5": ["XFLAG5"], "XFLAG6": ["XFLAG6"],
        "XFLAG7": ["XFLAG7"], "XFLAG8": ["XFLAG8"], "XFLAG9": ["XFLAG9"], "XFLAG10": ["XFLAG10"],
        "XFLAG11": ["XFLAG11"], "XFLAG12": ["XFLAG12"], "XFLAG13": ["XFLAG13"],
        "XFLAG14": ["XFLAG14"], "XFLAG15": ["XFLAG15"], "XFLAG16": ["XFLAG16"],
        "XFLAG17": ["XFLAG17"], "XFLAG18": ["XFLAG18"], "XFLAG19": ["XFLAG19"],
        "XSTR1": ["XSTR1"], "XSTR2": ["XSTR2"], "XSTR3": ["XSTR3"],
        "XSTR4": ["XSTR4"], "XSTR5": ["XSTR5"], "XSTR6": ["XSTR6"],
        "XSTR7": ["XSTR7"], "XSTR8": ["XSTR8"], "XSTR9": ["XSTR9"], "XSTR10": ["XSTR10"],
        "XSTR11": ["XSTR11"], "XSTR12": ["XSTR12"], "XSTR13": ["XSTR13"],
        "XSTR14": ["XSTR14"], "XSTR15": ["XSTR15"], "XSTR16": ["XSTR16"],
        "XSTR17": ["XSTR17"], "XSTR18": ["XSTR18"], "XSTR19": ["XSTR19"]
    }

    # Convert to pandas then to PySpark (small single-row DataFrames — no performance concern)
    df1_pandas = pd.DataFrame(merge_row_generator)
    df2_pandas = pd.DataFrame(funnel_row_generator)
    df1        = spark.createDataFrame(df1_pandas)
    df2        = spark.createDataFrame(df2_pandas)

    return df1, df2


def dummy_SOP():
    """
    Returns dummy (empty schema) DataFrames for the SOP (Statement of Position) feed.

    Structure mirrors dummy_SOT() — same pattern:
        df1 → All-empty merge DataFrame (empty SOP record)
        df2 → Funnel mapping DataFrame (column → column name)

    SOP-specific columns include position-related fields:
        QUANTITY, AVAILABLE_POSITION, POSITION_VALUE, PORTFOLIO_CODE,
        ON_LOAN_QUANTITY, AGGREGATE_QUANTITY, STMTBASIS, etc.

    Returns:
        (df1, df2) tuple of PySpark DataFrames
    """
    SOP_RowGenerator_merge = {
        'RECTYPEID': [''], 'SIDE': [''], 'PAGENO': [''], 'CONTIND': [''],
        'DESTINATIONID': [''], 'TERMINALID': [''],
        'STMTBASIS': [''], 'ASSET_TYPE': [''], 'SECURITY': [''],
        'ASSET_DESCRIPTION': [''], 'QUANTITY': [''], 'AVAILABLE_POSITION': [''],
        'PRICE': [''], 'PRICE_CURRENCY': [''], 'POSITION_VALUE': [''],
        'POSITION_VALUE_CURRENCY': [''], 'EXCHANGE_RATE': [''],
        'ACCRUED_AMOUNT': [''], 'ACCRUED_AMOUNT_CURRENCY': [''],
        'CATEGORY_CODE': [''], 'SENDERS_REFERENCE': [''],
        'MESSAGE_FUNCTION': [''], 'COPY_DUPLICATE_SUBCODE': [''],
        'PREPARATION_DATE': [''], 'STATEMENT_FREQUENCY': [''],
        'RELATED_REFERENCE': [''], 'COMPLETE_DELTA': [''],
        'ON_LOAN_QUANTITY': [''],
        'SENDER_TO_RECEIVER_INFORMATION': [''], 'PORTFOLIO_CODE': [''],
        'TRANSACTION_CODE': [''],
        'FORM_OF_SECURITIES': [''],
        'PLACE_OF_SAFEKEEPING': [''], 'AGGREGATE_QUANTITY': [''],
        'ACCRUED_AMOUNT_BASE': [''], 'ACCRUED_AMOUNT_BASE_CURRENCY': [''],
        'ACCRUED_AMOUNT_CURRENCY': [''], 'AMOUNT': [''], 'CURRENCY': [''],
        'AMOUNT_BASE': [''], 'AMOUNT_BASE_CURRENCY': [''],
        'BOOK': [''], 'COST_CURRENCY': [''],
        'DEAL_PRICE': [''], 'DEAL_PRICE_BASE': [''], 'DEAL_PRICE_BASE_CURRENCY': [''],
        'DEAL_PRICE_CURRENCY': [''],
        'INSTRUMENT_CLASS': [''], 'INSTRUMENT_SUB_CLASS': [''],
        'PRICE_BASE': [''], 'PRICE_BASE_CURRENCY': [''],
        'REFERENCE_1': [''], 'REFERENCE_2': [''], 'REFERENCE_3': [''],
        'REFERENCE_4': [''], 'REFERENCE_4_OVERFLOW': [''], 'SOURCE_SYSTEM': [''],
        # Extended generic fields (same pattern as SOT)
        'XAMT1': [''], 'XAMT2': [''], 'XAMT3': [''], 'XAMT4': [''], 'XAMT5': [''],
        'XAMT6': [''], 'XAMT7': [''], 'XAMT8': [''], 'XAMT9': [''], 'XAMT10': [''],
        'XCCY1': [''], 'XCCY2': [''], 'XCCY3': [''], 'XCCY4': [''], 'XCCY5': [''],
        'XCCY6': [''], 'XCCY7': [''], 'XCCY8': [''], 'XCCY9': [''], 'XCCY10': [''],
        'XDATE1': [''], 'XDATE2': [''], 'XDATE3': [''], 'XDATE4': [''], 'XDATE5': [''],
        'XDATE6': [''], 'XDATE7': [''], 'XDATE8': [''], 'XDATE9': [''], 'XDATE10': [''],
        'XFLAG1': [''], 'XFLAG2': [''], 'XFLAG3': [''], 'XFLAG4': [''], 'XFLAG5': [''],
        'XFLAG6': [''], 'XFLAG7': [''], 'XFLAG8': [''], 'XFLAG9': [''], 'XFLAG10': [''],
        'XFLAG11': [''], 'XFLAG12': [''], 'XFLAG13': [''], 'XFLAG14': [''], 'XFLAG15': [''],
        'XFLAG16': [''], 'XFLAG17': [''], 'XFLAG18': [''], 'XFLAG19': [''], 'XFLAG20': [''],
        'XSTR1': [''], 'XSTR2': [''], 'XSTR3': [''], 'XSTR4': [''], 'XSTR5': [''],
        'XSTR6': [''], 'XSTR7': [''], 'XSTR8': [''], 'XSTR9': [''], 'XSTR10': [''],
        'XSTR11': [''], 'XSTR12': [''], 'XSTR13': [''], 'XSTR14': [''], 'XSTR15': [''],
        'XSTR16': [''], 'XSTR17': [''], 'XSTR18': [''], 'XSTR19': ['']
    }

    SOP_RowGenerator_funnel = {
        'SIDE': ['SIDE'], 'PAGENO': ['PAGENO'], 'CONTIND': ['CONTIND'],
        'DESTINATIONID': ['DESTINATIONID'],
        'SUBACC': ['SUBACC'], 'FROMDATE': ['FROMDATE'], 'TODATE': ['TODATE'],
        'STMTBASIS': ['STMTBASIS'], 'ASSET_TYPE': ['ASSET_TYPE'],
        'QUANTITY': ['QUANTITY'], 'AVAILABLE_POSITION': ['AVAILABLE_POSITION'],
        'POSITION_VALUE_CURRENCY': ['POSITION_VALUE_CURRENCY'],
        'EXCHANGE_RATE': ['EXCHANGE_RATE'],
        'ACCRUED_AMOUNT_CURRENCY': ['ACCRUED_AMOUNT_CURRENCY'],
        'CATEGORY_CODE': ['CATEGORY_CODE'], 'SENDERS_REFERENCE': ['SENDERS_REFERENCE'],
        'MESSAGE_FUNCTION': ['MESSAGE_FUNCTION'],
        'COMPLETE_DELTA': ['COMPLETE_DELTA'], 'STATEMENT_FREQUENCY': ['STATEMENT_FREQUENCY'],
        'RELATED_REFERENCE': ['RELATED_REFERENCE'],
        'ON_LOAN_QUANTITY': ['ON_LOAN_QUANTITY'],
        'SENDER_TO_RECEIVER_INFORMATION': ['SENDER_TO_RECEIVER_INFORMATION'],
        'PLACE_OF_SAFEKEEPING': ['PLACE_OF_SAFEKEEPING'],
        'AGGREGATE_QUANTITY': ['AGGREGATE_QUANTITY'],
        'ACCRUED_AMOUNT_CURRENCY': ['ACCRUED_AMOUNT_CURRENCY'],
        'AMOUNT': ['AMOUNT'], 'CURRENCY': ['CURRENCY'],
        'AMOUNT_BASE': ['AMOUNT_BASE'], 'AMOUNT_BASE_CURRENCY': ['AMOUNT_BASE_CURRENCY'],
        'DEAL_PRICE_BASE_CURRENCY': ['DEAL_PRICE_BASE_CURRENCY'],
        'DEAL_PRICE_CURRENCY': ['DEAL_PRICE_CURRENCY'],
        'PRICE_BASE_CURRENCY': ['PRICE_BASE_CURRENCY'],
        'REFERENCE_1': ['REFERENCE_1'], 'REFERENCE_2': ['REFERENCE_2'],
        'XAMT2': ['XAMT2'], 'XAMT3': ['XAMT3'], 'XAMT4': ['XAMT4'],
        'XAMT5': ['XAMT5'], 'XAMT6': ['XAMT6'],
        'XCCY7': ['XCCY7'], 'XCCY8': ['XCCY8'], 'XCCY9': ['XCCY9'], 'XCCY10': ['XCCY10'],
        'XDATE1': ['XDATE1'], 'XDATE2': ['XDATE2'],
        'XFLAG1': ['XFLAG1'], 'XFLAG2': ['XFLAG2'], 'XFLAG3': ['XFLAG3'],
        'XFLAG4': ['XFLAG4'], 'XFLAG5': ['XFLAG5'],
        'XFLAG15': ['XFLAG15'], 'XFLAG16': ['XFLAG16'],
        'XFLAG17': ['XFLAG17'], 'XFLAG18': ['XFLAG18'],
        'XSTR10': ['XSTR10'], 'XSTR11': ['XSTR11'], 'XSTR12': ['XSTR12'],
        'XSTR13': ['XSTR13'], 'XSTR14': ['XSTR14']
    }

    df1_pandas = pd.DataFrame(SOP_RowGenerator_merge)
    df2_pandas = pd.DataFrame(SOP_RowGenerator_funnel)
    df1        = spark.createDataFrame(df1_pandas)
    df2        = spark.createDataFrame(df2_pandas)

    return df1, df2


def SOC_dummy():
    """
    Returns dummy (empty schema) DataFrames for the SOC (Statement of Cash) feed.

    Structure mirrors dummy_SOT() / dummy_SOP() — same pattern.

    SOC-specific columns include:
        STMTNO, STMTPG (statement page number), VALUE_DATE,
        PRICE_PER_UNIT, DEAL_QUANTITY, DEAL_AMOUNT, NARRATIVE,
        ASSET_CODE, ASSET_DESCRIPTION

    Returns:
        (df1, df2) tuple of PySpark DataFrames
    """
    SOC_RowGenerator_merge = {
        "RECTYPEID": [""], "SIDE": [""], "STMTNO": [""], "STMTPG": [""],
        "DESTINATIONID": [""], "TERMINALID": [""],
        "SUBACC": [""], "FROMDATE": [""], "TODATE": [""],
        "SOURCE_CODE": [""], "TRANSACTION_CODE": [""],
        "ASSET_CODE": [""], "ASSET_DESCRIPTION": [""], "NARRATIVE": [""],
        "ASSET_TYPE": [""], "DEAL_QUANTITY": [""],
        "PRICE_PER_UNIT": [""], "PRICE_CURRENCY": [""],
        "DEAL_AMOUNT": [""], "DEAL_CURRENCY": [""],
        "EXCHANGE_RATE": [""], "VALUE_DATE": [""],
        "REFERENCE_1": [""], "REFERENCE_2": [""], "REFERENCE_3": [""],
        "AMOUNT_BASE": [""], "AMOUNT_BASE_CURRENCY": [""],
        "DEAL_PRICE": [""], "DEAL_PRICE_BASE": [""], "DEAL_PRICE_BASE_CURRENCY": [""],
        "PRICE_BASE": [""], "PRICE_BASE_CURRENCY": [""],
        "SOURCE_SYSTEM": [""],
        # Extended generic fields
        "XCCY1": [""], "XCCY2": [""], "XCCY3": [""], "XCCY4": [""], "XCCY5": [""],
        "XCCY6": [""], "XCCY7": [""], "XCCY8": [""], "XCCY9": [""], "XCCY10": [""],
        "XFLAG11": [""], "XFLAG12": [""], "XFLAG13": [""], "XFLAG14": [""],
        "XFLAG15": [""], "XFLAG16": [""], "XFLAG17": [""], "XFLAG18": [""],
        "XFLAG19": [""], "XFLAG20": [""]
    }

    SOC_RowGenerator_funnel = {
        "SIDE": ["SIDE"], "STMTNO": ["STMTNO"], "STMTPG": ["STMTPG"],
        "DESTINATIONID": ["DESTINATIONID"], "TERMINALID": ["TERMINALID"],
        "VALUE_DATE": ["VALUE_DATE"],
        "REFERENCE_1": ["REFERENCE_1"], "REFERENCE_2": ["REFERENCE_2"],
        "REFERENCE_3": ["REFERENCE_3"],
        "ASSET_TYPE": ["ASSET_TYPE"], "DEAL_QUANTITY": ["DEAL_QUANTITY"],
        "PRICE_PER_UNIT": ["PRICE_PER_UNIT"], "PRICE_CURRENCY": ["PRICE_CURRENCY"],
        "AMOUNT_BASE": ["AMOUNT_BASE"], "AMOUNT_BASE_CURRENCY": ["AMOUNT_BASE_CURRENCY"],
        "DEAL_PRICE": ["DEAL_PRICE"], "DEAL_PRICE_BASE_CURRENCY": ["DEAL_PRICE_BASE_CURRENCY"],
        "PRICE_BASE": ["PRICE_BASE"], "PRICE_BASE_CURRENCY": ["PRICE_BASE_CURRENCY"],
        "SOURCE_SYSTEM": ["SOURCE_SYSTEM"],
        "XCCY5": ["XCCY5"], "XCCY6": ["XCCY6"], "XCCY7": ["XCCY7"],
        "XCCY8": ["XCCY8"], "XCCY9": ["XCCY9"], "XCCY10": ["XCCY10"],
        "XDATE1": ["XDATE1"], "XDATE2": ["XDATE2"],
        "XFLAG5": ["XFLAG5"], "XFLAG6": ["XFLAG6"], "XFLAG7": ["XFLAG7"],
        "XFLAG8": ["XFLAG8"], "XFLAG9": ["XFLAG9"], "XFLAG10": ["XFLAG10"],
        "XSTR7": ["XSTR7"], "XSTR8": ["XSTR8"], "XSTR9": ["XSTR9"],
        "XSTR10": ["XSTR10"], "XSTR11": ["XSTR11"], "XSTR12": ["XSTR12"]
    }

    df1_pandas = pd.DataFrame(SOC_RowGenerator_merge)
    df2_pandas = pd.DataFrame(SOC_RowGenerator_funnel)
    df1        = spark.createDataFrame(df1_pandas)
    df2        = spark.createDataFrame(df2_pandas)

    return df1, df2