################################################################################
# BRNUD070.py
#
# Glue Job Name : BRNETL-UD070
# DataStage Job : CUST_0062_VG_SHR_VISTA_EX_ERR (Sequence + Parallel + Common + SO jobs)
#
# Purpose:
#   Modernized AWS Glue equivalent of three chained DataStage jobs:
#     1. Parallel Job  → Read mainframe fixed-width file, apply date transforms,
#                        join with DB2 NAV price lookups (current + historic),
#                        write intermediate output to S3.
#     2. Common Job    → Read intermediate output + CLIENTID / POID reference files,
#                        apply further transformations, write second intermediate to S3.
#     3. SO (SOT) Job  → Read common job output, apply SOT schema transformation
#                        via shared brn_sot_transformation module, write final
#                        SOT output to TLM_OUT/ prefix in S3.
#
# Data Flow:
#   BRN_IN/OMNI.CUST.INSBN008.TLMFILE_<YYMMDD>_*.txt
#       └─► Parallel transforms (Transformer 1 + DB2 joins)
#           └─► Intermediate/VG_SHR_VISTA_EX_ERR_common_<timestamp>.csv  (Parallel output)
#               └─► Common transforms (CLIENTID + POID lookups)
#                   └─► Intermediate/VG_SHR_VISTA_EX_ERR_common2_<timestamp>.csv (Common output)
#                       └─► SOT transformation
#                           └─► TLM_OUT/SOT_VG_SHR_VISTA_EX_ERR_<timestamp>.csv  (Final output)
#
# S3 Source Bucket : institutional-prod-us-east-1-brn-etl-s3-source/BRN_IN/
# S3 Target Bucket : institutional-prod-us-east-1-brn-etl-s3-source/TLM_OUT/
# Mainframe Input  : OMNI.CUST.INSBN008.TLMFILE_*.txt  (fixed-width, 53 chars/record)
# DB2 Dependencies : AINS00.VC_VGI_INT_INS, AINS00.VC_CURR_PRC_VGI, AINS00.VISS_PRC_VGI
# File Dependencies: POID.DATA.RFM_*.txt, CLIENTID.DATA.RFM_*.txt (from BRN_IN/)
#
# Glue Job Parameters (all required at runtime):
#   --configfileName   : Path to db2_config.json (uploaded to Glue job assets)
#   --secretdb2arn     : AWS Secrets Manager ARN holding DB2 credentials {'u':..., 'p':...}
#   --sourcebucketname : Source S3 bucket name
#   --targetbucketname : Target S3 bucket name
#   --env              : Deployment environment ('eng', 'test', 'prod')
#   --orderdate        : Processing date in YYMMDD format (e.g., '250115')
################################################################################

import sys
import json
import boto3
import logging
import brn_common_utils          # Shared utilities: Spark init, data_split, get_latest_file, output_store
import brn_common_transformation  # Shared common-job transformation logic
import brn_sot_transformation     # Shared SOT (Statement of Transactions) transformation logic

from pytz import timezone
from operator import itemgetter
from datetime import datetime, timedelta
import re

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ThreadPoolExecutor: used to load DB2 current + historic reference data in parallel
# (both queries are independent — running them concurrently halves the DB2 fetch latency)
from concurrent.futures import ThreadPoolExecutor

from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Configure Python logging → output appears in AWS Glue CloudWatch log group
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)


################################################################################
# SECTION 1 — Initialize Spark / Glue Context
################################################################################

# Delegates Spark + GlueContext initialization to the shared utility module.
# brn_common_utils.initialize_glue_spark() returns (sc, glueContext, spark).
# Wrapping in try/except ensures a clear error message if the Glue environment
# isn't set up correctly (e.g., missing JARs, wrong Python version).
try:
    sc, glueContext, spark = brn_common_utils.initialize_glue_spark()
except Exception as e:
    logging.error(f"Error initializing Spark session: {e}")
    raise


################################################################################
# SECTION 2 — Resolve Glue Job Parameters
################################################################################

# getResolvedOptions reads --param_name arguments passed to the Glue job at runtime.
# All parameters here are required — missing any will raise an exception immediately,
# preventing silent failures downstream.
try:
    src_path = getResolvedOptions(
        sys.argv,
        ['configfileName', 'secretdb2arn', 'sourcebucketname', 'targetbucketname', 'env']
    )
    config_file    = src_path['configfileName']      # Path to db2_config.json
    environment    = src_path['env']                 # 'eng', 'test', or 'prod'
    source_bucket  = src_path['sourcebucketname']    # BRN_IN source bucket
    target_bucket  = src_path['targetbucketname']    # TLM_OUT target bucket

    # Retrieve DB2 credentials from AWS Secrets Manager at runtime.
    # Credentials are never hardcoded — stored in Secrets Manager as JSON: {"u": "...", "p": "..."}
    db_secrets = json.loads(
        boto3.client("secretsmanager")
            .get_secret_value(SecretId=src_path['secretdb2arn'])['SecretString']
    )

    # orderdate is resolved separately — it arrives from Step Functions as --orderdate
    # Format: YYMMDD (e.g., '250115' = January 15, 2025)
    Odate = getResolvedOptions(sys.argv, ['orderdate'])['orderdate']

except Exception as e:
    logging.error(f"Error resolving options: {e}")
    raise


################################################################################
# SECTION 3 — Load DB2 Connection Config from JSON
################################################################################

def load_config(config_file):
    """
    Reads and parses the db2_config.json configuration file.

    The config file contains per-environment DB2 JDBC URLs, driver names,
    KMS keys, and default S3 paths. It is uploaded as a Glue job asset
    and its path is passed via the --configfileName job parameter.

    Args:
        config_file : Local path to db2_config.json

    Returns:
        dict: Full parsed config dict (all environments)

    Raises:
        Exception: If file is missing or JSON is malformed
    """
    try:
        with open(config_file, 'r') as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


# Load the full config and extract the section matching the active environment
config_data = load_config(config_file)

if environment not in config_data:
    raise ValueError(f"Environment '{environment}' not found in configuration file")

env_config       = config_data[environment]
jdbc_url         = env_config["jdbc_url"]          # e.g., jdbc:db2://<db2-host-prod>:<port>/<db>...
jdbc_driver_name = env_config["jdbc_driver_name"]  # com.ibm.db2.jcc.DB2Driver


################################################################################
# SECTION 4 — Script-Level Constants (DataStage Job Variables)
################################################################################

# These constants mirror the DataStage job-level variables from the original
# CUST_0062_VG_SHR_VISTA_EX_ERR sequence job. Centralizing them here makes
# maintenance easier — update once, applies everywhere in this script.
class config:
    Glue_Job_Name               = 'BRNETL-UD070'               # AWS Glue job identifier
    SO_Type                     = 'SOT'                         # Statement type for SOT transformation
    Source_file_Name            = 'OMNI.CUST.INSBN008.TLMFILE' # Mainframe source file prefix
    DSJobName                   = 'CUST_0062_SHR_VISTA_EX_ERR' # Original DataStage job name (reference only)

    # Output file name prefixes for each pipeline stage
    Parallel_Job_Target_File_Name = 'VG_SHR_VISTA_EX_ERR_common_'  # Stage 1 intermediate output
    Common_Job_Target_File_Name   = 'VG_SHR_VISTA_EX_ERR_common2_' # Stage 2 intermediate output
    SO_Target_File_Name           = 'SOT_VG_SHR_VISTA_EX_ERR_'     # Stage 3 final SOT output

    # S3 folder prefixes
    intermediate = 'Intermediate'  # Temporary inter-stage files
    incoming     = 'BRN_IN'        # Raw mainframe input files
    target       = 'TLM_OUT'       # Final output destination

    # Common reference file prefixes (CLIENTID and POID lookups)
    CLIENTID  = 'CLIENTID.DATA.RFM'   # Client ID mapping reference file
    POID      = 'POID.DATA.RFM'       # Portfolio / POID reference file

    # DB2 schema — all tables are in AINS00 schema
    db_schema = "AINS00"


################################################################################
# SECTION 5 — Job Status Logging
################################################################################

# Log the job start time in Eastern Time (America/New_York).
# Using pytz timezone ensures correct DST handling for the US East Coast environment.
logger.info(f'Job status for {config.Glue_Job_Name}')
now         = datetime.now(timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S.%f')
logger.info(f'Latest job_runtime for {config.Glue_Job_Name} is {str(now)}')

# target_date: timestamp used in output filenames to make them unique per run.
# Format: YYMMDD_HHMMSS  (e.g., '250115_143022')
# This prevents overwriting a prior run's output if the job is re-triggered on the same day.
target_date = datetime.now(timezone("America/New_York")).strftime('%y%m%d_%H%M%S')


################################################################################
# SECTION 6 — Stage 1: Parallel Job
#   Read mainframe fixed-width file → transform → join DB2 lookups → write intermediate
################################################################################

# ── 6a. Locate and validate the mainframe source file in S3 ──────────────────

try:
    # Build the file pattern: e.g., 'OMNI.CUST.INSBN008.TLMFILE_250115'
    # get_latest_file uses this as a prefix to find the most recently modified match
    file_pattern       = f"{config.Source_file_Name}_{Odate}"
    latest_source_file = brn_common_utils.get_latest_file(
        source_bucket, config.incoming, file_pattern
    )

    if latest_source_file is not None:
        logger.info(f'Mainframe source file is present in S3 bucket for Odate {Odate}')
    else:
        # Hard fail — no source file = no processing possible for this date
        raise ValueError(f'Mainframe source file is not present in S3 bucket for Odate {Odate}')
except Exception as e:
    logging.error(f'Error while reading glue job arguments : {e}')
    raise

# Build the full S3 URI for the source file
source_key = f"s3://{src_path['sourcebucketname']}/{config.incoming}/{latest_source_file}"
logger.info(f'Complete path of Mainframe source file in S3 bucket is {source_key}')


# ── 6b. Parse the fixed-width mainframe file into a typed DataFrame ───────────

# Fixed-width layout for OMNI.CUST.INSBN008.TLMFILE (53 chars per record):
#   Port_ID      : chars  1-4   (4)   Portfolio identifier
#   delimiter    : char   5     (1)   Field separator
#   Vista_Fund   : chars  6-11  (6)   Vista fund code
#   delimiter1   : char  12     (1)
#   Trade_Date   : chars 13-20  (8)   Trade date (MMDDYYYY from mainframe)
#   delimiter2   : char  21     (1)
#   Dollar_Amount: chars 22-42  (21)  Dollar amount (may include commas)
#   delimiter3   : char  43     (1)
#   Sign         : char  44     (1)   +/- sign for the dollar amount
#   delimiter4   : char  45     (1)
#   Process_Date : chars 46-53  (8)   Processing date (MMDDYYYY from mainframe)
#   delimiter5   : char  54     (1)
#
# Note: delimiter columns preserve the original pipe-separated format from the source file.
# brn_common_utils.data_split reads the S3 file and applies these offsets using PySpark substring().
columns = [
    'Port_ID', 'delimiter', 'Vista_Fund', 'delimiter1', 'Trade_Date', 'delimiter2',
    'Dollar_Amount', 'delimiter3', 'Sign', 'delimiter4', 'Process_Date', 'delimiter5'
]
lengths = [4, 1, 6, 1, 8, 1, 21, 1, 1, 1, 8, 1]  # Sum = 54 chars per record

df_parallel_job_source = brn_common_utils.data_split(columns, lengths, source_key, spark)
logger.info(f'Mainframe source file has {df_parallel_job_source.count()} records')


# ── 6c. Connect to DB2 and load NAV price reference data in parallel ──────────

# JDBC connection options — credentials from Secrets Manager, schema locked to AINS00.
# READ_UNCOMMITTED: avoids row-level locks on the DB2 reference tables,
# acceptable for lookup/reference queries that don't need transaction isolation.
jdbc_config = {
    "url":            jdbc_url,
    "driver":         jdbc_driver_name,
    "user":           db_secrets['u'],     # DB2 username from Secrets Manager
    "password":       db_secrets['p'],     # DB2 password from Secrets Manager
    "currentSchema":  config.db_schema,    # AINS00
    "isolationLevel": "READ_UNCOMMITTED"   # No lock contention on reference tables
}

# Current NAV prices: joins portfolio instrument table (VC_VGI_INT_INS) with
# current price table (VC_CURR_PRC_VGI) on INS_ID.
# Filters to USD NAV prices only — returns PORT_ID, effective date, and NAV unit amount.
current_query = """
SELECT
    PORT.PORT_ID,
    CURR.PRC_EFFTV_DT AS PRC_EFFTV_DT2,
    CAST(CURR.UNIT_AM AS VARCHAR(17)) AS UNIT_AM
FROM AINS00.VC_VGI_INT_INS PORT, AINS00.VC_CURR_PRC_VGI CURR
WHERE PORT.INS_ID   = CURR.INS_ID
AND   CURR.CURRCY_CD = 'USD'
AND   CURR.PRC_TYP_CD = 'NAV'
"""

# Historic NAV prices: same portfolio join, but against the historic price table (VISS_PRC_VGI).
# Used as a fallback lookup when no current price matches the trade date.
historic_query = """
SELECT
    PORT.PORT_ID,
    HPRC.PRC_EFFTV_DT AS PRC_EFFTV_DT1,
    CAST(HPRC.UNIT_AM AS VARCHAR(17)) AS UNIT_AM
FROM AINS00.VC_VGI_INT_INS PORT, AINS00.VISS_PRC_VGI HPRC
WHERE PORT.INS_ID    = HPRC.INS_ID
AND   HPRC.CURRCY_CD  = 'USD'
AND   HPRC.PRC_TYP_CD = 'NAV'
"""


def load_current_reference_data():
    """
    Loads current NAV price reference data from DB2 via JDBC.

    Returns:
        DataFrame: PORT_ID, PRC_EFFTV_DT2 (effective date), UNIT_AM (NAV unit amount)
    """
    logger.info(f"Current reference data loading started at {now}")
    return (
        spark.read.format("jdbc")
             .options(**jdbc_config)
             .option("query", current_query)
             .load()
    )


def load_historic_reference_data():
    """
    Loads historic NAV price reference data from DB2 via JDBC.

    Returns:
        DataFrame: PORT_ID, PRC_EFFTV_DT1 (effective date), UNIT_AM (historic NAV)
    """
    logger.info(f"Historic reference data loading started at {now}")
    return (
        spark.read.format("jdbc")
             .options(**jdbc_config)
             .option("query", historic_query)
             .load()
    )


# Submit both DB2 reads concurrently using ThreadPoolExecutor.
# ThreadPoolExecutor is safe here because each function creates its own JDBC connection.
# Running both in parallel avoids waiting for the first query to complete before
# the second starts — typical DB2 query latency is 30-120 seconds each.
with ThreadPoolExecutor() as executor:
    future_current  = executor.submit(load_current_reference_data)
    future_historic = executor.submit(load_historic_reference_data)

# Block and retrieve both results — .result() raises any exception from the thread
df_ref1_current  = future_current.result()
df_ref2_historic = future_historic.result()

logger.info(f'db2_Current file has {df_ref1_current.count()} records')
logger.info(f'db2_Historic file has {df_ref2_historic.count()} records')


# ── 6d. Parallel Job Transformations (DataStage Transformer 1) ───────────────

logger.info('Starting parallel job transformations as per Datastage logic')

# --- Transformer Stage Variables ---
# DataStage "stage variables" are intermediate computed columns used across multiple
# output columns. In PySpark they are computed as intermediate withColumn() steps.
#
# date  : Today's date in yyyyMMdd format (e.g., '20250115')
# date1 : Reformatted Process_Date → takes century (date[0:2]) + MMDD from Process_Date
#         Process_Date arrives as MMDDYYYY from mainframe, reformatted to YYMMDD for output
# date2 : Reformatted Trade_Date → parsed to SQL Date using century prefix + MMDD
df1 = df_parallel_job_source \
    .withColumn('date', date_format(current_date(), 'yyyyMMdd')) \
    .withColumn('date1',
        # Reconstruct Process_Date: century (chars 1-2 of today) + YY from Process_Date + MM + DD
        # Process_Date in source = MMDDYYYY → output = YYMMDD
        concat(
            col('date').substr(1, 2),          # Century from today's date (e.g., '20')
            col('Process_Date').substr(-2, 2), # YY = last 2 chars of Process_Date (YYYY)
            col('date').substr(1, 2),          # Century again (intentional — DataStage logic)
            col('Process_Date').substr(4, 2)   # MM = chars 4-5 of MMDDYYYY
        )
    ) \
    .withColumn('date2',
        # Reconstruct Trade_Date as SQL Date for join key matching with DB2 PRC_EFFTV_DT
        # Trade_Date in source = MMDDYYYY → output = yyyy-MM-dd SQL Date
        to_date(
            concat(
                col('date').substr(1, 2),         # Century (e.g., '20')
                col('Trade_Date').substr(-2, 2),  # YY = last 2 chars of MMDDYYYY
                lit('-'),
                col('Trade_Date').substr(1, 2),   # MM = chars 1-2 of MMDDYYYY
                lit('-'),
                col('Trade_Date').substr(4, 2)    # DD = chars 4-5 of MMDDYYYY
            ),
            'yyyy-MM-dd'
        )
    )

# --- Transformer Output Columns ---
# Clean and normalize all fields before the DB2 join.
# ROWNUM provides a stable row ID for ordering and downstream deduplication.
df2 = df1 \
    .withColumn('ROWNUM', monotonically_increasing_id()) \
    .withColumn('Port_Id',      trim(col('Port_ID'))) \
    .withColumn('Vista_Fund',   trim(col('Vista_Fund'))) \
    .withColumn('Trade_Date',   trim(col('date2'))) \
    .withColumn('Dollar_Amount',
        # Remove commas from dollar amounts (mainframe format: "1,234,567.89" → "1234567.89")
        regexp_replace(trim(col('Dollar_Amount')), ",", "")
    ) \
    .withColumn('Process_Date', trim(col('date1'))) \
    .select(
        'ROWNUM', 'Port_Id', 'delimiter', 'Vista_Fund', 'delimiter1',
        'Trade_Date', 'delimiter2', 'Dollar_Amount', 'delimiter3',
        'Sign', 'delimiter4', 'Process_Date', 'delimiter5'
    )

# --- Join with Current NAV Price Reference (LEFT OUTER) ---
# Left join preserves all source records even if no current price match exists.
# Join keys: Port_Id = PORT_ID AND Trade_Date = PRC_EFFTV_DT2
# Records with no current match will have null UNIT_AM — handled in the historic join below.
df3 = df2.join(
    df_ref1_current,
    (df2.Port_Id    == df_ref1_current.PORT_ID) &
    (df2.Trade_Date == df_ref1_current.PRC_EFFTV_DT2),
    how='left'
)

# --- Join with Historic NAV Price Reference (LEFT OUTER) ---
# Second fallback lookup: joins on Port_Id + Trade_Date against historic price table.
# Used to fill UNIT_AM for records that had no current price match.
df4 = df3.join(
    df_ref2_historic,
    (df3.Port_Id    == df_ref2_historic.PORT_ID) &
    (df3.Trade_Date == df_ref2_historic.PRC_EFFTV_DT1),
    how='left'
)

# --- Combine Current and Historic NAV using coalesce ---
# coalesce returns the first non-null value: prefer current NAV, fall back to historic.
# This mirrors the DataStage "If IsNull(current_price) Then historic_price Else current_price" logic.
df5 = df4.withColumn(
    'UNIT_AM_FINAL',
    coalesce(
        col('df_ref1_current.UNIT_AM'),   # Current NAV (preferred)
        col('df_ref2_historic.UNIT_AM')   # Historic NAV (fallback)
    )
)

# Write intermediate Stage 1 output to S3 (Intermediate folder).
# dependent_output_store uses pipe-delimited CSV + KMS encryption.
# target_date ensures filename uniqueness across runs on the same day.
logger.info('Writing Parallel Job output to S3 Intermediate folder')
brn_common_utils.dependent_output_store(
    datadf      = df5,
    split_no    = df5.count(),
    file_name   = config.Parallel_Job_Target_File_Name,  # 'VG_SHR_VISTA_EX_ERR_common_'
    extension   = 'csv',
    target_path = target_bucket,
    sub_folder  = config.intermediate,                   # 'Intermediate'
    folder_name = target_date                            # 'YYMMDD_HHMMSS' timestamp
)
logger.info(f'Parallel Job output written: {config.Parallel_Job_Target_File_Name}{target_date}.csv')


################################################################################
# SECTION 7 — Stage 2: Common Job
#   Read Stage 1 intermediate + CLIENTID + POID lookups → transform → write Stage 2 intermediate
################################################################################

# ── 7a. Read Stage 1 intermediate output ─────────────────────────────────────

# Locate the file written by the Parallel Job above using its known prefix + timestamp
parallel_output_pattern  = f"{config.Parallel_Job_Target_File_Name}{target_date}"
latest_parallel_file     = brn_common_utils.get_latest_file(
    target_bucket, config.intermediate, parallel_output_pattern
)

if latest_parallel_file is None:
    raise ValueError(f'Parallel job output file not found for pattern: {parallel_output_pattern}')

parallel_output_key = f"s3://{target_bucket}/{config.intermediate}/{latest_parallel_file}"
logger.info(f'Parallel job output file found: {parallel_output_key}')


# ── 7b. Load CLIENTID and POID common reference files ────────────────────────

# CLIENTID.DATA.RFM: maps client identifiers — used for client-level enrichment
clientid_pattern = f"{config.CLIENTID}_{Odate}"
latest_clientid_file = brn_common_utils.get_latest_file(
    source_bucket, config.incoming, clientid_pattern
)
if latest_clientid_file is None:
    raise ValueError(f'CLIENTID reference file not found for Odate: {Odate}')

clientid_key = f"s3://{source_bucket}/{config.incoming}/{latest_clientid_file}"
logger.info(f'CLIENTID reference file found: {clientid_key}')

# POID.DATA.RFM: maps portfolio order identifiers — used for POID enrichment
poid_pattern = f"{config.POID}_{Odate}"
latest_poid_file = brn_common_utils.get_latest_file(
    source_bucket, config.incoming, poid_pattern
)
if latest_poid_file is None:
    raise ValueError(f'POID reference file not found for Odate: {Odate}')

poid_key = f"s3://{source_bucket}/{config.incoming}/{latest_poid_file}"
logger.info(f'POID reference file found: {poid_key}')


# ── 7c. Apply Common Job transformations via shared module ────────────────────

# brn_common_transformation encapsulates the DataStage Common Job logic:
#   - Reads the parallel output + CLIENTID + POID files
#   - Applies lookup joins and field-level transformations
#   - Returns the enriched DataFrame ready for SOT transformation
logger.info('Starting Common Job transformations')
df_common_output = brn_common_transformation.transform(
    spark              = spark,
    parallel_output_key= parallel_output_key,
    clientid_key       = clientid_key,
    poid_key           = poid_key
)
logger.info(f'Common Job output has {df_common_output.count()} records')

# Write Stage 2 intermediate output to S3
brn_common_utils.dependent_output_store(
    datadf      = df_common_output,
    split_no    = df_common_output.count(),
    file_name   = config.Common_Job_Target_File_Name,   # 'VG_SHR_VISTA_EX_ERR_common2_'
    extension   = 'csv',
    target_path = target_bucket,
    sub_folder  = config.intermediate,
    folder_name = target_date
)
logger.info(f'Common Job output written: {config.Common_Job_Target_File_Name}{target_date}.csv')


################################################################################
# SECTION 8 — Stage 3: SO (SOT) Job
#   Read Stage 2 intermediate → apply SOT schema mapping → write final TLM_OUT output
################################################################################

# ── 8a. Read Stage 2 intermediate output ─────────────────────────────────────

common_output_pattern = f"{config.Common_Job_Target_File_Name}{target_date}"
latest_common_file    = brn_common_utils.get_latest_file(
    target_bucket, config.intermediate, common_output_pattern
)

if latest_common_file is None:
    raise ValueError(f'Common job output file not found for pattern: {common_output_pattern}')

common_output_key = f"s3://{target_bucket}/{config.intermediate}/{latest_common_file}"
logger.info(f'Common job output file found: {common_output_key}')


# ── 8b. Apply SOT transformation via shared module ───────────────────────────

# brn_sot_transformation maps the enriched Common output into the SOT (Statement of
# Transactions) schema format required by the downstream TLM system.
# SO_Type = 'SOT' controls which schema mapping the module applies.
logger.info('Starting SOT transformation')
df_sot_output = brn_sot_transformation.transform(
    spark             = spark,
    common_output_key = common_output_key,
    so_type           = config.SO_Type   # 'SOT'
)
logger.info(f'SOT Job output has {df_sot_output.count()} records')


# ── 8c. Write final SOT output to TLM_OUT/ ───────────────────────────────────

# output_store writes to a date-partitioned path:
#   TLM_OUT/YYYY/MM/DD/SOT_VG_SHR_VISTA_EX_ERR_<YYMMDD_HHMMSS>.csv
# This is the final deliverable consumed by the downstream TLM processing system.
brn_common_utils.output_store(
    datadf      = df_sot_output,
    file_name   = config.SO_Target_File_Name,   # 'SOT_VG_SHR_VISTA_EX_ERR_'
    extension   = 'csv',
    target_path = target_bucket,
    sub_folder  = config.target,                # 'TLM_OUT'
    folder_name = Odate                         # YYMMDD order date (used for partition path)
)
logger.info(
    f'SOT Job final output written to {config.target}/{config.SO_Target_File_Name}{Odate}.csv'
)
logger.info(f'BRNETL-UD070 completed successfully for Odate: {Odate}')