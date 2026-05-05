################################################################################
# BRNUD041.py
#
# Glue Job Name  : BRNETL-UD041
# DataStage Job  : CUST_0036_DC_DISB_CHK_EXC (Sequence + Parallel + Common + SOC jobs)
# Author         : mohammed_aamer@vanguard.com
#
# Purpose:
#   AWS Glue modernization of the DataStage CUST_0036_DC_DISB_CHK_EXC sequence job.
#   Processes DC (Disbursement Check) Exception records from the RECON.MICRO.FEED
#   mainframe fixed-width file and produces a SOC (Statement of Cash) output file
#   consumed by the downstream TLM system.
#
# Pipeline Stages:
#   1. Parallel Job  → Parse fixed-width RECON.MICRO.FEED file, apply Transformer 1
#                      (filter INSBC08x classes, derive SOC fields), join with
#                      PageNumber lookup, apply Transformer 2 (date reformatting,
#                      STMTNO derivation), write intermediate output + dummy control file
#   2. Common Job    → Read parallel output, join CLIENTID + POID reference files,
#                      derive ClientId/Poid enrichment columns, write second intermediate
#   3. SOC Job       → Filter for RECTYPEID='SOC', merge against SOC_dummy schema,
#                      sort, union with empty funnel frame, write final TLM_OUT file
#
# Data Flow:
#   BRN_IN/RECON.MICRO.FEED_<YYMMDD>_*.txt
#       └─► Parallel transforms (Transformer 1 + PageNumber join + Transformer 2)
#           ├─► Intermediate/DC_DISB_CHK_EXC_common_<timestamp>.txt  (Parallel output)
#           └─► Intermediate/DC_DISB_CHK_EXC_control_<timestamp>.txt (Dummy control)
#               └─► Common transforms (CLIENTID + POID enrichment)
#                   └─► Intermediate/DC_DISB_CHK_EXC_common2_<timestamp>.txt
#                       └─► SOC transformation
#                           └─► TLM_OUT/SOC_DC_DISB_CHK_EXC_<timestamp>.txt (Final output)
#
# S3 Source Bucket : vgi-institutional-eng-us-east-1-brn-etl-s3-source/BRN_IN/
# S3 Target Bucket : vgi-institutional-eng-us-east-1-brn-etl-s3-source/TLM_OUT/
# Mainframe Input  : RECON.MICRO.FEED_*.txt (fixed-width, variable-length fields)
# Dependencies     : PageNumber_*.ds, POID.DATA.RFM_*.txt, CLIENTID.DATA.RFM_*.txt
#
# Glue Job Parameters (all required at runtime):
#   --sourcebucketname : Source S3 bucket name
#   --targetbucketname : Target S3 bucket name
#   --filename         : Mainframe source file prefix (e.g., 'RECON.MICRO.FEED')
#   --required_files   : Comma-separated dependency file prefixes: "CLIENTID,POID,PageNumber"
#   --orderdate        : Processing date in YYMMDD format (e.g., '250115')
#
# Fixed-Width Layout (RECON.MICRO.FEED — 35 fields):
#   RecordType  [1]   Process_date [8]   Class       [8]   ItemID      [26]
#   PortIssueDt [8]   AvailPdDt    [8]   EqDT        [1]
#   AmtSgn_1    [1]   Amt_1    [13,2]    AmtSgn_2    [1]   Amt_2   [13,2]
#   AmtSgn_3    [1]   Amt_3    [13,2]    AmtSgn_4    [1]   Amt_4   [13,2]
#   AmtSgn_5    [1]   Amt_5    [13,2]    AmtSgn_6    [1]   Amt_6   [13,2]
#   AmtSgn_7    [1]   Amt_7    [13,2]    State       [2]   TranCode1   [3]
#   ActCode     [3]   CheckNum [10]       CheckNum2  [10]   Filler1     [4]
#   Port        [4]   Plan      [6]       SSN         [9]   SeqNum     [10]
#   BundleSeq   [3]   Bundle    [6]       Comment    [25]   Fund        [6]
################################################################################

import sys
import boto3
import logging
import Common_Script      # Shared utility module: spark_session, data_split, get_latest_file,
                          # dependent_output_store, output_store, delete_s3_file, SOC_dummy

from pytz import timezone
from operator import itemgetter
from datetime import datetime, timedelta
import re

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


################################################################################
# SECTION 1 — Logging Configuration
################################################################################

# Configure root logger at INFO level — output goes to AWS Glue CloudWatch log group.
# StreamHandler ensures messages also appear in the Glue console output stream.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)


################################################################################
# SECTION 2 — Initialize Spark Session & Glue Context
################################################################################

# Common_Script.spark_session() calls SparkContext.getOrCreate() and wraps it in
# a GlueContext, returning the active SparkSession. Using the shared utility here
# ensures consistent Spark configuration (maxResultSize, optimizer iterations)
# across all BRN Glue jobs that import Common_Script.
try:
    sc          = Common_Script.spark_session()
    glueContext = Common_Script.GlueContext(sc)
except Exception as e:
    logging.error(f"Error initializing Spark session or Glue context: {e}")
    raise


################################################################################
# SECTION 3 — Apply Spark Configuration
################################################################################

# Set Spark driver result size to the value defined in Common_Script (default '5G').
# This is required when using toPandas() or large collect() operations on the driver.
# Prevents OOM errors during the intermediate file writes.
try:
    spark = Common_Script.spark_session()
    Common_Script.sc._conf.set(
        "spark.driver.maxResultSize",
        Common_Script.spark_driver_maxResultSize   # '5G' — defined in Common_Script module level
    )
except Exception as e:
    logging.error(f"Error setting Spark configuration: {e}")
    raise


################################################################################
# SECTION 4 — Resolve Glue Job Parameters
################################################################################

# All parameters are passed at runtime by AWS Step Functions / Glue trigger.
# Splitting into separate getResolvedOptions calls mirrors the original script's
# grouping by parameter purpose (bucket paths, filename, dependency files, date).
try:
    # Bucket names — source (BRN_IN) and target (TLM_OUT)
    src_path      = getResolvedOptions(sys.argv, ['sourcebucketname', 'targetbucketname'])
    source_bucket = src_path['sourcebucketname']
    target_bucket = src_path['targetbucketname']

    # Mainframe source file prefix passed dynamically — allows reuse of this job
    # for different source files without code changes
    args1            = getResolvedOptions(sys.argv, ['filename'])
    Source_file_name = args1['filename']    # e.g., 'RECON.MICRO.FEED'

    # Dependency file prefixes passed as a single comma-separated string.
    # Lambda's check_files_in_s3() validates all three exist before this job runs.
    # Format: "CLIENTID.DATA.RFM,POID.DATA.RFM,PageNumber"
    args2                    = getResolvedOptions(sys.argv, ['required_files'])
    CLIENTID, POID, PageNumber = args2['required_files'].split(",")

    # Order date in YYMMDD format — used to find today's drop of all source/reference files
    args3 = getResolvedOptions(sys.argv, ['orderdate'])
    Odate = args3['orderdate']   # e.g., '250115'

except Exception as e:
    logging.error(f"Error resolving options: {e}")
    raise


################################################################################
# SECTION 5 — Script-Level Constants (DataStage Job Variables)
################################################################################

# Constants mirror the DataStage sequence job variables for CUST_0036_DC_DISB_CHK_EXC.
# Centralizing here makes file naming and folder routing easy to maintain.
class config:
    Glue_Job_Name               = 'BRNETL-UD041'              # AWS Glue job name
    SO_Type                     = 'SOC'                        # Statement type — Statement of Cash
    SO_Record                   = 'a'                          # Single-char dummy record value
    DSJobName                   = 'CUST_0036_DC_DISB_CHK_EXC' # Original DataStage job name (reference)

    # Output file prefixes for each pipeline stage
    Parallel_Job_Target_File_Name = 'DC_DISB_CHK_EXC_common_'   # Stage 1 parallel output
    Dummy_Target_File_Name        = 'DC_DISB_CHK_EXC_control_'  # Stage 1 control/dummy output
    Common_Job_Target_File_Name   = 'DC_DISB_CHK_EXC_common2_'  # Stage 2 common output
    SO_Target_File_Name           = 'SOC_DC_DISB_CHK_EXC_'      # Stage 3 final SOC output

    # S3 folder prefixes
    intermediate = 'Intermediate'   # Temporary inter-stage files
    incoming     = 'BRN_IN'         # Raw mainframe input files
    target       = 'TLM_OUT'        # Final output destination for TLM system


################################################################################
# SECTION 6 — Job Status Logging & Timestamp
################################################################################

# Log job start in Eastern Time — matches Vanguard's operational time zone.
# target_date is appended to all output filenames to guarantee uniqueness per run.
logger.info(f'Job status for {config.Glue_Job_Name}')
now         = datetime.now(timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S.%f')
logger.info(f'Latest job_runtime for {config.Glue_Job_Name} is {str(now)}')
target_date = datetime.now(timezone("America/New_York")).strftime('%Y%m%d_%H%M%S')


################################################################################
# SECTION 7 — Stage 1: Parallel Job
#   Parse fixed-width mainframe file → apply transformations → join PageNumber
#   → write intermediate + dummy control files
################################################################################

try:

    # ── 7a. Create minimal dummy DataFrame ─────────────────────────────────────
    # df_dummy: single-row, single-column DataFrame representing a control/sentinel record.
    # Written to S3 as a "control file" — consumed by downstream Step Function
    # to signal that this job's parallel stage ran successfully (even if 0 source records).
    # SO_Type='SOC', SO_Record='a' → { 'SOC': 'a' }
    df_dummy = spark.createDataFrame([{config.SO_Type: config.SO_Record}])


    # ── 7b. Locate and validate PageNumber reference file ─────────────────────
    # PageNumber file was generated by BRNORC127.py (Oracle → S3) and placed in
    # the Intermediate folder. It maps (pFile, processDate) → pageNo for STMTNO derivation.
    try:
        lookup_file_pattern = f"{PageNumber}_{Odate}"   # e.g., 'PageNumber_250115'
        PageNumber_file     = Common_Script.get_latest_file(
            source_bucket, config.intermediate, lookup_file_pattern
        )
        if PageNumber_file is not None:
            logger.info(f'PageNumber file is present in S3 bucket for Odate {Odate}')
        else:
            raise ValueError(f'PageNumber file is not present in S3 bucket for Odate {Odate}')
    except Exception as e:
        logging.error(f'Error while reading glue arguments: {e}')
        raise

    # Read PageNumber file — pipe-delimited, no header.
    # Schema: pFile (sub-account name), processDate (yyyymmdd string), pageNo (integer)
    lookup_key    = f"s3://{source_bucket}/{config.intermediate}/{PageNumber_file}"
    lookup_schema = StructType([
        StructField('pFile',       StringType(),  True),
        StructField('processDate', StringType(),  True),
        StructField('pageNo',      IntegerType(), True)
    ])
    df_lookup = spark.read.csv(lookup_key, sep="|", header=False, schema=lookup_schema)


    # ── 7c. Locate and validate mainframe source file ─────────────────────────
    try:
        file_pattern       = f"{Source_file_name}_{Odate}"   # e.g., 'RECON.MICRO.FEED_250115'
        latest_source_file = Common_Script.get_latest_file(
            source_bucket, config.incoming, file_pattern
        )
        if latest_source_file is not None:
            logger.info(f'Mainframe source file is present in S3 bucket for Odate {Odate}')
        else:
            raise ValueError(f'Mainframe source file is not present in S3 bucket for Odate {Odate}')
    except Exception as e:
        logging.error(f'Error while reading glue job arguments: {e}')
        raise

    source_key = f"s3://{source_bucket}/{config.incoming}/{latest_source_file}"
    logger.info(f'Complete path of Mainframe source file in S3 bucket is {source_key}')


    # ── 7d. Parse fixed-width mainframe file ──────────────────────────────────
    # Column definitions and byte-widths for RECON.MICRO.FEED.
    # [13, 2] entries are Decimal fields: 13 total digits, 2 after the decimal point.
    # Common_Script.data_split() reads the raw file as single-column text and uses
    # PySpark substring() to split each row at the specified offsets.
    columns = [
        'RecordType', 'Process_date', 'Class', 'ItemID', 'PortIssueDt', 'AvailPdDt', 'EqDT',
        'AmtSgn_1', 'Amt_1', 'AmtSgn_2', 'Amt_2', 'AmtSgn_3', 'Amt_3',
        'AmtSgn_4', 'Amt_4', 'AmtSgn_5', 'Amt_5', 'AmtSgn_6', 'Amt_6',
        'AmtSgn_7', 'Amt_7', 'State', 'TranCode1', 'ActCode',
        'CheckNum', 'CheckNum2', 'Filler1', 'Port', 'Plan', 'SSN',
        'SeqNum', 'BundleSeq', 'Bundle', 'Comment', 'Fund'
    ]
    lengths = [
        1, 8, 8, 26, 8, 8, 1,           # RecordType through EqDT
        1, [13, 2],                       # AmtSgn_1, Amt_1
        1, [13, 2],                       # AmtSgn_2, Amt_2
        1, [13, 2],                       # AmtSgn_3, Amt_3
        1, [13, 2],                       # AmtSgn_4, Amt_4
        1, [13, 2],                       # AmtSgn_5, Amt_5
        1, [13, 2],                       # AmtSgn_6, Amt_6
        1, [13, 2],                       # AmtSgn_7, Amt_7
        2, 3, 3, 10, 10, 4, 4, 6, 9, 10, # State through SeqNum
        3, 6, 25, 6                        # BundleSeq, Bundle, Comment, Fund
    ]
    # All fields extracted as VarChar (string) — Decimal casting is handled inside data_split()
    # for [13,2] entries; explicit string type used for all others
    data_types = ['VarChar'] * len(columns)

    # Schema for the raw text read — one column holding the entire fixed-width row
    schema = StructType([StructField('all_column_values', StringType(), True)])

    # data_split reads the S3 text file, then uses substring() to split each record
    # into named columns at the declared byte offsets
    df_parallel_job_source = Common_Script.data_split(
        spark.read
             .option("header", "false")
             .schema(schema)
             .option("inferSchema", "true")
             .text(source_key),
        lengths,
        data_types,
        columns
    )

    src_count = df_parallel_job_source.count()
    logger.info(f'Mainframe source file has {src_count} records')


    # ── 7e. Parallel Job Transformations — Transformer 1 ─────────────────────
    logger.info('Starting parallel job transformations as per DataStage logic')

    # Stage variable: cast Amt_1 to Decimal(13,2) and strip trailing zeros for display.
    # Amount is kept as string after cleanup — used as the SOC AMOUNT field downstream.
    df1 = df_parallel_job_source \
        .withColumn('Amount_1',
            col('Amt_1').cast('Decimal(13,2)')  # Explicit cast for arithmetic comparison
        ) \
        .withColumn('Amount',
            # Remove trailing zeros and optional decimal point (e.g., '100.00' → '100')
            regexp_replace(col('Amount_1').cast('string'), r'\.?0+$', '')
        )

    # Constraint filter — DataStage "Transformer 1 constraint":
    #   - Exclude header/trailer records (RecordType != '0')
    #   - Only non-zero amounts (Amount_1 != 0) — zero amounts are irrelevant for disbursement exceptions
    #   - Only specific INSBC08x class codes representing check exception types:
    #       INSBC08A = Paid Check Total
    #       INSBC08C = MICR Stop Check
    #       INSBC08D = MICR Conflicting Amount
    #       INSBC08E = MICR Check Number Discrepancy
    #       INSBC08F = MICR Void Check
    #       INSBC08G = MICR Previously Paid Check
    df2 = df1.filter(
        (trim(col('RecordType')) != '0') &
        (col('Amount_1') != 0) &
        (col('Class').isin('INSBC08A', 'INSBC08C', 'INSBC08D', 'INSBC08E', 'INSBC08F', 'INSBC08G'))
    )

    # Derive all SOC output columns from the filtered source DataFrame.
    # Literal constants mirror the DataStage Transformer 1 output column derivations.
    df3 = df2 \
        .withColumn('ROWNUM',      monotonically_increasing_id()) \
        .withColumn('OPBAL',       lit('0')) \
        .withColumn('CLBAL',       lit('0')) \
        .withColumn('STMTPG',      lit('1')) \
        .withColumn('OPBALSIGN',   lit('C')) \
        .withColumn('CLBALSIGN',   lit('C')) \
        .withColumn('SUBACC',      lit('DC_DISB_CHK_EXC'))    # Sub-account identifier for TLM routing
        .withColumn('OPBALTP',     lit('F')) \
        .withColumn('CLBALTP',     lit('F')) \
        .withColumn('CLNT_FLG',    lit('N')) \
        .withColumn('POID_FLG',    lit('N')) \
        .withColumn('OURREF',      lit('OURREF')) \
        .withColumn('RECTYPEID',   lit('SOC')) \
        .withColumn('XSTR1',       lit('THRU CHECK EXCEPTIONS')) \
        .withColumn('OPBALCY',     lit('USD')) \
        .withColumn('CLBALCY',     lit('USD')) \
        .withColumn('SIDE',        lit('L')) \
        .withColumn('SSNint',      lit('')) \
        .withColumn('PLANCHAR',    lit('')) \
        # XSTR9 carries the audit trail: job origin identifier for TLM traceability
        .withColumn('XSTR9',       concat(lit('RECJD078-'), lit(config.DSJobName))) \
        .withColumn('pfile',       lit('DC_DISB_CHK_EXC')) \
        .withColumn('REFERENCE_1', col('CheckNum')) \
        .withColumn('VALUEDATE',   col('PortIssueDt')) \
        .withColumn('OPBALDATE',   col('PortIssueDt')) \
        .withColumn('CLBALDATE',   col('PortIssueDt')) \
        .withColumn('AMOUNT',      col('Amount')) \
        # STMTNO weekend/holiday adjustment logic:
        #   If Saturday after noon (dayofweek=6, time > 12:00) OR
        #   Sunday before 3pm (dayofweek=1, time < 15:00) → STMTNO = '2'
        #   (weekend cutoff window — next business day statement number)
        .withColumn('STMTNO',
            when(
                ((dayofweek(current_date()) == 6) &
                 (date_format(current_timestamp(), "HH:mm:ss") > "12:00:00")) |
                ((dayofweek(current_date()) == 1) &
                 (date_format(current_timestamp(), "HH:mm:ss") < "15:00:00")),
                lit("2")
            )
        ) \
        # XDATE fields: conditionally set the AvailPdDt based on Class code
        # These represent different types of "available/effective" dates per exception type
        .withColumn('XDATE4',
            when(col('Class') == 'INSBC08C', col('AvailPdDt')).otherwise('')
        ) \
        .withColumn('XDATE3',
            when(col('Class') == 'INSBC08F', col('AvailPdDt')).otherwise('')
        ) \
        .withColumn('XDATE2',
            when(col('Class') == 'INSBC08G', col('AvailPdDt')).otherwise('')
        ) \
        # XDATE10: today's date with hyphens replaced by 'A' (TLM system date encoding)
        .withColumn('XDATE10',
            regexp_replace(current_date().cast('string'), '-', 'A')
        ) \
        # XDATE1: PortIssueDt for all classes except INSBC08A (paid checks don't carry issue date)
        .withColumn('XDATE1',
            when(col('Class') != 'INSBC08A', col('PortIssueDt')).otherwise('')
        ) \
        # DRORCR: Debit or Credit indicator — INSBC08A (Paid Check Total) is Credit; all others Debit
        .withColumn('DRORCR',
            when(col('Class') == 'INSBC08A', 'C').otherwise('D')
        ) \
        .withColumn('date', trim(col('PortIssueDt'))) \
        # ENTRYDATE: class-specific effective date for TLM entry posting
        #   INSBC08A, INSBC08E  → PortIssueDt (issue date = entry date)
        #   INSBC08C, INSBC08F, INSBC08G → EqDt (equalization date = entry date)
        #   INSBC08D             → AvailPdDt (available paid date = entry date)
        .withColumn('ENTRYDATE',
            when(col('Class') == 'INSBC08A', col('PortIssueDt'))
            .when(col('Class') == 'INSBC08C', col('EqDT'))
            .when(col('Class') == 'INSBC08D', col('AvailPdDt'))
            .when(col('Class') == 'INSBC08E', col('PortIssueDt'))
            .when(col('Class') == 'INSBC08F', col('EqDT'))
            .when(col('Class') == 'INSBC08G', col('EqDT'))
            .otherwise('')
        ) \
        # XSTR2: human-readable description of the check exception type for TLM narrative
        .withColumn('XSTR2',
            when(col('Class') == 'INSBC08A', 'PAID CHK TOTAL')
            .when(col('Class') == 'INSBC08C', 'MICR STOP CHK')
            .when(col('Class') == 'INSBC08D', 'MICR CONFLICTING AMT')
            .when(col('Class') == 'INSBC08E', 'MICR CHK NUMBER DISC')
            .when(col('Class') == 'INSBC08F', 'MICR VOID CHK')
            .when(col('Class') == 'INSBC08G', 'MICR PREV PAID CHK')
            .otherwise('')
        ) \
        .select(
            'ROWNUM', 'CLNT_FLG', 'PLANCHAR', 'POID_FLG', 'SSNint',
            'RECTYPEID', 'SIDE', 'SUBACC', 'STMTNO', 'STMTPG',
            'VALUEDATE', 'OPBALDATE', 'OPBAL', 'OPBALCY', 'OPBALSIGN', 'OPBALTP',
            'DRORCR', 'AMOUNT', 'CLBALDATE', 'CLBAL', 'CLBALCY', 'CLBALSIGN', 'CLBALTP',
            'ENTRYDATE', 'OURREF', 'XSTR1', 'XSTR2', 'XSTR9',
            'REFERENCE_1', 'XDATE1', 'XDATE2', 'XDATE3', 'XDATE4', 'XDATE10',
            'pfile', 'date'
        )


    # ── 7f. Join with PageNumber lookup (LEFT OUTER) ──────────────────────────
    # Join on date (PortIssueDt) + pfile (sub-account) to retrieve the pageNo
    # for each statement date. LEFT join preserves all source records even if
    # no matching PageNumber row exists for that date (e.g., first day of month).
    df4 = df3.join(
        df_lookup,
        (df3.date  == df_lookup.processDate) &
        (df3.pfile == df_lookup.pFile),
        how='left'
    ).select(
        'ROWNUM', 'CLNT_FLG', 'PLANCHAR', 'POID_FLG', 'SSNint',
        'RECTYPEID', 'SIDE', 'SUBACC', 'STMTNO', 'STMTPG',
        'VALUEDATE', 'OPBALDATE', 'OPBAL', 'OPBALCY', 'OPBALSIGN', 'OPBALTP',
        'DRORCR', 'AMOUNT', 'CLBALDATE', 'CLBAL', 'CLBALCY', 'CLBALSIGN', 'CLBALTP',
        'ENTRYDATE', 'OURREF', 'XSTR1', 'XSTR2', 'XSTR9',
        'REFERENCE_1', 'XDATE1', 'XDATE2', 'XDATE3', 'XDATE4', 'pageNo'
    )


    # ── 7g. Parallel Job Transformations — Transformer 2 ─────────────────────

    # pageNo derivation:
    #   - If no matching PageNumber row (null) → default to 1 (first page)
    #   - If matched → pageNo + 1 (next page number after the last known statement)
    df5 = df4.withColumn(
        'pageNo',
        when(col('pageNo').isNull(), 1)
        .otherwise(col('pageNo') + 1)
        .cast('int')
    )

    # Final Parallel Job output columns — complete date reformatting stage.
    # All dates are reformatted from yyyy-MM-dd (Spark default) to YYMMDD TLM format:
    #   Input: '2025-01-15'  →  Output: '250115'
    #   Logic: substr(-2,2) = YY from year end, substr(5,2) = MM, substr(1,4) = YYYY
    #   Actually used as: last2(year) + MM + first4(year) — matches DataStage concat pattern
    df_parallel_job_final = df5 \
        .withColumn('STMTNO', col('pageNo')) \
        .withColumn('SIGN',   col('DRORCR')) \
        # Date reformat helpers — convert yyyy-MM-dd ISO dates to YYMMDD compact format
        # by concatenating: [year last 2 chars] + [month chars 5-6] + [full year chars 1-4]
        .withColumn('XDATE6',
            concat(
                substring('XDATE1', -2, 2),   # YY
                substring('XDATE1',  5, 2),   # MM
                substring('XDATE1',  1, 4)    # YYYY
            )
        ) \
        .withColumn('XDATE7',
            concat(substring('XDATE2', -2, 2), substring('XDATE2', 5, 2), substring('XDATE2', 1, 4))
        ) \
        .withColumn('XDATE8',
            concat(substring('XDATE3', -2, 2), substring('XDATE3', 5, 2), substring('XDATE3', 1, 4))
        ) \
        .withColumn('XDATE9',
            concat(substring('XDATE4', -2, 2), substring('XDATE4', 5, 2), substring('XDATE4', 1, 4))
        ) \
        .withColumn('CLBALDATE',
            concat(substring('CLBALDATE', -2, 2), substring('CLBALDATE', 5, 2), substring('CLBALDATE', 1, 4))
        ) \
        .withColumn('ENTRY_DATE',
            concat(substring('ENTRYDATE', -2, 2), substring('ENTRYDATE', 5, 2), substring('ENTRYDATE', 1, 4))
        ) \
        .withColumn('OPBALDATE',
            concat(substring('OPBALDATE', -2, 2), substring('OPBALDATE', 5, 2), substring('OPBALDATE', 1, 4))
        ) \
        .withColumn('VALUE_DATE',
            concat(substring('VALUEDATE', -2, 2), substring('VALUEDATE', 5, 2), substring('VALUEDATE', 1, 4))
        ) \
        .withColumn('XFLAG3',     lit('')) \
        # CURRENCY: only populated when AMOUNT is non-zero (no currency for zero-value records)
        .withColumn('CURRENCY',
            when(col('AMOUNT') != '0', 'USD').otherwise('')
        ) \
        .withColumn('SOURCE_SYSTEM', col('XSTR9')) \
        # Shift XSTR values: XSTR3 gets what was XSTR2 (exception description),
        # XSTR2 gets what was XSTR1 ('THRU CHECK EXCEPTIONS')
        # This mirrors DataStage's output column remapping between Transformer stages
        .withColumn('XSTR3', col('XSTR2')) \
        .withColumn('XSTR2', col('XSTR1')) \
        .select(
            'ROWNUM', 'CLNT_FLG', 'PLANCHAR', 'POID_FLG', 'SSNint',
            'RECTYPEID', 'SIDE', 'SUBACC', 'STMTNO', 'STMTPG',
            'VALUE_DATE', 'OPBALDATE', 'OPBAL', 'OPBALCY', 'OPBALSIGN', 'OPBALTP',
            'SIGN', 'AMOUNT', 'CLBALDATE', 'CLBAL', 'CLBALCY', 'CLBALSIGN', 'CLBALTP',
            'ENTRY_DATE', 'OURREF', 'XDATE6', 'XDATE7', 'XDATE8', 'XDATE9',
            'REFERENCE_1', 'XSTR2', 'XSTR3', 'SOURCE_SYSTEM', 'XFLAG3', 'CURRENCY'
        )

    logger.info('Parallel job transformations ended')

    try:
        # Write Stage 1 parallel output to Intermediate/ with pipe-delimiter + KMS encryption.
        # This file is read by the Common Job stage below.
        logger.info('Storing parallel job output in intermediate path of S3 bucket')
        Common_Script.dependent_output_store(
            df_parallel_job_final,
            df_parallel_job_final.count(),
            config.Parallel_Job_Target_File_Name,   # 'DC_DISB_CHK_EXC_common_'
            'txt',
            target_bucket,
            config.intermediate,
            target_date
        )

        # Write dummy control file — single-row sentinel consumed by Step Functions
        # to confirm the parallel stage completed. Even if source has 0 valid records,
        # this file signals successful parallel stage execution.
        logger.info('Storing dummy dataframe in intermediate path of S3 bucket')
        Common_Script.dependent_output_store(
            df_dummy,
            df_dummy.count(),
            config.Dummy_Target_File_Name,          # 'DC_DISB_CHK_EXC_control_'
            'txt',
            target_bucket,
            config.intermediate,
            target_date
        )
    except Exception as e:
        logging.error(f'Error storing parallel job output: {e}')
        raise


    ############################################################################
    # SECTION 8 — Stage 2: Common Job
    #   Load CLIENTID + POID reference files → enrich parallel output → write intermediate
    ############################################################################

    # ── 8a. Load CLIENTID reference file ──────────────────────────────────────
    # Maps Plan ID → Client ID. Used to populate TLM's XFLAG5 (ClientId) field
    # for records where CLNT_FLG = 'Y'.
    try:
        client_file_pattern = f"{CLIENTID}_{Odate}"   # e.g., 'CLIENTID.DATA.RFM_250115'
        clientid_file       = Common_Script.get_latest_file(
            source_bucket, config.incoming, client_file_pattern
        )
        if clientid_file is not None:
            logger.info(f'CLIENTID file is present in S3 bucket for Odate {Odate}')
        else:
            raise ValueError(f'CLIENTID file is not present in S3 bucket for Odate {Odate}')
    except Exception as e:
        logging.error(f'Error while reading glue job arguments: {e}')
        raise

    # Read pipe-delimited CLIENTID file — columns: PLN_ID (plan/portfolio ID), CLNT_ID (client ID)
    clnt_key = f"s3://{source_bucket}/{config.incoming}/{clientid_file}"
    df_clnt  = spark.read.option("delimiter", "|").csv(clnt_key) \
                   .selectExpr('_c0 as PLN_ID', '_c1 as CLNT_ID')


    # ── 8b. Load POID reference file ──────────────────────────────────────────
    # Maps Participant ID (SSN) → Portfolio Order ID. Populates XFLAG6 (Poid)
    # for records where POID_FLG = 'Y'.
    try:
        poid_file_pattern = f"{POID}_{Odate}"   # e.g., 'POID.DATA.RFM_250115'
        poid_file         = Common_Script.get_latest_file(
            source_bucket, config.incoming, poid_file_pattern
        )
        if poid_file is not None:
            logger.info(f'POID file is present in S3 bucket for Odate {Odate}')
        else:
            raise ValueError(f'POID file is not present in S3 bucket for Odate {Odate}')
    except Exception as e:
        logging.error(f'Error while reading glue job arguments: {e}')
        raise

    # Read pipe-delimited POID file — columns: PART_ID (participant/SSN), POID (portfolio order ID)
    poid_key = f"s3://{source_bucket}/{config.incoming}/{poid_file}"
    df_poid  = spark.read.option("delimiter", "|").csv(poid_key) \
                   .selectExpr('_c0 as PART_ID', '_c1 as POID')


    # ── 8c. Common Job Transformations ────────────────────────────────────────
    logger.info('Starting common job transformations as per DataStage logic')

    # Transformer 1: initialize enrichment columns as empty — will be populated by joins
    df_C1 = df_parallel_job_final \
        .withColumn("PLN_ID",  trim(col("PLANCHAR"))) \
        .withColumn("CLNT_ID", lit("")) \
        .withColumn("POID",    lit(""))

    # Split into two streams based on CLNT_FLG:
    #   df_C2: records needing CLIENTID lookup (CLNT_FLG = 'Y')
    #   df_C3: records that bypass CLIENTID lookup (all others)
    df_C2 = df_C1.filter(col("CLNT_FLG") == 'Y')
    df_C3 = df_C1.filter(col("CLNT_FLG") != 'Y')

    # LEFT JOIN df_C2 with CLIENTID reference on PLN_ID to populate CLNT_ID.
    # drop() removes ambiguous duplicate columns from both sides of the join.
    df_C4 = df_C2 \
        .join(df_clnt, df_C2.PLN_ID == df_clnt.PLN_ID, "left") \
        .drop(df_C2.CLNT_ID) \
        .drop(df_clnt.PLN_ID)

    # Reunite enriched (CLNT_FLG='Y' after join) + bypassed (CLNT_FLG!='Y') records
    df_C5 = df_C4.union(df_C3)

    # Second split based on POID_FLG:
    #   df_C6: records needing POID lookup (POID_FLG = 'Y')
    #   df_C7: records that bypass POID lookup
    df_C6 = df_C5.filter(col("POID_FLG") == 'Y')
    df_C7 = df_C5.filter(col("POID_FLG") != 'Y')

    # LEFT JOIN df_C6 with POID reference on SSNint = PART_ID to populate POID.
    # SSNint (Social Security Number as integer string) is the participant join key.
    df_C8 = df_C6 \
        .join(df_poid, df_C6.SSNint == df_poid.PART_ID, "left") \
        .drop(df_poid.PART_ID) \
        .drop(df_C6.POID)

    # Reunite POID-enriched + bypassed records
    df_C9 = df_C8.union(df_C7)

    # Transformer 2: derive final enrichment fields
    df_C10 = df_C9 \
        .withColumn("ClientId",
            # CLNT_FLG='Y': take last 5 chars of CLNT_ID, left-pad to 5 digits with zeros
            # e.g., CLNT_ID='ABC12345' → '12345'; '123' → '00123'
            when(col("CLNT_FLG") == "Y",
                lpad(substring(trim(col("CLNT_ID")), -5, 5), 5, "0")
            ).otherwise(lit(""))
        ) \
        .withColumn("Poid",
            # POID_FLG='Y'  → use POID from reference join
            # POID_FLG not 'Y' and not 'N' → use POID_FLG value itself (passthrough)
            # POID_FLG='N'  → empty string
            when(col("POID_FLG") == "Y", col("POID"))
            .when((trim(col("POID_FLG")) != "Y") & (trim(col("POID_FLG")) != "N"), col("POID_FLG"))
            .otherwise(lit(""))
        )

    # Final Common Job columns — map enrichment fields to TLM XFLAG slots
    df_common_job_final = df_C10 \
        .withColumn("XFLAG5", col('ClientId')) \
        .withColumn("XFLAG6", col('Poid')) \
        # XSTR4: normalize XFLAG3 to 6 chars by prepending '0' if exactly 5 chars long
        # This handles fund codes that may be missing a leading zero
        .withColumn("XSTR4",
            when(length(trim(col("XFLAG3"))) == 5,
                concat(lit("0"), trim(col("XFLAG3")))
            ).otherwise(trim(col("XFLAG3")))
        )

    logger.info('Common job transformations ended')

    # Write Stage 2 intermediate output to Intermediate/
    logger.info('Storing common job output in intermediate path of S3 bucket')
    Common_Script.dependent_output_store(
        df_common_job_final,
        df_common_job_final.count(),
        config.Common_Job_Target_File_Name,   # 'DC_DISB_CHK_EXC_common2_'
        'txt',
        target_bucket,
        config.intermediate,
        target_date
    )


    ############################################################################
    # SECTION 9 — Stage 3: SOC Job
    #   Filter SOC records → merge against SOC schema dummy → sort → write final output
    ############################################################################

    # Load the SOC schema dummy DataFrames from Common_Script:
    #   df_merge  : All-empty SOC schema row — used to guarantee all SOC columns exist
    #               in the output even if the source data doesn't have them
    #   df_funnel : Column-name self-mapping row — defines the output column set
    # df_funnel.limit(0) → empty schema frame (no data rows, just schema) used for unionAll
    df_merge, df_funnel = Common_Script.SOC_dummy()
    df_funnel = df_funnel.limit(0)   # Strip the single mapping row — keep schema only

    logger.info('Starting SOC job transformations as per DataStage logic')

    # Filter for RECTYPEID = 'SOC' — only Statement of Cash records proceed to final output.
    # Any non-SOC records (e.g., header/control rows) are dropped here.
    df_S01 = df_common_job_final.filter(col("RECTYPEID") == "SOC")

    # LEFT JOIN filtered data with the SOC merge dummy frame on RECTYPEID.
    # Purpose: guarantees all SOC output columns are present in df_S02 even if
    # the source data is missing any optional SOC columns (e.g., XAMT1-10, XCCY1-10).
    # The merge dummy provides null-filled defaults for any missing columns.
    df_S02 = df_S01.alias("df_S01").join(
        df_merge.alias("df_merge"),
        on="RECTYPEID",
        how="left"
    )

    # Full SOC output column list — matches the TLM SOC schema exactly.
    # Column selection prioritizes df_S01 (source data) over df_merge (dummy defaults):
    #   if a column exists in df_S01 → use df_S01 value
    #   if only in df_merge          → use df_merge null/default value
    # This ensures populated source fields aren't overwritten by dummy nulls.
    columns_to_select = [
        "SIDE", "STMTNO", "STMTPG", "DESTINATIONID", "TERMINALID", "MTYPE", "OURREF", "THEIRREF",
        "SUBACC", "OPBALTP", "OPBALDATE", "OPBALCY", "OPBAL", "CLBALTP", "CLBALDATE", "CLBALCY",
        "CLBAL", "ENTRY_DATE", "VALUE_DATE", "REFERENCE_1", "REFERENCE_2", "REFERENCE_3",
        "REFERENCE_4", "REFERENCE_4_OVERFLOW", "AMOUNT", "CURRENCY", "REDENOM_AMOUNT",
        "REDENOM_CURRENCY", "SOURCE_CODE", "TRANSACTION_CODE", "ASSET_CODE", "ASSET_DESCRIPTION",
        "NARRATIVE", "ASSET_TYPE", "DEAL_QUANTITY", "PRICE_PER_UNIT", "PRICE_CURRENCY",
        "RELATED_REF", "COUNTERPARTY_REF", "SIGN", "SSRGIN", "ACCRUED_AMOUNT",
        "ACCRUED_AMOUNT_BASE", "ACCRUED_AMOUNT_BASE_CURRENCY", "ACCRUED_AMOUNT_CURRENCY",
        "AMOUNT_BASE", "AMOUNT_BASE_CURRENCY", "DEAL_PRICE", "DEAL_PRICE_BASE",
        "DEAL_PRICE_BASE_CURRENCY", "DEAL_PRICE_CURRENCY", "EXCHANGE", "INSTRUMENT_CLASS",
        "INSTRUMENT_SUB_CLASS", "PRICE_BASE", "PRICE_BASE_CURRENCY", "SOURCE_SYSTEM",
        "XAMT1",  "XAMT2",  "XAMT3",  "XAMT4",  "XAMT5",  "XAMT6",  "XAMT7",  "XAMT8",
        "XAMT9",  "XAMT10",
        "XCCY1",  "XCCY2",  "XCCY3",  "XCCY4",  "XCCY5",  "XCCY6",  "XCCY7",  "XCCY8",
        "XCCY9",  "XCCY10",
        "XDATE1", "XDATE2", "XDATE3", "XDATE4", "XDATE5", "XDATE6", "XDATE7", "XDATE8",
        "XDATE9", "XDATE10",
        "XFLAG1",  "XFLAG2",  "XFLAG3",  "XFLAG4",  "XFLAG5",  "XFLAG6",  "XFLAG7",
        "XFLAG8",  "XFLAG9",  "XFLAG10", "XFLAG11", "XFLAG12", "XFLAG13", "XFLAG14",
        "XFLAG15", "XFLAG16", "XFLAG17", "XFLAG18", "XFLAG19", "XFLAG20",
        "XSTR1",  "XSTR2",  "XSTR3",  "XSTR4",  "XSTR5",  "XSTR6",  "XSTR7",  "XSTR8",
        "XSTR9",  "XSTR10", "XSTR11", "XSTR12", "XSTR13", "XSTR14", "XSTR15", "XSTR16",
        "XSTR17", "XSTR18", "XSTR19",
        "BOOK", "COST_CURRENCY", "EXECUTING_BROKER"
    ]

    # Disambiguate columns: prefer df_S01 (source) over df_merge (dummy) for each column name
    df_S02 = df_S02.select([
        col(f"df_S01.{col_name}").alias(col_name) if col_name in df_S01.columns
        else col(f"df_merge.{col_name}").alias(col_name)
        for col_name in columns_to_select
    ])

    # Sort output by SUBACC + CLBALDATE — ensures deterministic record order in TLM
    # matching the DataStage sequential output sort stage
    df_S03 = df_S02.orderBy('SUBACC', 'CLBALDATE')

    # Union with the empty funnel frame (schema-only, zero rows).
    # This ensures the output DataFrame's schema exactly matches the SOC funnel schema
    # even if df_S03 is empty — prevents schema mismatch errors in downstream consumers.
    df_final = df_funnel.unionAll(df_S03)

    # Replace underscores in column names with spaces — TLM system expects space-delimited headers.
    # Replace empty strings with None (null) — TLM interprets empty fields as nulls.
    df_final = df_final.select(
        [col(c).alias(c.replace('_', ' ')) for c in df_final.columns]
    ).replace("", None)

    logger.info('SOC job transformations ended')

    # Write final SOC output to TLM_OUT/ — this is the file consumed by the TLM system
    logger.info('Storing SOC job output in TLM path of S3 bucket')
    Common_Script.dependent_output_store(
        df_final,
        df_final.count(),
        config.SO_Target_File_Name,   # 'SOC_DC_DISB_CHK_EXC_'
        'txt',
        target_bucket,
        config.target,                # 'TLM_OUT'
        target_date
    )

    logger.info(f'Execution of {config.Glue_Job_Name} job completed successfully')

except Exception as e:
    logging.error(f'Error while processing the data: {e}')
    raise


################################################################################
# SECTION 10 — Cleanup: Delete Intermediate Files (finally block)
#
# The finally block runs regardless of success or failure.
# Intermediate files are always cleaned up to prevent:
#   - Reprocessing stale files on the next run
#   - S3 storage accumulation from repeated job executions
#   - Downstream jobs accidentally picking up old intermediate files
#
# delete_s3_file() uses boto3.delete_object() with error logging (returns True/False).
# Even if deletion fails (e.g., file not found), the job is not re-failed —
# cleanup is best-effort to avoid masking the real job result.
################################################################################

finally:
    logger.info('Deleting temporary files from intermediate path')

    # Delete Stage 1 parallel output
    Common_Script.delete_s3_file(
        target_bucket,
        config.intermediate,
        f'{config.Parallel_Job_Target_File_Name}{target_date}.txt'
    )

    # Delete dummy control file
    Common_Script.delete_s3_file(
        target_bucket,
        config.intermediate,
        f'{config.Dummy_Target_File_Name}{target_date}.txt'
    )

    # Delete Stage 2 common job output
    Common_Script.delete_s3_file(
        target_bucket,
        config.intermediate,
        f'{config.Common_Job_Target_File_Name}{target_date}.txt'
    )

    logger.info('Successfully deleted temporary files from intermediate path')
    spark.stop()   # Cleanly release Spark resources — important in shared Glue worker pools