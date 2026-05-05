################################################################################
# BRNORC127.py
#
# Glue Job    : BRNORC127 (BRN Oracle Extract — PageNumber File Generator)
#
# Purpose:
#   Connects to the production Oracle TLM database (tlmadmin schema) via JDBC,
#   queries the message_header and message_feed tables, and generates a
#   pipe-delimited PageNumber reference file that is written to S3.
#
#   The PageNumber file is consumed by downstream BRN Glue jobs (e.g., BRNUD070)
#   as part of the 'common_files' dependency set — specifically the 'PageNumber'
#   token resolved in Lambda app.py's check_files_in_s3().
#
# Output File:
#   PageNumber_ITOC_<YYYYMMDD_HHMMSS>.txt
#   Written to: s3://<targetbucketname>/<intermediatefolder>/
#
# Oracle Source Tables:
#   tlmadmin.message_header  — Statement header records (sub_acc_no, stmt_date, stmt_no)
#   tlmadmin.message_feed    — Statement feed records  (sub_acc_no join key)
#
# Query Logic:
#   For account 'ITOC_942_ADVISORY', group by sub_acc_no + stmt_date and
#   take MAX(stmt_no) as the page number for that statement date.
#   Output columns: pFile, processDate (yyyymmdd), pageNo
#
# S3 Target Bucket   : institutional-eng-us-east-1-brn-etl-s3-source
# S3 Target Folder   : Intermediate/
# KMS Key (prod)     : arn:aws:kms:us-east-1:<account-id-prod>:key/<kms-key-id-prod>
#
# Glue Job Parameters (all required at runtime):
#   --JOB_NAME          : Glue job name (injected by Glue automatically)
#   --secretarn         : AWS Secrets Manager ARN for Oracle credentials {'u':..., 'p':...}
#   --intermediatefolder: S3 folder path prefix (e.g., 'Intermediate/')
#   --targetbucketname  : S3 bucket name to write the output file into
#
# Oracle JDBC Connection:
#   URL    : jdbc:oracle:thin:@ldap://<oracle-ldap-host>:3060/<oracle-service>,...
#   Driver : oracle.jdbc.OracleDriver
#   Note   : LDAP-based JDBC URL — Oracle connection is resolved via LDAP directory
#            (<oracle-ldap-host>), not a direct host:port/SID connection
################################################################################

import sys
import boto3
import json
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import date_format
from datetime import datetime
from pytz import timezone


################################################################################
# SECTION 1 — Logging Configuration
################################################################################

# basicConfig sets the root logger level — INFO captures job progress messages.
# Using __name__ as the logger name scopes log output to this module,
# making it easier to filter in CloudWatch Logs.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


################################################################################
# SECTION 2 — Main Job Block (try/except wraps the entire job body)
#
# Wrapping all job logic in a single try/except ensures:
#   - Any unhandled exception is logged with a meaningful error message
#   - The Glue job reports FAILED status rather than silently stopping
#   - job.commit() is only called on the success path
################################################################################

try:

    ##############################################################################
    # SECTION 2a — Resolve Glue Job Parameters
    ##############################################################################

    # getResolvedOptions reads --param_name arguments passed at Glue job runtime.
    # All four parameters are required — missing any will raise an exception immediately.
    args   = getResolvedOptions(
        sys.argv,
        ['JOB_NAME', 'secretarn', 'intermediatefolder', 'targetbucketname']
    )
    target = args['targetbucketname']    # e.g., 'institutional-eng-us-east-1-brn-etl-s3-source'
    folder = args['intermediatefolder']  # e.g., 'Intermediate/' (must include trailing slash)


    ##############################################################################
    # SECTION 2b — Initialize Spark / Glue Context
    ##############################################################################

    # Standard Glue job initialization pattern.
    # SparkContext → GlueContext → SparkSession are the three layers used for all
    # DataFrame and SQL operations. job.init() registers this run with the Glue catalog.
    sc          = SparkContext()
    glueContext = GlueContext(sc)
    spark       = glueContext.spark_session
    job         = Job(glueContext)
    job.init(args['JOB_NAME'], args)   # Registers the job run; enables job bookmarks if configured


    ##############################################################################
    # SECTION 2c — Retrieve Oracle Credentials from AWS Secrets Manager
    ##############################################################################

    # Oracle credentials are stored in Secrets Manager as a JSON string:
    #   { "u": "<username>", "p": "<password>" }
    # Never hardcoded — SecretId is passed at runtime via --secretarn parameter.
    client                    = boto3.client("secretsmanager")
    get_secret_value_response = client.get_secret_value(SecretId=args['secretarn'])
    secret                    = json.loads(get_secret_value_response['SecretString'])
    db_username               = secret['u']
    db_password               = secret['p']
    logger.info("DB user: %s", db_username)


    ##############################################################################
    # SECTION 2d — Oracle JDBC Connection Setup
    ##############################################################################

    # LDAP-based Oracle JDBC URL.
    # This connects via Oracle Internet Directory (OID) LDAP service on port 3060,
    # resolving the service name 'brpprd20_all' from the LDAP directory tree.
    # LDAP-based Oracle JDBC URL — avoids hardcoding host:port/SID and allows transparent DB failover.
    jdbc_url         = (
        "jdbc:oracle:thin:@ldap://<oracle-ldap-host>:3060/"
        "<oracle-service>,cn=OracleContext,dc=example,dc=com"
    )
    jdbc_driver_name = "oracle.jdbc.OracleDriver"   # Oracle Thin JDBC driver

    # Source tables in the tlmadmin schema:
    #   message_header : Statement-level metadata (one row per statement)
    #   message_feed   : Statement feed details (one row per feed entry)
    table_name      = 'tlmadmin.message_header'
    table_name_feed = 'tlmadmin.message_feed'


    ##############################################################################
    # SECTION 2e — Read Oracle Tables via JDBC
    ##############################################################################

    # Full-table reads via JDBC — both tables are loaded into Spark DataFrames.
    # Filtering and aggregation are done in Spark SQL (Section 2f) rather than
    # in the JDBC query, keeping the connection layer simple.
    # Note: For large tables, consider pushing predicates down via .option("query", ...)
    # instead of full-table reads to reduce driver memory pressure.

    df_message_header = (
        glueContext.read.format("jdbc")
            .option("driver",   jdbc_driver_name)
            .option("url",      jdbc_url)
            .option("dbtable",  table_name)       # Reads full tlmadmin.message_header table
            .option("user",     db_username)
            .option("password", db_password)
            .load()
    )

    df_message_feed = (
        glueContext.read.format("jdbc")
            .option("driver",   jdbc_driver_name)
            .option("url",      jdbc_url)
            .option("dbtable",  table_name_feed)  # Reads full tlmadmin.message_feed table
            .option("user",     db_username)
            .option("password", db_password)
            .load()
    )


    ##############################################################################
    # SECTION 2f — Register Temp Views and Execute SQL
    ##############################################################################

    # Register both DataFrames as Spark SQL temp views.
    # This allows using standard SQL syntax (JOIN, GROUP BY, MAX) rather than
    # chaining DataFrame API calls — easier to map 1:1 back to the original
    # DataStage SQL-based transformation logic.
    df_message_header.createOrReplaceTempView("message_header")
    df_message_feed.createOrReplaceTempView("message_feed")

    # PageNumber extraction query for the ITOC_942_ADVISORY sub-account.
    #
    # Business Logic:
    #   - Filter to the specific advisory account 'ITOC_942_ADVISORY'
    #   - Join message_header → message_feed on sub_acc_no to confirm feed existence
    #   - Group by account + statement date to get one row per date
    #   - MAX(stmt_no) = the highest (latest) page/statement number for that date
    #     This becomes the 'pageNo' used by downstream jobs to identify which
    #     statement page to process
    #
    # Output columns:
    #   pFile       : Sub-account identifier (always 'ITOC_942_ADVISORY' here)
    #   processDate : Statement date formatted as yyyymmdd (e.g., '20250115')
    #   pageNo      : Maximum statement number for that date (used as page reference)
    query_itoc = """
    SELECT
        A.sub_acc_no                             AS pFile,
        date_format(A.stmt_date, 'yyyymmdd')     AS processDate,
        MAX(A.stmt_no)                           AS pageNo
    FROM message_header A
    JOIN message_feed   B
      ON A.sub_acc_no = B.sub_acc_no
    WHERE A.sub_acc_no = 'ITOC_942_ADVISORY'
    GROUP BY A.sub_acc_no, A.stmt_date
    ORDER BY A.sub_acc_no, A.stmt_date
    """

    result_df_itoc = spark.sql(query_itoc)

    # Show a preview of the results in the Glue CloudWatch log for debugging
    result_df_itoc.show()
    logger.info("PageNumber_ITOC row count: %d", result_df_itoc.count())


    ##############################################################################
    # SECTION 2g — Write Output to Local Temp File (pipe-delimited)
    ##############################################################################

    # toPandas() collects the Spark DataFrame to the Glue driver node.
    # Acceptable here because the PageNumber file is small (one row per statement date —
    # typically a few hundred rows at most).
    # sep='|' : pipe-delimited — matches the format expected by downstream
    #           BRN jobs that read common reference files
    # index=False : suppress pandas row index column in the CSV output
    result_df_itoc.toPandas().to_csv('temp.csv', index=False, sep='|')


    ##############################################################################
    # SECTION 2h — Upload Output File to S3 with KMS Encryption
    ##############################################################################

    # Generate a timestamped filename to ensure uniqueness per run.
    # Timestamp is in Eastern Time (America/New_York) — consistent with other BRN jobs.
    # Format: PageNumber_ITOC_<YYYYMMDD_HHMMSS>.txt
    # Example: PageNumber_ITOC_20250115_143022.txt
    current_datetime = datetime.now(timezone("America/New_York")).strftime("%Y%m%d_%H%M%S")
    file_name_itoc   = f"PageNumber_ITOC_{current_datetime}.txt"

    # Upload the local temp.csv to S3 as a .txt file.
    # The file extension is .txt (not .csv) to match the BRN convention for
    # reference/common files consumed by Lambda's check_files_in_s3().
    #
    # ServerSideEncryption: aws:kms — mandatory for all BRN S3 writes
    # SSEKMSKeyId: prod KMS key ARN — configure per environment via --env + config lookup
    #
    # NOTE: This KMS key is hardcoded to prod — consider parameterizing via
    #       --env + config lookup (as done in BRNUD070.py) for multi-env support.
    s3_client = boto3.client('s3')
    s3_client.upload_file(
        'temp.csv',                     # Local file written by toPandas().to_csv()
        target,                         # S3 bucket name (from --targetbucketname)
        folder + file_name_itoc,        # S3 key: e.g., 'Intermediate/PageNumber_ITOC_20250115_143022.txt'
        ExtraArgs={
            "ServerSideEncryption": "aws:kms",
            # Production KMS key — encrypts the file at rest in S3
            "SSEKMSKeyId": "arn:aws:kms:us-east-1:<account-id-prod>:key/<kms-key-id-prod>"
        }
    )

    logger.info("Data fetched successfully for PageNumber_ITOC")
    logger.info("Output file written to s3://%s/%s%s", target, folder, file_name_itoc)

    # Commit the Glue job — marks the job run as SUCCEEDED in the Glue catalog.
    # Must only be called after all processing is complete and the file is in S3.
    job.commit()

except Exception as e:
    # Catch-all exception handler — logs the full error message to CloudWatch.
    # The job will report FAILED status automatically when an exception propagates
    # out of the try block without job.commit() being called.
    logger.error("An error occurred: %s", str(e))
    raise   # Re-raise so Glue marks the job as FAILED (not just errored in logs)
