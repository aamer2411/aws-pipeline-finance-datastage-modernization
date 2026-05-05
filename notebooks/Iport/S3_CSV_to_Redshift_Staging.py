################################################################################
# Import all the required packages and modules
################################################################################

import glue_utils
import sys
import psycopg2
import logging
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# ── Logging Setup ─────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()

# ── Glue Job Parameters ───────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['sourcebucket'])
sourcebucket_name = args['sourcebucket']
sourcebucket = args['sourcebucket']

args = getResolvedOptions(sys.argv, ['env'])
env = args['env']

# ── Connections ───────────────────────────────────────────────────────────────
rd_cursor, rd_conn = glue_utils.redshiftdb_connection(env)
ora_cursor, ora_conn = glue_utils.oracle_connection(env)

(
    redshift_host,
    redshift_db,
    redshift_secret_arn,
    redshift_port,
    redshift_iam,
    redshift_url,
    s3_source_path,
    redshift_driver,
    jdbc_url,
    jdbc_driver_name,
    oracle_secret_arn
) = glue_utils.extract_config_values(env)

credentials = glue_utils.get_secret(redshift_secret_arn)
redshift_user = credentials.get('u', '')
redshift_password = credentials.get('p', '')

# ── Schema Constants ──────────────────────────────────────────────────────────
rd_dre_schema   = "dre_schema"
rd_dre11_schema = "dre11_schema"
rd_stg_schema   = "dre11_schema"

properties = {
    "user":     redshift_user,
    "password": redshift_password,
    "driver":   redshift_driver
}

################################################################################
# SECTION 1 — Seven TRR_LOOKUP_0725_PERCENTILE_50 Staging Tables (via JDBC)
################################################################################

t22_table = [
    "TRR_LOOKUP_0725_PERCENTILE_50_AGE_PARAMS",
    "TRR_LOOKUP_0725_PERCENTILE_50_AGE_5YRS_PARAMS",
    "TRR_LOOKUP_0725_PERCENTILE_50_AGE_VPEX_PARAMS",
    "TRR_LOOKUP_0725_PERCENTILE_50_BAL_PARAMS",
    "TRR_LOOKUP_0725_PERCENTILE_50_EA_PARAMS",
    "TRR_LOOKUP_0725_PERCENTILE_50_JOB_PARAMS",
    "TRR_LOOKUP_0725_PERCENTILE_50_SUMRY_PARAMS"
]

latest_source_file = [
    "TRR_seq_0725_PERCENTILE_50_AGE_PARAMS.csv",
    "TRR_seq_0725_PERCENTILE_50_5YRS_PARAMS.csv",
    "TRR_seq_0725_PERCENTILE_50_AGE_VPEX_PARAMS.csv",
    "TRR_seq_0725_PERCENTILE_50_BAL_PARAMS.csv",
    "TRR_seq_0725_PERCENTILE_50_EA_PARAMS.csv",
    "TRR_seq_0725_PERCENTILE_50_JOB_PARAMS.csv",
    "TRR_seq_0725_PERCENTILE_50_SUMRY_PARAMS.csv"
]

for i in range(7):
    tbl_nm = t22_table[i]
    src_fl = latest_source_file[i]

    create_table_sql = f"""
    DROP TABLE IF EXISTS {rd_dre11_schema}.{tbl_nm};
    CREATE TABLE IF NOT EXISTS {rd_dre11_schema}.{tbl_nm} (
        pPARTITIONBY        VARCHAR(50),
        pSEGMENT            VARCHAR(50),
        pCLNT_ID            VARCHAR(50),
        pPLN_ID             VARCHAR(50),
        pPLAN_TYPE          VARCHAR(50),
        pAGE_GR_SEG         VARCHAR(50),
        pAGE_GR_SEG_5YRS    VARCHAR(50),
        pAGE_GR_SEG_VPEX    VARCHAR(50),
        pJOB_TEN_CD_SEG     VARCHAR(50),
        pBALANCE_CD_SEG     VARCHAR(50),
        pEQUITY_CD_SEG      VARCHAR(50),
        pCLT_SZ_CD_SEG      VARCHAR(50),
        pINDUSTRY_CD_SEG    VARCHAR(50),
        pRETURN_TYPE_COLUMN VARCHAR(50),
        pRETURN_TYPE        VARCHAR(50),
        pEMP_DIV_LOC        VARCHAR(50),
        MAIN_LOOP_COUNTER   VARCHAR(100),
        CHILD_LOOP_COUNTER  VARCHAR(50)
    );"""
    rd_cursor.execute(create_table_sql)
    rd_conn.commit()
    print(f"Table {rd_dre11_schema}.{tbl_nm} created successfully.")

    source_key = f"{sourcebucket_name}M13/{src_fl}"

    columns = [
        "pPARTITIONBY", "pSEGMENT", "pCLNT_ID", "pPLN_ID", "pPLAN_TYPE",
        "pAGE_GR_SEG", "pAGE_GR_SEG_5YRS", "pAGE_GR_SEG_VPEX",
        "pJOB_TEN_CD_SEG", "pBALANCE_CD_SEG", "pEQUITY_CD_SEG",
        "pCLT_SZ_CD_SEG", "pINDUSTRY_CD_SEG", "pRETURN_TYPE_COLUMN",
        "pRETURN_TYPE", "pEMP_DIV_LOC", "MAIN_LOOP_COUNTER", "CHILD_LOOP_COUNTER"
    ]

    lookup_schema = StructType([StructField(col, StringType(), True) for col in columns])

    df = spark.read.csv(source_key, sep="|", header=True, schema=lookup_schema)

    t22_redshift_table = f"{rd_dre11_schema}.{tbl_nm}"

    df.write \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", t22_redshift_table) \
        .option("user", redshift_user) \
        .option("password", redshift_password) \
        .option("driver", redshift_driver) \
        .mode("overwrite") \
        .save()

    print(f"{rd_dre11_schema}.{tbl_nm} table loaded successfully.")


################################################################################
# SECTION 2 — IndustrialCode_AssetName_Extract Staging Table (via COPY)
################################################################################

tar_table = "STG_M13_IndustrialCode_AssetName_Extract"
try:
    stg_table = f"STG_M13_RAW_{tar_table}"
    file_name = "IndustrialCode_AssetName_Extract.csv"

    # Step 1 — Create raw staging table (single wide VARCHAR column)
    CREATE_TABLE = f"""
    DROP TABLE IF EXISTS {rd_stg_schema}.{stg_table};
    CREATE TABLE IF NOT EXISTS {rd_stg_schema}.{stg_table} (
        raw_data       VARCHAR(65535),
        rec_flag       CHAR(1),
        createddate_ts TIMESTAMP,
        modified_ts    TIMESTAMP
    )"""
    rd_cursor.execute(CREATE_TABLE)

    # Step 2 — COPY raw CSV from S3 into raw staging table
    s3_source_file = f"{sourcebucket_name}M13/{file_name}"
    copy_sql = glue_utils.copy_to_redshift(
        rd_stg_schema, stg_table, s3_source_file, redshift_iam, rec_flag="I"
    )
    rd_cursor.execute(copy_sql)

    # Step 3 — Create parsed target staging table
    CREATE_TABLE = f"""
    DROP TABLE IF EXISTS {rd_stg_schema}.{tar_table};
    CREATE TABLE IF NOT EXISTS {rd_stg_schema}.{tar_table} (
        ASSET_NUM        VARCHAR(6),
        X_PLAN_NAME      VARCHAR(150),
        NAME             VARCHAR(150),
        NAICS_INDUS_CD   VARCHAR(10),
        NAICS_INDUS_DESC VARCHAR(150)
    )"""
    rd_cursor.execute(CREATE_TABLE)

    # Step 4 — Parse pipe-delimited raw_data into structured columns
    load_sql = f"""
    INSERT INTO {rd_stg_schema}.{tar_table}
    SELECT
        SPLIT_PART(raw_data, '|', 1) AS ASSET_NUM,
        SPLIT_PART(raw_data, '|', 2) AS X_PLAN_NAME,
        SPLIT_PART(raw_data, '|', 3) AS NAME,
        SPLIT_PART(raw_data, '|', 4) AS NAICS_INDUS_CD,
        SPLIT_PART(raw_data, '|', 5) AS NAICS_INDUS_DESC
    FROM {rd_stg_schema}.{stg_table};"""
    rd_cursor.execute(load_sql)
    print("IndustrialCode_AssetName_Extract table created successfully.")

except Exception as e:
    print(f"Error: {str(e)}")


################################################################################
# SECTION 3 — TCOMM_SEG / ILY_PSRA_TRNX_IN Staging Table (via COPY)
################################################################################

try:
    stg_table1 = "STG_M13_RAW_PSRA_TRNX_IN"
    file_name1 = "ILY_PSRA_TRNX_IN_AUG_RUN.csv"

    # Step 1 — Create raw staging table
    CREATE_TABLE = f"""
    DROP TABLE IF EXISTS {rd_stg_schema}.{stg_table1};
    CREATE TABLE IF NOT EXISTS {rd_stg_schema}.{stg_table1} (
        raw_data       VARCHAR(65535),
        rec_flag       CHAR(1),
        createddate_ts TIMESTAMP,
        modified_ts    TIMESTAMP
    )"""
    rd_cursor.execute(CREATE_TABLE)

    # Step 2 — COPY raw CSV from S3
    s3_source_file1 = f"{sourcebucket}M13/{file_name1}"
    copy_sql = glue_utils.copy_to_redshift(
        rd_stg_schema, stg_table1, s3_source_file1, redshift_iam, rec_flag="I"
    )
    rd_cursor.execute(copy_sql)

    source_count = glue_utils.get_source_count(rd_stg_schema, stg_table1, rd_cursor)

    # Step 3 — Create parsed target staging table
    CREATE_TABLE = f"""
    DROP TABLE IF EXISTS {rd_stg_schema}.STG_M13_PSRA_TRNX_IN;
    CREATE TABLE IF NOT EXISTS {rd_stg_schema}.STG_M13_PSRA_TRNX_IN (
        ACTUL_YY VARCHAR(4),
        PLN_ID   VARCHAR(6),
        CLNT_ID  VARCHAR(10),
        PRT_ID   VARCHAR(9),
        FND_ID   VARCHAR(6),
        SRC_ID   VARCHAR(3),
        M0       DECIMAL(15,2),
        M1       DECIMAL(15,2),
        M2       DECIMAL(15,2),
        M3       DECIMAL(15,2),
        M4       DECIMAL(15,2),
        M5       DECIMAL(15,2),
        M6       DECIMAL(15,2),
        M7       DECIMAL(15,2),
        M8       DECIMAL(15,2),
        M9       DECIMAL(15,2),
        M10      DECIMAL(15,2),
        M11      DECIMAL(15,2)
    )"""
    rd_cursor.execute(CREATE_TABLE)

    # Step 4 — Parse pipe-delimited raw_data, skip header row
    load_sql = f"""
    INSERT INTO {rd_stg_schema}.STG_M13_PSRA_TRNX_IN
    SELECT
        SPLIT_PART(raw_data, '|', 1)  AS ACTUL_YY,
        SPLIT_PART(raw_data, '|', 2)  AS PLN_ID,
        SPLIT_PART(raw_data, '|', 3)  AS CLNT_ID,
        SPLIT_PART(raw_data, '|', 4)  AS PRT_ID,
        SPLIT_PART(raw_data, '|', 5)  AS FND_ID,
        SPLIT_PART(raw_data, '|', 6)  AS SRC_ID,
        SPLIT_PART(raw_data, '|', 7)  AS M0,
        SPLIT_PART(raw_data, '|', 8)  AS M1,
        SPLIT_PART(raw_data, '|', 9)  AS M2,
        SPLIT_PART(raw_data, '|', 10) AS M3,
        SPLIT_PART(raw_data, '|', 11) AS M4,
        SPLIT_PART(raw_data, '|', 12) AS M5,
        SPLIT_PART(raw_data, '|', 13) AS M6,
        SPLIT_PART(raw_data, '|', 14) AS M7,
        SPLIT_PART(raw_data, '|', 15) AS M8,
        SPLIT_PART(raw_data, '|', 16) AS M9,
        SPLIT_PART(raw_data, '|', 17) AS M10,
        SPLIT_PART(raw_data, '|', 18) AS M11
    FROM {rd_stg_schema}.{stg_table1}
    WHERE raw_data NOT LIKE 'ACTUL_YY%';"""
    rd_cursor.execute(load_sql)
    print("dre11_schema.STG_M13_PSRA_TRNX_IN staging table created successfully.")

except Exception as e:
    print(f"Error: {str(e)}")