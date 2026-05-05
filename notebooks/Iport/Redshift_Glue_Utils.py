################################################################################
# Utility module for AWS Glue ETL operations (IPORT Pipeline — Vanguard IIG)
#
# Covers:
#   - Spark / GlueContext initialization
#   - AWS Secrets Manager credential retrieval
#   - S3 config JSON reading + environment-specific config extraction
#   - Redshift (psycopg2) connection and data operations
#   - Oracle (Spark JVM JDBC) connection and data operations
#   - DB2 (Spark JVM JDBC) connection
#   - S3 file utilities: row count, existence check, upload, archive, latest file
#   - Data movement: S3 → Redshift (COPY), Redshift → Oracle (Spark JDBC)
#   - Metadata: cycle control table upsert, audit status insert
#   - Validation: source/target count, table data comparison, CDC diff
#   - SFTP configuration extraction
#   - Staging table creation and purge
#
# NOTE: Logic has NOT been changed. Only indentation, formatting,
#       and clarifying comments were added.
################################################################################

import sys
import json
import boto3
import traceback
import pytz
import psycopg2
import os
import subprocess
from datetime import datetime as dt, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

################################################################################
# SECTION 1 — Spark / Glue Session Initialization
# These objects are module-level singletons shared by all functions below.
################################################################################

sc           = SparkContext()
glueContext  = GlueContext(sc)
spark        = glueContext.spark_session  # Used by oracle_connection, db2_connection, copy_redshift_to_oracle


################################################################################
# SECTION 2 — Secrets & Configuration Management
################################################################################

def get_secret(secret_arn):
    """
    Retrieves a secret from AWS Secrets Manager using the AWS CLI subprocess.

    Why CLI instead of boto3 directly:
        The Glue job's IAM role may route Secrets Manager calls through a specific
        endpoint accessible only via CLI in some VPC configurations.
        Note: boto3 boto3.client('secretsmanager').get_secret_value() is the preferred
        approach in standard setups — this is the original implementation preserved as-is.

    Args:
        secret_arn (str): Full ARN of the secret to retrieve.

    Returns:
        dict: Parsed secret key-value pairs (e.g., {'u': 'username', 'p': 'password'})
              Returns {} on CLI error, "" on exception.
    """
    try:
        cmd = f"aws secretsmanager get-secret-value --secret-id {secret_arn}"
        out, err = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            shell=True
        ).communicate()

        if len(err.decode('utf-8')) > 0:
            print('Error retrieving secret: ' + err.decode('utf-8'))
            return {}
        else:
            # Double eval: outer parses CLI JSON response, inner parses SecretString value
            secret = eval(eval(out.decode('utf-8'))['SecretString'])
            return secret
    except Exception as e:
        print(f"Error: {str(e)}")
        return ""


def load_env_config(env):
    """
    Placeholder hook for future environment name transformation logic.
    Currently returns env unchanged — allows central override if naming conventions change.
    """
    return env


def read_json_from_s3(env):
    """
    Reads the environment configuration JSON file from the project's S3 config bucket.

    Config file path pattern:
        s3://application-payload-vgi-institutional-<env>-us-east-1/
            ILY/IPORTGluePythonScript/config/config.json

    Returns:
        dict: Full parsed config JSON containing all environment sections,
              or {} on failure.
    """
    try:
        s3 = boto3.client('s3')
        env_name = load_env_config(env)
        bucket_name = "application-payload-vgi-institutional-" + env_name + "-us-east-1"
        file_key    = "ILY/IPORTGluePythonScript/config/config.json"

        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content  = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        print(f"Error reading config from S3: {str(e)}")
        return {}


def get_config(env):
    """
    Returns the environment-specific config section from the full config JSON.

    Maps env names to config keys:
        'eng'  → *_eng  sections
        'test' → *_sat  sections  (SAT = System Acceptance Testing)
        'prod' → *_prod sections

    Returns:
        dict with keys: 'Redshift', 'S3', 'Oracle', 'SFTP' (or 'sftp' per section)
        Returns {} on failure or invalid env.
    """
    try:
        env_name = load_env_config(env)
        config   = read_json_from_s3(env_name)

        if env_name.lower() == 'eng':
            return {
                'Redshift': config['Redshift_eng'],
                'S3':       config['S3_eng'],
                'Oracle':   config['Oracle_eng'],
                'SFTP':     config['SFTP_eng']
            }
        elif env_name.lower() == 'test':
            return {
                'Redshift': config['Redshift_sat'],
                'S3':       config['S3_sat'],
                'Oracle':   config['Oracle_sat'],
                'sftp':     config['SFTP_sat']
            }
        elif env_name.lower() == 'prod':
            return {
                'Redshift': config['Redshift_prod'],
                'S3':       config['S3_prod'],
                'Oracle':   config['Oracle_prod'],
                'sftp':     config['SFTP_prod']
            }
        else:
            raise ValueError(f"Invalid environment: {env}")
    except Exception as e:
        print(f"Error reading config: {str(e)}")
        return {}


def extract_config_values(env):
    """
    Extracts and returns all connection/path values from the environment config
    as a flat tuple — used by connection functions and job scripts.

    Returns:
        tuple: (
            redshift_host, redshift_db, redshift_secret_arn,
            redshift_port, redshift_iam, redshift_url,
            s3_source_path, redshift_driver,
            jdbc_url, jdbc_driver_name, oracle_secret_arn
        )
        Returns tuple of 11 Nones on failure.
    """
    try:
        config = get_config(env)
        redshift_config = config['Redshift']
        s3_config       = config['S3']
        oracle_config   = config['Oracle']

        # Redshift connection + IAM role for COPY command
        redshift_host       = redshift_config['redshift_host']
        redshift_db         = redshift_config['redshift_db']
        redshift_secret_arn = redshift_config['redshift_secret_arn']
        redshift_port       = redshift_config['redshift_port']
        redshift_iam        = redshift_config['redshift_iam']   # IAM role ARN for COPY FROM S3
        redshift_url        = redshift_config['redshift_url']   # JDBC URL for Spark reads
        redshift_driver     = redshift_config['redshift_driver']

        # S3 source path (base path for input files)
        s3_source_path = s3_config['s3_source_path']

        # Oracle JDBC connection details
        jdbc_url         = oracle_config['jdbc_url']
        jdbc_driver_name = oracle_config['jdbc_driver_name']
        oracle_secret_arn = oracle_config['oracle_secret_arn']

        return (
            redshift_host, redshift_db, redshift_secret_arn,
            redshift_port, redshift_iam, redshift_url, s3_source_path,
            redshift_driver, jdbc_url, jdbc_driver_name, oracle_secret_arn
        )
    except Exception as e:
        print(f"Error extracting config values: {str(e)}")
        return tuple([None] * 11)


################################################################################
# SECTION 3 — Redshift Operations
################################################################################

def redshiftdb_connection(env):
    """
    Opens a psycopg2 connection to Redshift Serverless.

    Key settings:
        autocommit = True — each cursor.execute() is immediately committed.
        This avoids open transactions locking tables during long-running ETL jobs.
        (See project doc: glue_utils sets autocommit=True in redshiftdb_connection)

    Returns:
        (cursor, conn) tuple, or (None, None) on failure.
    """
    try:
        redshift_vals       = extract_config_values(env)
        redshift_host       = redshift_vals[0]
        redshift_db         = redshift_vals[1]
        redshift_secret_arn = redshift_vals[2]
        redshift_port       = redshift_vals[3]

        credentials       = get_secret(redshift_secret_arn)
        redshift_username = credentials.get('u', '')
        password          = credentials.get('p', '')

        conn = psycopg2.connect(
            database=redshift_db,
            host=redshift_host,
            port=redshift_port,
            user=redshift_username,
            password=password
        )
        conn.autocommit = True  # Prevents uncommitted transactions from locking tables
        cursor = conn.cursor()
        return cursor, conn
    except Exception as e:
        print(f"Error connecting to Redshift: {str(e)}")
        return None, None


def copy_to_redshift(schema_name, stg_table, s3_source_file, redshift_iam, rec_flag):
    """
    Generates a SQL block to COPY a raw file from S3 into a Redshift staging table,
    then immediately UPDATE the metadata columns (rec_flag, timestamps).

    Design pattern used across IPORT ingestion (see S3-CSV-to-Redshift script):
        - Load entire raw file as single-column raw_data VARCHAR(65535)
        - COPY uses newline as delimiter (each line = one raw_data row)
        - Post-COPY UPDATE stamps rec_flag and timestamps for all newly loaded rows
          (WHERE rec_flag IS NULL identifies rows inserted in this COPY run)

    COPY options:
        ACCEPTINVCHARS — replaces invalid UTF-8 characters (handles mainframe encoding)
        REMOVEQUOTES   — strips surrounding quotes from fields
        EMPTYASNULL    — treats empty strings as NULL

    Args:
        schema_name   : Redshift schema (e.g., 'dre11_schema')
        stg_table     : Staging table name (e.g., 'STG_M13_RAW_PSRA_TRNX_IN')
        s3_source_file: Full S3 URI of source file (e.g., 's3://bucket/path/file.csv')
        redshift_iam  : IAM role ARN authorized to read from the S3 bucket
        rec_flag      : Record flag value to stamp (e.g., 'I' for Insert)

    Returns:
        str: Complete SQL string ready for cursor.execute(), or None on error.
    """
    try:
        copy_sql = f"""
COPY {schema_name}.{stg_table} (raw_data)
FROM '{s3_source_file}'
IAM_ROLE '{redshift_iam}'
ACCEPTINVCHARS
DELIMITER '\\n'
REMOVEQUOTES
EMPTYASNULL;
UPDATE {schema_name}.{stg_table}
SET rec_flag = '{rec_flag}',
    createdDate_ts = GETDATE(),
    modified_ts    = GETDATE()
WHERE rec_flag IS NULL;
        """
        return copy_sql
    except Exception as e:
        print(f"Error building COPY SQL: {str(e)}")
        return None


def get_source_count(schema_name, stg_table, cursor, where=None):
    """
    Returns the row count from a staging table.

    Optional filter: if where=True, counts only rows where first character
    of raw_data is 'E' (used for fixed-width mainframe file record type filtering).

    Args:
        schema_name: Redshift schema name
        stg_table  : Staging table name
        cursor     : Active Redshift cursor
        where      : If truthy, applies SUBSTRING(raw_data, 1, 1) = 'E' filter

    Returns:
        int: Row count
    """
    try:
        where_clause = "WHERE SUBSTRING(raw_data, 1, 1) = 'E'" if where else ""
        source_count_sql = f"""
            SELECT COUNT(*) AS source_count
            FROM {schema_name}.{stg_table}
            {where_clause}
        """
        cursor.execute(source_count_sql)
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error getting source count: {str(e)}")
        raise e


def get_target_count(schema_name, target_table, cursor):
    """
    Returns the total row count from a target (non-staging) table.
    Used for post-load validation — compare source_count vs target_count.

    Returns:
        int: Row count
    """
    try:
        target_count_sql = f"""
            SELECT COUNT(*) AS target_count
            FROM {schema_name}.{target_table}
        """
        cursor.execute(target_count_sql)
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error getting target count: {str(e)}")
        raise e


def truncate_staging_table(schema_name, stg_table, cursor):
    """
    Truncates a staging table — removes all rows without dropping the table structure.
    Used before reload operations to ensure idempotent (re-runnable) ETL jobs.
    """
    try:
        cursor.execute(f"TRUNCATE TABLE {schema_name}.{stg_table}")
    except Exception as e:
        print(f"Error truncating staging table: {str(e)}")
        raise e


def check_and_delete_existing_records(rd_stg_schema, rd_tgt_schema, target_table, stg_table,
                                       dimension_key, dimension_value, cursor, where=True):
    """
    Deletes existing target rows whose dimension key matches the MIN staging value.
    Used before re-inserting to prevent duplicate records on job reruns (append-load pattern).

    Pattern:
        1. Find MIN(dimension_value) from staging (optionally filtered for record type 'E')
        2. Check if target has rows for that dimension key
        3. If yes, delete them — making room for fresh insert

    Returns:
        bool: True if operation succeeded (with or without deletion), False on error.
    """
    try:
        where_clause = "WHERE SUBSTRING(raw_data, 1, 1) = 'E'" if where else ""

        # Check if matching rows exist in target
        check_existing_sql = f"""
            SELECT COUNT(*)
            FROM {rd_tgt_schema}.{target_table}
            WHERE {dimension_key} = (
                SELECT MIN(CAST({dimension_value} AS INT))
                FROM {rd_stg_schema}.{stg_table}
                {where_clause}
            )
        """
        cursor.execute(check_existing_sql)
        existing_count = cursor.fetchone()[0]

        if existing_count > 0:
            delete_existing_sql = f"""
                DELETE FROM {rd_tgt_schema}.{target_table}
                WHERE {dimension_key} = (
                    SELECT MIN(CAST({dimension_value} AS INT))
                    FROM {rd_stg_schema}.{stg_table}
                    {where_clause}
                )
            """
            cursor.execute(delete_existing_sql)
        return True
    except Exception as e:
        print(f"Error checking/deleting existing records: {str(e)}")
        traceback.print_exc()
        return False


def redshift_purge_partition(partition_count, rd_tgt_schema, target_table, rd_cursor):
    """
    Purges older partitions from a time-partitioned Redshift table.

    Logic: deletes all rows where dt_dimension_key <= MAX(dt_dimension_key) - partition_count
    Used for rolling-window retention (e.g., keep last N periods, delete older data).

    Args:
        partition_count: Number of recent partitions to RETAIN (older ones get deleted)
        rd_tgt_schema  : Target schema name
        target_table   : Target table name
        rd_cursor      : Active Redshift cursor

    Returns:
        str: Summary message with deleted row count, or raises on failure.
    """
    try:
        rd_cursor.execute(f"SELECT MAX(dt_dimension_key) FROM {rd_tgt_schema}.{target_table}")
        max_dt = rd_cursor.fetchone()[0]

        if max_dt is None:
            raise ValueError("No data found in the table — cannot determine purge threshold.")

        purge_query = f"""
            DELETE FROM {rd_tgt_schema}.{target_table}
            WHERE dt_dimension_key <= ({max_dt} - {partition_count})
        """
        rd_cursor.execute(purge_query)
        rows_deleted = rd_cursor.rowcount
        return f"Successfully purged {rows_deleted} rows."
    except Exception as e:
        raise Exception(f"Failed to purge partitions: {str(e)}")


################################################################################
# SECTION 4 — Oracle Operations
################################################################################

def oracle_connection(env):
    """
    Establishes a JDBC connection to Oracle using Spark's JVM DriverManager bridge.

    Why Spark JVM instead of cx_Oracle:
        AWS Glue doesn't natively include cx_Oracle. Using spark._jvm.DriverManager
        leverages the Oracle JDBC driver loaded into the Spark classpath (configured
        in the Glue job's --extra-jars parameter).

    Returns:
        (oracle_cursor, oracle_conn) where cursor is a JDBC Statement object,
        or (None, None) on failure.
    """
    try:
        oracle_vals      = extract_config_values(env)
        jdbc_url         = oracle_vals[8]
        jdbc_driver_name = oracle_vals[9]
        oracle_secret_arn = oracle_vals[10]

        credentials     = get_secret(oracle_secret_arn)
        oracle_user     = credentials.get('u', '')
        oracle_password = credentials.get('p', '')

        if not oracle_password:
            raise Exception("Failed to retrieve Oracle password from Secrets Manager.")

        # Connect via Spark JVM JDBC bridge (no cx_Oracle dependency needed)
        oracle_conn   = spark._jvm.java.sql.DriverManager.getConnection(
            jdbc_url, oracle_user, oracle_password
        )
        oracle_cursor = oracle_conn.createStatement()  # JDBC Statement (not Python cursor)
        return oracle_cursor, oracle_conn
    except Exception as e:
        print(f"Error connecting to Oracle: {str(e)}")
        traceback.print_exc()
        return None, None


def copy_redshift_to_oracle(ora_schema, target_table, oracle_data_sql, env):
    """
    Executes a SELECT query on Redshift and appends the result set into an Oracle table.

    Uses Spark as the data bridge:
        Redshift → spark.read.jdbc() → DataFrame → df.write.jdbc() → Oracle

    Why Spark bridge:
        Handles type mapping, large row sets, and avoids manual cursor-level row iteration.
        The 'append' write mode ensures existing Oracle rows are not overwritten.

    Args:
        ora_schema      : Oracle target schema (e.g., 'iportadm')
        target_table    : Table name in Oracle (same name as in Redshift)
        oracle_data_sql : SELECT query to run on Redshift (returns rows to insert into Oracle)
        env             : Environment identifier

    Raises:
        Exception on failure (propagated to caller for Step Functions retry handling).
    """
    try:
        (
            redshift_host, redshift_db, redshift_secret_arn, redshift_port,
            redshift_iam, redshift_url, s3_source_path, redshift_driver,
            jdbc_url, jdbc_driver_name, oracle_secret_arn
        ) = extract_config_values(env)

        # Fetch Redshift credentials
        rs_creds          = get_secret(redshift_secret_arn)
        redshift_username = rs_creds.get('u', '')
        redshift_password = rs_creds.get('p', '')

        # Fetch Oracle credentials
        ora_creds       = get_secret(oracle_secret_arn)
        oracle_user     = ora_creds.get('u', '')
        oracle_password = ora_creds.get('p', '')

        # Step 1: Read query result from Redshift into a Spark DataFrame
        redshift_df = spark.read \
            .format("jdbc") \
            .option("url", redshift_url) \
            .option("query", oracle_data_sql) \
            .option("user", redshift_username) \
            .option("password", redshift_password) \
            .option("driver", redshift_driver) \
            .load()

        # Step 2: Append Spark DataFrame into Oracle target table
        redshift_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{ora_schema}.{target_table}") \
            .option("user", oracle_user) \
            .option("password", oracle_password) \
            .option("driver", jdbc_driver_name) \
            .mode("append") \
            .save()

    except Exception as e:
        print(f"Error copying Redshift → Oracle: {str(e)}")
        traceback.print_exc()
        raise e  # Re-raise so Step Functions can catch job failure and retry


def execute_plsql_procedure(env, schema_name, procedure_name, params=None):
    """
    Invokes an Oracle PL/SQL stored procedure via JDBC callable statement.

    Used for:
        - PARTITION_MGT.COMPUTE_STATISTICS — refreshes Oracle optimizer stats after DML
        - execute_statements — DDL execution proxy for constraint management

    Args:
        env            : Environment identifier
        schema_name    : Oracle schema owning the procedure
        procedure_name : Procedure name (e.g., 'PARTITION_MGT.COMPUTE_STATISTICS')
        params         : Optional list of string parameters to bind positionally

    Returns:
        True on success, exits with code 1 on failure (signals Glue job failure).
    """
    try:
        oracle_cursor, oracle_conn = oracle_connection(env)
        if not oracle_cursor or not oracle_conn:
            raise Exception("Failed to establish Oracle connection.")

        if params:
            # Build parameterized call: {call schema.proc(?, ?, ...)}
            param_placeholders = ','.join(['?' for _ in params])
            call_statement = f"{{call {schema_name}.{procedure_name}({param_placeholders})}}"
        else:
            call_statement = f"{{call {schema_name}.{procedure_name}}}"

        callable_stmt = oracle_conn.prepareCall(call_statement)

        if params:
            for i, param in enumerate(params, start=1):
                callable_stmt.setString(i, str(param))  # Bind each param positionally

        callable_stmt.execute()
        callable_stmt.close()
        oracle_cursor.close()
        oracle_conn.close()

        print(f"Successfully executed procedure: {schema_name}.{procedure_name}")
        return True
    except Exception as e:
        print(f"Error executing PL/SQL procedure: {str(e)}")
        traceback.print_exc()
        exit(1)
        return False


def enable_disabled_constraints(schema_name, table_name, oracle_cursor):
    """
    Finds and enables all DISABLED constraints on a given Oracle table.

    Used after bulk data loads that disable constraints for performance,
    then re-enable them to restore data integrity enforcement.

    Delegates actual ALTER TABLE execution to the 'execute_statements' stored proc
    rather than direct DDL execution (per project's Oracle access control pattern).

    Returns:
        True on success, False on error.
    """
    try:
        # Query Oracle data dictionary for disabled constraints on this table
        query = f"""
            SELECT constraint_name
            FROM all_constraints
            WHERE status     = 'DISABLED'
              AND owner      = '{schema_name.upper()}'
              AND table_name = '{table_name.upper()}'
        """
        result_set = oracle_cursor.executeQuery(query)

        while result_set.next():
            constraint_name = result_set.getString(1)
            enable_sql = f"ALTER TABLE {schema_name}.{table_name} ENABLE CONSTRAINT {constraint_name}"
            # Execute DDL via stored procedure proxy (direct DDL may be restricted by Oracle permissions)
            execute_plsql_procedure(env, schema_name, 'execute_statements', [enable_sql])
            print(f"Enabled constraint: {constraint_name}")
        return True
    except Exception as e:
        print(f"Error enabling constraints: {str(e)}")
        traceback.print_exc()
        return False


def delete_oracle_records(rd_schema, ora_schema, stg_table, target_table, dimension_key,
                           dimension_value, env, where=True):
    """
    Deletes Oracle rows whose dimension_key matches the minimum dimension value
    found in the Redshift staging table.

    Pattern: used before re-inserting refreshed data to prevent duplicates
    when Oracle doesn't support MERGE/UPSERT efficiently for this use case.

    Args:
        rd_schema       : Redshift staging schema
        ora_schema      : Oracle target schema
        stg_table       : Redshift staging table name
        target_table    : Oracle target table name
        dimension_key   : Column name to match on in Oracle
        dimension_value : Column name in staging to derive the MIN value from
        env             : Environment identifier
        where           : If True, filters staging rows to first-char = 'E' records only

    Returns:
        True if deletion executed or no records found, False on error.
    """
    try:
        cursor, redshift_conn = redshiftdb_connection(env)
        if cursor is None:
            raise Exception("Failed to connect to Redshift.")

        # Optional filter for fixed-width mainframe record type 'E'
        and_opt = "AND SUBSTRING(raw_data, 1, 1) = 'E'" if where else ""

        # Find the minimum dimension value from staging (this is the "key" to delete from Oracle)
        redshift_query = f"""
            SELECT MIN({dimension_value})
            FROM {rd_schema}.{stg_table}
            WHERE rec_flag = 'I' {and_opt}
        """
        cursor.execute(redshift_query)
        dim_value_result = cursor.fetchone()[0]
        print(f"Dimension value to delete: {dim_value_result}")

        if dim_value_result is None:
            print("No matching records found in Redshift staging table.")
            return False

        cursor.close()
        redshift_conn.close()

        oracle_cursor, oracle_conn = oracle_connection(env)

        # Check if Oracle has records for this dimension key before attempting delete
        count_sql = f"""
            SELECT COUNT(*) AS cnt
            FROM {ora_schema}.{target_table}
            WHERE {dimension_key} = {dim_value_result}
        """
        count_result = oracle_cursor.executeQuery(count_sql)
        count_result.next()
        record_count = count_result.getInt(1)

        if record_count > 0:
            delete_sql = f"""
                DELETE FROM {ora_schema}.{target_table}
                WHERE {dimension_key} = {dim_value_result}
            """
            oracle_conn.createStatement().execute(delete_sql)

        oracle_conn.close()
        return True
    except Exception as e:
        print(f"Error deleting Oracle records: {str(e)}")
        traceback.print_exc()
        return False


################################################################################
# SECTION 5 — DB2 Operations
################################################################################

def db2_connection(bucket_name):
    """
    Opens a JDBC connection to DB2 (Non-Prod) using Spark's JVM DriverManager.

    DB2 is a source system in the IPORT pipeline (e.g., TPLN_PRT_SUM_RTN table).
    Connection config is read from the config JSON (Db2_Non_Prod section).

    Note: db2_password is assigned directly from db2_secret_arn per original logic —
    this may be intentional for non-prod environments where the secret value IS the password.

    Args:
        bucket_name: Used as the env key to get_config (acts as env identifier here)

    Returns:
        (db2_cursor, db2_conn) or (None, None) on failure.
    """
    try:
        config         = get_config(bucket_name)
        db2_config     = config['Db2_Non_Prod']
        db2_username   = db2_config['db2_username']
        db2_secret_arn = db2_config['db2_secret_arn']
        db2_driver     = db2_config['db2_driver']
        db2_jdbc_url   = db2_config['db2_jdbc_url']

        # Per original logic: db2_secret_arn value is used directly as the password
        db2_password = db2_secret_arn
        if not db2_password:
            raise Exception("Failed to retrieve DB2 password.")

        db2_conn   = spark._jvm.java.sql.DriverManager.getConnection(
            db2_jdbc_url, db2_username, db2_password
        )
        db2_cursor = db2_conn.createStatement()
        print("Successfully connected to DB2 database.")
        return db2_cursor, db2_conn
    except Exception as e:
        print(f"Error connecting to DB2: {str(e)}")
        traceback.print_exc()
        return None, None


################################################################################
# SECTION 6 — S3 File Utilities
################################################################################

def get_dat_file_row_count(env, file_name):
    """
    Counts non-empty lines in a fixed-width DAT file stored in the SFTP-ingested S3 bucket.

    Used for source row count validation before/after data loads.
    Filters out blank lines (common in mainframe-generated fixed-width files).

    Returns:
        int: Non-empty line count, or -1 on error.
    """
    try:
        if env == 'eng':
            bucket_name = "vgi-instfsrks-eng-us-east-1-sftp-internal-bucket-ily-eng"
            file_path   = "home/aidmq-sftp-sat/" + file_name
        elif env == 'test':
            bucket_name = "vgi-instfsrks-test-us-east-1-sftp-internal-bucket-ily-test"
            file_path   = "home/aidmq-sftp-sat/" + file_name
        elif env == 'prod':
            bucket_name = "vgi-instfsrks-prod-us-east-1-sftp-internal-bucket-ily-prd"
            file_path   = "home/aidmq-sftp-sat/" + file_name
        else:
            raise ValueError("Invalid environment.")

        s3_client = boto3.client('s3')
        response  = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        content   = response['Body'].read().decode('utf-8')

        # Count only non-blank lines (strips whitespace before checking)
        row_count = len([line for line in content.split('\n') if line.strip()])
        return row_count
    except Exception as e:
        print(f"Error reading DAT file: {str(e)}")
        traceback.print_exc()
        return -1


def get_latest_file_from_s3(env, file_name):
    """
    Finds the most recently modified S3 object matching the given file path prefix.

    Uses paginator to handle buckets with >1,000 objects (avoids the S3 1,000-object
    list_objects_v2 truncation issue — see project challenges doc section 7.3).

    Returns:
        str: S3 key of the latest file, or None on error.
    """
    try:
        s3_client = boto3.client('s3')
        if env == 'eng':
            bucket_name = "vgi-instfsrks-eng-us-east-1-sftp-internal-bucket-ily-eng"
            file_path   = "home/aidmq-sftp-sat/" + file_name
        elif env == 'test':
            bucket_name = "vgi-instfsrks-test-us-east-1-sftp-internal-bucket-ily-test"
            file_path   = "home/aidmq-sftp-prd/" + file_name
        elif env == 'prod':
            bucket_name = "vgi-instfsrks-prod-us-east-1-sftp-internal-bucket-ily-prd"
            file_path   = "home/aidmq-sftp-prd/" + file_name
        else:
            raise ValueError("Invalid environment.")

        paginator   = s3_client.get_paginator('list_objects_v2')
        latest_file = None
        latest_time = dt.min.replace(tzinfo=dt.now().astimezone().tzinfo)

        # Paginate through all objects (handles >1,000 objects correctly)
        for page in paginator.paginate(Bucket=bucket_name, Prefix=file_path):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['LastModified'] > latest_time:
                        latest_time = obj['LastModified']
                        latest_file = obj['Key']
        return latest_file
    except Exception as e:
        print(f"Error getting latest file from S3: {str(e)}")
        return None


def extract_date_from_filename(env, file_name):
    """
    Extracts the date token (second dot-delimited segment) from the latest S3 filename.

    Assumes filename pattern: <prefix>.<YYYYMMDD>.<ext>
    Example: IPORT_DATA.20250115.dat → returns '20250115'

    Returns:
        str: Date portion of filename, or None on error.
    """
    try:
        latest_file = get_latest_file_from_s3(env, file_name)
        base_name   = latest_file.split('/')[-1]   # Strip path prefix
        return base_name.split('.')[1]              # Return second segment (date token)
    except (IndexError, TypeError) as e:
        print(f"Error extracting date from filename: {str(e)}")
        return None


def check_file_exists(file_name, env):
    """
    Checks if a file exists in the ETL source bucket's Incoming/ path.
    Used by Step Functions dependency-check Lambda (via Glue job or direct call).

    Returns:
        bool: True if file exists, False if 404 or error.
    """
    if env == 'eng':
        bucket_name = "vgi-institutional-eng-us-east-1-ily-etl-s3-source"
        file_path   = "Incoming/" + file_name
    elif env == 'test':
        bucket_name = "vgi-institutional-test-us-east-1-ily-etl-s3-source"
        file_path   = "Incoming/" + file_name
    elif env == 'prod':
        bucket_name = "vgi-institutional-prod-us-east-1-ily-etl-s3-source"
        file_path   = "Incoming/" + file_name
    else:
        print("Invalid environment.")
        return False

    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=file_path)
        return True
    except Exception as e:
        if hasattr(e, 'response') and e.response.get('Error', {}).get('Code') == '404':
            print(f"File '{file_name}' not found in bucket '{bucket_name}'.")
        else:
            print(f"Error checking file existence: {e}")
        return False


def check_build1_file_exists(file_name, env):
    """
    Variant of check_file_exists for SFTP-ingested internal buckets (Build 1 jobs).
    Checks a different bucket set than check_file_exists (SFTP internal vs ETL source).

    Returns:
        bool: True if file exists, False otherwise.
    """
    if env == 'eng':
        bucket_name = "vgi-instfsrks-eng-us-east-1-sftp-internal-bucket-ily-eng"
        file_path   = "home/aidmq-sftp-sat/" + file_name
    elif env == 'test':
        bucket_name = "vgi-instfsrks-test-us-east-1-sftp-internal-bucket-ily-test"
        file_path   = "home/aidmq-sftp-sat/" + file_name
    elif env == 'prod':
        bucket_name = "vgi-instfsrks-prod-us-east-1-sftp-internal-bucket-ily-prd"
        file_path   = "home/aidmq-sftp-prd/" + file_name
    else:
        print("Invalid environment.")
        return False

    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=file_path)
        return True
    except Exception as e:
        if hasattr(e, 'response') and e.response.get('Error', {}).get('Code') == '404':
            print(f"File '{file_name}' not found in bucket '{bucket_name}'.")
        else:
            print(f"Error checking file existence: {e}")
        return False


def upload_file_to_s3(env, sourcebucket):
    """
    Recursively uploads all files from /tmp/glue_transfer to the specified S3 path.
    Uses AWS CLI with KMS server-side encryption (required per Vanguard data security policy).

    Why CLI instead of boto3 put_object:
        The --recursive flag and --sse-kms-key-id are easier to express as a single CLI command
        than looping over files with boto3; and the KMS key alias is environment-specific.

    Called by: SFTP ingestion script (orard_Prod10.py) after downloading files locally.
    """
    try:
        if env in ('eng', 'test', 'prod'):
            cmd = (
                f"aws s3 cp /tmp/glue_transfer {sourcebucket} "
                f"--recursive --sse aws:kms --sse-kms-key-id alias/VGI-KMS-S3"
            )
        else:
            raise ValueError("Invalid environment.")

        child_process = subprocess.Popen(cmd, shell=True)
        stdout, stderr = child_process.communicate()
        rc = child_process.returncode

        if int(rc) > 0:
            print("Error copying files from /tmp/glue_transfer to S3.")
            sys.exit(1)
    except Exception as e:
        print(f"S3 upload error: {e}")
        sys.exit(1)


def file_move_archive_folder(file_name, env):
    """
    Archives a processed file by:
        1. Copying it from Incoming/ to Archive/<filename>_YYYY-MM-DD.<ext>
        2. Deleting the original from Incoming/

    S3 does not support native rename/move — copy + delete is the standard pattern.
    KMS encryption is applied per environment using environment-specific key ARNs.

    Args:
        file_name: File to archive (without path prefix)
        env      : Environment identifier
    """
    s3 = boto3.client('s3')
    try:
        if env == 'eng':
            bucket_name  = "vgi-institutional-eng-us-east-1-ily-etl-s3-source"
            ssekms_key_id = "arn:aws:kms:us-east-1:169470052459:key/6369003d-4ce4-4df4-923a-d3f4242d0537"
        elif env == 'test':
            bucket_name  = "vgi-institutional-test-us-east-1-ily-etl-s3-source"
            ssekms_key_id = "arn:aws:kms:us-east-1:213654017801:key/84644305-1e24-4443-8b36-da64eb567cfc"
        elif env == 'prod':
            bucket_name  = "vgi-institutional-prod-us-east-1-ily-etl-s3-source"
            ssekms_key_id = "arn:aws:kms:us-east-1:192891038817:key/0538d146-e1ea-4e81-a68c-7579686fc95c"
        else:
            raise ValueError("Invalid environment. Use 'eng', 'test', or 'prod'.")

        source_key       = "Incoming/" + file_name
        current_date     = dt.now().strftime("%Y-%m-%d")

        # Build archive filename: <name>_YYYY-MM-DD.<ext>
        name_part, ext_part        = file_name.rsplit('.', 1)
        file_name_with_date        = f"{name_part}_{current_date}.{ext_part}"
        destination_key            = "Archive/" + file_name_with_date

        # Step 1: Copy to Archive with KMS encryption (preserving original file integrity)
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=destination_key,
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=ssekms_key_id
        )
        print(f"File archived to: {destination_key}")

        # Step 2: Delete original from Incoming/ after successful copy
        s3.delete_object(Bucket=bucket_name, Key=source_key)
        print(f"Original deleted from: {source_key}")
    except Exception as e:
        print(f"Error during file archive operation: {str(e)}")
        raise


def execute_s3_sql_file(env, file_name):
    """
    Reads a SQL script file from the project's S3 ETL code bucket and returns its content.
    Allows SQL logic to be stored in S3 and loaded dynamically at runtime (avoids hardcoding).

    Returns:
        str: SQL script content, or False on error.
    """
    try:
        if env == 'eng':
            bucket_name = "application-payload-vgi-institutional-eng-us-east-1"
        elif env == 'test':
            bucket_name = "application-payload-vgi-institutional-test-us-east-1"
        elif env == 'prod':
            bucket_name = "application-payload-vgi-institutional-prod-us-east-1"
        else:
            raise ValueError("Invalid environment.")

        file_path = "ILY/IPORTGluePythonScript/iport-etlcode/" + file_name
        s3_client = boto3.client('s3')
        response  = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error reading SQL file from S3: {str(e)}")
        traceback.print_exc()
        return False


def get_config_SFTP(env):
    """
    Extracts SFTP server configuration (hostname, port, secret ARN) from config.
    Used by the SFTP ingestion script (orard_Prod10.py) before connecting via paramiko.

    Returns:
        tuple: (hostname, port, secret_arn), or tuple of Nones on error.
    """
    try:
        config      = get_config(env)
        sftp_config = config['sftp']
        hostname    = sftp_config['hostname']
        port        = sftp_config['port']
        secret_arn  = sftp_config['secret_arn']
        return (hostname, port, secret_arn)
    except Exception as e:
        print(f"Error extracting SFTP config: {str(e)}")
        return (None, None, None)


################################################################################
# SECTION 7 — Audit and Metadata Management
################################################################################

def get_audit_run_dt(args, schema_name, cursor):
    """
    Determines the audit run timestamp for the current ETL cycle.

    Two modes:
        1. Explicit: If additional_params contains a valid 'YYYY-MM-DD HH:MM:SS' timestamp,
           use it directly (supports manual reruns with specific dates).
        2. Dynamic: If additional_params is 'dummy' or blank, look up MAX(audit_run_ts)
           from the audit dimension table — uses the most recent completed run's timestamp.

    Returns:
        tuple: (audit_run_ts_full str 'YYYY-MM-DD HH:MM:SS', audit_run_dt_date str 'YYYY-MM-DD')

    Raises:
        ValueError: If date format is invalid.
    """
    try:
        param_val = args['additional_params'].split()[0]
        if param_val and param_val != 'dummy':
            # Explicit timestamp provided — parse and reformat
            audit_run_dt = dt.strptime(param_val, '%Y-%m-%d %H:%M:%S')
            return audit_run_dt.strftime('%Y-%m-%d %H:%M:%S'), audit_run_dt.strftime('%Y-%m-%d')
        else:
            # Derive from audit dimension table (most recent completed cycle)
            audit_run_dt_sql = f"SELECT MAX(audit_run_ts) AS audit_run_dt FROM {schema_name}.taudt"
            cursor.execute(audit_run_dt_sql)
            audit_run_dt_str = cursor.fetchone()[0]
            audit_run_dt     = dt.strptime(audit_run_dt_str, '%Y-%m-%d %H:%M:%S')
            return audit_run_dt.strftime('%Y-%m-%d %H:%M:%S'), audit_run_dt.strftime('%Y-%m-%d')
    except ValueError:
        raise ValueError("Invalid date format. Expected 'YYYY-MM-DD HH:MM:SS'.")
    except Exception as e:
        print(f"Error getting audit run date: {str(e)}")
        raise e


def insert_update_cycle_control_table(env, cycle_flag, cycle_name, cycle_data_date,
                                       rd_cursor, rd_tgt_schema, target_table, cycle_end_ts=None):
    """
    Upserts a row in the ETL cycle control table to track pipeline start/end lifecycle.

    States:
        'start' → marks cycle as started (CYCLE_COMPLETE_STATS = 'N')
        'end'   → marks cycle as complete (CYCLE_COMPLETE_STATS = 'Y', stamps end timestamp)

    Logic:
        If a row already exists for (CYCLE_NAME, CYCLE_DATA_DATE):
            - 'start': UPDATE — reset start timestamp, clear end timestamp
            - 'end'  : UPDATE — set CYCLE_COMPLETE_STATS = 'Y', stamp end timestamp
        If no row exists (first run for this date):
            - INSERT new row with start timestamp and CYCLE_COMPLETE_STATS = 'N'

    Args:
        cycle_flag      : 'start' or 'end'
        cycle_name      : Logical cycle name (e.g., 'IADM_ETL_DAILY_CYCLE')
        cycle_data_date : Date this cycle processes (e.g., '2025-01-15')
        cycle_end_ts    : Optional — explicit end timestamp (defaults to CURRENT_TIMESTAMP)
    """
    try:
        cycle_end_timestamp = cycle_end_ts if cycle_end_ts else 'Null'

        # Check if a row already exists for this cycle + date combination
        check_existing_sql = f"""
            SELECT COUNT(*) AS cycle_count
            FROM {rd_tgt_schema}.{target_table}
            WHERE CYCLE_DATA_DATE = '{cycle_data_date}'
              AND CYCLE_NAME = '{cycle_name}'
        """
        rd_cursor.execute(check_existing_sql)
        cycle_existing_count = rd_cursor.fetchone()[0]

        if cycle_existing_count > 0 and cycle_flag == 'start':
            # Row exists + starting again (rerun) — reset start time, clear end time
            update_sql = f"""
                UPDATE {rd_tgt_schema}.{target_table}
                SET CYCLE_COMPLETE_STATS = 'N',
                    CYCLE_START_TS = TO_TIMESTAMP(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF1'),
                    CYCLE_END_TS   = {cycle_end_timestamp}
                WHERE CYCLE_NAME = '{cycle_name}'
                  AND CYCLE_DATA_DATE = '{cycle_data_date}'
            """
            rd_cursor.execute(update_sql)

        elif cycle_existing_count > 0 and cycle_flag == 'end':
            # Row exists + cycle completing — mark done and stamp end time
            update_end_ts_sql = f"""
                UPDATE {rd_tgt_schema}.{target_table}
                SET CYCLE_COMPLETE_STATS = 'Y',
                    CYCLE_END_TS = TO_TIMESTAMP(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF1')
                WHERE CYCLE_NAME = '{cycle_name}'
                  AND CYCLE_DATA_DATE = '{cycle_data_date}'
            """
            rd_cursor.execute(update_end_ts_sql)

        else:
            # No existing row — first run for this date, insert new cycle record
            insert_sql = f"""
                INSERT INTO {rd_tgt_schema}.{target_table}
                    (CYCLE_NAME, CYCLE_DATA_DATE, CYCLE_COMPLETE_STATS, CYCLE_START_TS, CYCLE_END_TS)
                VALUES (
                    '{cycle_name}',
                    '{cycle_data_date}',
                    'N',
                    TO_TIMESTAMP(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF1'),
                    NULL
                )
            """
            rd_cursor.execute(insert_sql)
    except Exception as e:
        print(f"Error upserting cycle control table: {str(e)}")
        raise e


def insert_tdbse_status_record(rd_tgt_schema, audt_run_ts, target_table, cursor):
    """
    Inserts a status tracking record into the tdbse_status table.

    Links the current load to:
        - DT_DIMENSION_KEY: date dimension key matching the audit run date
        - AUDT_DIMENSION_KEY: audit dimension key matching the audit run timestamp

    Used for lineage and completeness tracking — downstream reporting
    queries tdbse_status to determine which tables have been loaded for a given cycle date.

    Returns:
        True on success, False on error.
    """
    try:
        inst_sts_sql = f"""
            INSERT INTO {rd_tgt_schema}.tdbse_status VALUES (
                (SELECT DT_DIMENSION_KEY
                 FROM {rd_tgt_schema}.TDT
                 WHERE ACTUL_DT = DATE('{audt_run_ts}')),
                '{target_table}',
                'N',
                DATE('{audt_run_ts.split()[0]}') + INTERVAL '1 day',
                (SELECT audt_dimension_key
                 FROM {rd_tgt_schema}.taudt
                 WHERE audt_run_ts = '{audt_run_ts}')
            )
        """
        cursor.execute(inst_sts_sql)
        return True
    except Exception as e:
        print(f"Error inserting tdbse_status record: {str(e)}")
        traceback.print_exc()
        return False


################################################################################
# SECTION 8 — Data Validation & CDC
################################################################################

def compare_table_data(temp_schema, main_schema, table_name, cursor):
    """
    Performs structural and data comparison between two Oracle table versions:
        temp_schema.table_name  (new/candidate version)
        main_schema.table_name  (existing/production version)

    Checks:
        1. Row count difference (temp vs main)
        2. Column-level MINUS query — finds columns with differing values
        3. Sample mismatched rows (full row diff) if column mismatches found

    Uses Oracle JDBC ResultSet API (cursor.executeQuery) since this is a JVM-bridge cursor.

    Returns:
        dict: {
            'column_mismatches'    : list of column names with differences,
            'temp_count'           : int,
            'main_count'           : int,
            'row_count_diff'       : int (temp - main),
            'sample_mismatched_rows': list of {col: val} dicts
        }
        Returns dict with error key and -1 counts on failure.
    """
    try:
        # Get column list for the table (from Oracle data dictionary)
        column_query = f"""
            SELECT column_name
            FROM all_tab_columns
            WHERE table_name = '{table_name.upper()}'
              AND owner      = '{temp_schema.upper()}'
        """
        result_set = cursor.executeQuery(column_query)
        columns = []
        while result_set.next():
            columns.append(result_set.getString(1))

        # Row count comparison: temp vs main
        row_count_query = f"""
            SELECT
                (SELECT COUNT(*) FROM {temp_schema}.{table_name}) AS temp_count,
                (SELECT COUNT(*) FROM {main_schema}.{table_name}) AS main_count
            FROM dual
        """
        count_result = cursor.executeQuery(row_count_query)
        count_result.next()
        temp_count    = count_result.getInt(1)
        main_count    = count_result.getInt(2)
        row_count_diff = temp_count - main_count

        # Column-level MINUS check: find which columns have differing values
        column_mismatches = []
        for col in columns:
            compare_query = f"""
                SELECT COUNT(*) AS cnt FROM (
                    SELECT {col} FROM {temp_schema}.{table_name}
                    MINUS
                    SELECT {col} FROM {main_schema}.{table_name}
                )
            """
            compare_result = cursor.executeQuery(compare_query)
            if compare_result.next():
                if compare_result.getInt(1) > 0:
                    column_mismatches.append(col)

        # Full row diff: collect sample mismatched rows if column mismatches found
        sample_mismatched_rows = []
        if column_mismatches:
            row_compare_query = f"""
                SELECT * FROM (
                    SELECT * FROM {temp_schema}.{table_name}
                    MINUS
                    SELECT * FROM {main_schema}.{table_name}
                )
            """
            row_result   = cursor.executeQuery(row_compare_query)
            meta         = row_result.getMetaData()
            column_count = meta.getColumnCount()

            while row_result.next():
                row_data = {}
                for i in range(1, column_count + 1):
                    col_name = meta.getColumnName(i)
                    value    = row_result.getString(i)
                    row_data[col_name] = value if value is not None else ''
                sample_mismatched_rows.append(row_data)

        return {
            'column_mismatches':     column_mismatches,
            'temp_count':            temp_count,
            'main_count':            main_count,
            'row_count_diff':        row_count_diff,
            'sample_mismatched_rows': sample_mismatched_rows
        }
    except Exception as e:
        print(f"Error comparing table data: {str(e)}")
        traceback.print_exc()
        return {
            'column_mismatches':     [],
            'row_count_diff':        -1,
            'temp_count':            -1,
            'main_count':            -1,
            'sample_mismatched_rows': [],
            'error':                  str(e)
        }


def perform_cdc(before_data, after_data):
    """
    Python in-memory CDC (Change Data Capture) using key-based set comparison.

    Used as a lightweight alternative to SQL-based CDC for smaller datasets.
    Assumes row structure: [CLNT_INDSTRY_CD_BRDG_KEY, NAIC_DIMENSION_KEY, CLNT_DIMENSION_KEY, ...]
    Natural composite key: (row[2], row[1]) = (CLNT_DIMENSION_KEY, NAIC_DIMENSION_KEY)

    Change codes:
        1 = Insert  (key in after, not in before)
        2 = Update  (key in both, but CLNT_INDSTRY_CD_BRDG_KEY [row[0]] differs)
        3 = Delete  (key in before, not in after)

    Args:
        before_data: List of tuples representing the previous state
        after_data : List of tuples representing the current source state

    Returns:
        list of (row_tuple, change_code) pairs
    """
    try:
        # Build key sets for O(1) membership checks
        before_keys = {(row[2], row[1]) for row in before_data}
        after_keys  = {(row[2], row[1]) for row in after_data}
        changes = []

        # Inserts: key in after but not in before
        for row in after_data:
            key = (row[2], row[1])
            if key not in before_keys:
                changes.append((row, 1))

        # Updates: key in both, but the non-key attribute (row[0]) differs
        for after_row in after_data:
            after_key = (after_row[2], after_row[1])
            for before_row in before_data:
                before_key = (before_row[2], before_row[1])
                if after_key == before_key and after_row[0] != before_row[0]:
                    changes.append((after_row, 2))
                    print(f"Update: CLNT_DIMENSION_KEY={after_row[2]}, NAIC_DIMENSION_KEY={after_row[1]}")

        # Deletes: key in before but not in after
        for row in before_data:
            key = (row[2], row[1])
            if key not in after_keys:
                changes.append((row, 3))
                print(f"Delete: CLNT_DIMENSION_KEY={row[2]}, NAIC_DIMENSION_KEY={row[1]}")

        return changes
    except Exception as e:
        print(f"Error in perform_cdc: {str(e)}")
        raise


################################################################################
# SECTION 9 — Staging Table Utilities
################################################################################

def create_all_staging_tables(schema_name, cursor, additional_tables=None):
    """
    Idempotently creates the full set of standard IPORT staging tables.

    All staging tables follow the same schema:
        raw_data       VARCHAR(MAX)  — full raw row loaded via COPY (pipe-delimited or fixed-width)
        rec_flag       CHAR(1)       — record type flag (e.g., 'I' = Insert)
        createdDate_ts TIMESTAMP     — load timestamp
        modified_ts    TIMESTAMP     — last modification timestamp

    CREATE TABLE IF NOT EXISTS ensures this is safe to call on reruns.
    Additional tables can be passed in for job-specific staging needs.

    Args:
        schema_name       : Redshift schema to create tables in (e.g., 'dre11_schema')
        cursor            : Active Redshift cursor
        additional_tables : Optional list of extra table name suffixes to create
    """
    try:
        base_table_names = [
            'TDT', 'TTXN_TYP', 'TVISTA_AVT', 'TAUDT', 'TACCSS_STAT_DLY',
            'TRMD_NOTFY', 'TVGI_CNTCT', 'TLOAN_BAL_FACT', 'TBRK_IN_SERV_FACT',
            'TPLN', 'TFND', 'TCNTRBN_SRC', 'TPROCS', 'TCMPN', 'TFREQ',
            'TBRK_IN_SERV', 'TLOAN', 'TPLN_FND', 'TSUSPN_ACTVTY', 'TCLNT',
            'TDIVSN_LOCN', 'TACCSS_METH', 'TPLN_PROCS', 'TSUSPN', 'TNQP',
            'TEMP_PART', 'TPART', 'TPRS_PRT', 'TLOAN_TXN_FND_FACT', 'TBNFCRY',
            'TPRT_RLLVR_FACT', 'TNQP_APP', 'SUNDAY_TLOAN_BAL_FACT',
            'SUNDAY_TLOAN_TXN_FND_FACT', 'TC_ISS_RTN_VGI', 'TC_VGI_INT_INS',
            'TPRT_SYS_MULT_ID'
        ]
        table_names = base_table_names + (additional_tables if additional_tables else [])

        for table_name in table_names:
            staging_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.STG_{table_name} (
                    raw_data       VARCHAR(MAX),
                    rec_flag       CHAR(1),
                    createdDate_ts TIMESTAMP,
                    modified_ts    TIMESTAMP
                )
            """
            cursor.execute(staging_table_sql)
    except Exception as e:
        print(f"Error creating staging tables: {str(e)}")
        raise e