####################################################################################################
# DS Job Name   : IPTUDA01
# Source Table  : VRLDEF1 (IDP_DB2_CURRENT.TRLDEF)  — DB2 on-premises
# Target Table  : TVISN_RULE_DEFNTN                  — Redshift + Oracle
#
# Script Purpose:
#   a. Load current (as-of process_date) target data → STG_BEFORE_DS_TVISN_RULE_DEFNTN
#   b. Load current source data from DB2              → STG_AFTER_DS_TVISN_RULE_DEFNTN
#   c. Derive CDC between BEFORE and AFTER            → STG_CDC_TVISN_RULE_DEFNTN
#   d. Apply SCD Type 2 changes to Redshift target table (expire old + insert new versions)
#   e. Mirror changes to Oracle 20 downstream DB
#   f. Recompute Oracle statistics via stored procedure
#
# CDC Change Codes (in STG_CDC_TVISN_RULE_DEFNTN):
#   0 = NO CHANGE
#   1 = INSERT   (new RULE_ID in source, not in target)
#   2 = DELETE   (RULE_ID in target but disappeared from source)
#   3 = UPDATE   (RULE_ID in both, but one or more attributes differ)
#
# SCD2 Key Fields:
#   Natural Key    : RULE_ID
#   Surrogate Key  : VISN_RULE_DEFNTN_KEY
#   Effective Date : IPRT_EFFTV_DT
#   Expiration Date: IPRT_EXPIRN_DT  (open/current row = 4096-12-31)
#   Load Timestamp : BUILD_DT
####################################################################################################

import glue_utils  # Shared utility module: connections, config, helpers, argument parsing

####################################################################################################
# SECTION 1 — Parse Glue Job Arguments
####################################################################################################

# JOB_NAME is required by AWS Glue internally even if not used explicitly in the script logic
args = glue_utils.getResolvedOptions(
    glue_utils.sys.argv,
    ['JOB_NAME', 'schema_name', 'additional_params', 'env']
)

# additional_params format: "<process_date> ..."
# If process_date is 'dummy', it will be derived dynamically from the cycle control table
process_date = args['additional_params'].split()[0]

# schema_name format: "<rd_stg_schema>,<rd_tgt_schema>,<ora_schema>,<db2_schema>"
# rd_stg_schema  — Redshift staging schema (e.g., dre11_schema)
# rd_tgt_schema  — Redshift target schema  (e.g., dre_schema)
# ora_schema     — Oracle target schema    (e.g., iportadm)
# db2_schema     — DB2 source schema alias (e.g., rsdret00rsdret04all)
rd_stg_schema, rd_tgt_schema, ora_schema, db2_schema = args['schema_name'].split(',')

# Environment flag used to resolve environment-specific config (dev / qa / prod)
env = args['env'].split()[0]

####################################################################################################
# SECTION 2 — Extract Configuration and Establish DB Connections
####################################################################################################

# Extract all environment-specific config values (URLs, ARNs, driver paths, etc.)
# Unused variables are preserved for compatibility with the shared config extraction function
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

# Establish psycopg2 connection to Redshift (returns cursor + connection)
rd_cursor, rd_conn = glue_utils.redshiftdb_connection(env)

# Establish cx_Oracle connection to Oracle 20 (returns cursor + connection)
ora_cursor, ora_conn = glue_utils.oracle_connection(env)

####################################################################################################
# SECTION 3 — Table Identifiers
####################################################################################################

source_table = 'idp_db2_current.TRLDEF'   # DB2 source: vision rule definition reference table
target_table = 'TVISN_RULE_DEFNTN'        # Target: both Redshift and Oracle

####################################################################################################
# SECTION 4 — Main SCD2 ETL / CDC Flow
####################################################################################################

try:

    # ── Step 1: Resolve Process Date ──────────────────────────────────────────────────────────────
    # If 'dummy' is passed (e.g., for scheduled runs), derive the process date dynamically
    # from the IADM daily cycle control table instead of a hardcoded value
    if process_date == 'dummy':
        process_date_sql = f"""
            SELECT TO_CHAR(MAX(CYCLE_DATA_DATE), 'YYYY-MM-DD') AS PROCESS_DATE
            FROM {rd_tgt_schema}.TIADM_CYCLE_CTRL_TBL
            WHERE CYCLE_NAME = 'IADM_ETL_DAILY_CYCLE'
        """
        rd_cursor.execute(process_date_sql)
        result = rd_cursor.fetchone()
        if result and result[0]:
            process_date = result[0]
        else:
            raise ValueError("Failed to fetch process date from Redshift TIADM_CYCLE_CTRL_TBL.")

    # ── Step 2: Determine Next Surrogate Key Base ─────────────────────────────────────────────────
    # SCD2 requires a unique surrogate key per version row.
    # We find the current MAX and add 1 as the starting point for new inserts this run.
    # Later, ROW_NUMBER() OVER () is added to this base to ensure uniqueness across the batch.
    visn_rule_defntn_key_sql = f"""
        SELECT NVL(MAX(VISN_RULE_DEFNTN_KEY), 0) + 1
        FROM {rd_tgt_schema}.TVISN_RULE_DEFNTN
    """
    rd_cursor.execute(visn_rule_defntn_key_sql)
    visn_rule_defntn_key = rd_cursor.fetchone()[0]

    # ── Step 3: Build BEFORE Snapshot (Current Active Target Rows as of process_date) ─────────────
    # SCD2 principle: Only compare against the currently active version of each row.
    # Active rows are those where: effective_date <= process_date < expiration_date
    rd_cursor.execute(f"DROP TABLE IF EXISTS {rd_stg_schema}.STG_BEFORE_DS_TVISN_RULE_DEFNTN")

    insert_stg_before_data_set_sql = f"""
        CREATE TABLE {rd_stg_schema}.STG_BEFORE_DS_TVISN_RULE_DEFNTN AS
        SELECT *
        FROM {rd_tgt_schema}.{target_table}
        WHERE IPRT_EFFTV_DT  <= TO_DATE('{process_date}', 'YYYY-MM-DD')
          AND IPRT_EXPIRN_DT  > TO_DATE('{process_date}', 'YYYY-MM-DD')
    """
    rd_cursor.execute(insert_stg_before_data_set_sql)

    # ── Step 4: Build AFTER Snapshot (Current State from DB2 Source) ─────────────────────────────
    # Pull fresh data from DB2 and normalize text fields:
    #   - Empty or whitespace-only strings → replaced with 'UNKNOWN'
    #   - Internal multi-spaces → collapsed to single space via REGEXP_REPLACE
    #   - Leading/trailing spaces → removed via TRIM/LTRIM/RTRIM
    # This normalization prevents false-positive UPDATE detections due to formatting differences
    rd_cursor.execute(f"DROP TABLE IF EXISTS {rd_stg_schema}.STG_AFTER_DS_TVISN_RULE_DEFNTN")

    insert_stg_after_data_set_sql = f"""
        CREATE TABLE {rd_stg_schema}.STG_AFTER_DS_TVISN_RULE_DEFNTN AS
        SELECT
            A.RULE_ID,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_SHRT_DESC_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_SHRT_DESC_TX, '[[:space:]]+', ' '))))
            END AS RULE_SHRT_DESC_TX,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_LNG_DESC_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_LNG_DESC_TX, '[[:space:]]+', ' '))))
            END AS RULE_LNG_DESC_TX,
            A.RULE_EDT_FL,
            A.RULE_PLN_LEVL_FL,
            A.RULE_TXN_LEVL_FL,
            A.RULE_FND_LEVL_FL,
            A.RULE_SRC_LEVL_FL,
            A.RULE_MULT_ALOWD_FL,
            A.RULE_EXT_DSPLY_FL,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_HDG1_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_HDG1_TX, '[[:space:]]+', ' '))))
            END AS RULE_HDG1_TX,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_HDG2_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_HDG2_TX, '[[:space:]]+', ' '))))
            END AS RULE_HDG2_TX,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_HDG3_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_HDG3_TX, '[[:space:]]+', ' '))))
            END AS RULE_HDG3_TX,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_DTL1_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_DTL1_TX, '[[:space:]]+', ' '))))
            END AS RULE_DTL1_TX,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_DTL2_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_DTL2_TX, '[[:space:]]+', ' '))))
            END AS RULE_DTL2_TX,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_DTL3_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_DTL3_TX, '[[:space:]]+', ' '))))
            END AS RULE_DTL3_TX,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_PART_DESC_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_PART_DESC_TX, '[[:space:]]+', ' '))))
            END AS RULE_PRT_DESC_TX,
            CASE
                WHEN LENGTH(TRIM(RTRIM(LTRIM(A.RULE_ASSOC_DESC_TX)))) = 0 THEN 'UNKNOWN'
                ELSE TRIM(RTRIM(LTRIM(REGEXP_REPLACE(A.RULE_ASSOC_DESC_TX, '[[:space:]]+', ' '))))
            END AS RULE_ASSOC_DESC_TX
        FROM {db2_schema}.{source_table} A
    """
    rd_cursor.execute(insert_stg_after_data_set_sql)

    # ── Step 5: Derive CDC — Full Outer Join + Change Classification ──────────────────────────────
    # Full outer join between BEFORE and AFTER on natural key (RULE_ID):
    #   - Only in AFTER (BEFORE_FLAG IS NULL)  → CHANGE_CODE = 1 (INSERT)
    #   - Only in BEFORE (AFTER_FLAG IS NULL)  → CHANGE_CODE = 2 (DELETE / soft expire)
    #   - In both, any attribute differs       → CHANGE_CODE = 3 (UPDATE)
    #   - In both, no attribute differs        → CHANGE_CODE = 0 (NO CHANGE — no DML triggered)
    rd_cursor.execute(f"DROP TABLE IF EXISTS {rd_stg_schema}.STG_CDC_TVISN_RULE_DEFNTN")

    VALID_CDC_STG_TVISN_RULE_DEFNTN_sql = f"""
        CREATE TABLE {rd_stg_schema}.STG_CDC_TVISN_RULE_DEFNTN AS
        SELECT
            COALESCE(CAST(BEFORE_DS.RULE_ID AS INTEGER), CAST(AFTER_DS.RULE_ID AS INTEGER)) AS RULE_ID,
            AFTER_DS.RULE_SHRT_DESC_TX,
            AFTER_DS.RULE_LNG_DESC_TX,
            AFTER_DS.RULE_EDT_FL,
            AFTER_DS.RULE_PLN_LEVL_FL,
            AFTER_DS.RULE_TXN_LEVL_FL,
            AFTER_DS.RULE_FND_LEVL_FL,
            AFTER_DS.RULE_SRC_LEVL_FL,
            AFTER_DS.RULE_MULT_ALOWD_FL,
            AFTER_DS.RULE_EXT_DSPLY_FL,
            AFTER_DS.RULE_HDG1_TX,
            AFTER_DS.RULE_HDG2_TX,
            AFTER_DS.RULE_HDG3_TX,
            AFTER_DS.RULE_DTL1_TX,
            AFTER_DS.RULE_DTL2_TX,
            AFTER_DS.RULE_DTL3_TX,
            AFTER_DS.RULE_PRT_DESC_TX,
            AFTER_DS.RULE_ASSOC_DESC_TX,
            BEFORE_DS.VISN_RULE_DEFNTN_KEY,  -- Surrogate key of current active row (for expiry UPDATE)
            BEFORE_DS.IPRT_EXPIRN_DT,
            BEFORE_DS.IPRT_EFFTV_DT,
            BEFORE_DS.BEFORE_FLAG,
            AFTER_DS.AFTER_FLAG,
            CASE
                WHEN BEFORE_DS.BEFORE_FLAG IS NULL
                 AND AFTER_DS.AFTER_FLAG  IS NOT NULL
                    THEN 1  -- INSERT: new RULE_ID appeared in source

                WHEN BEFORE_DS.BEFORE_FLAG IS NOT NULL
                 AND AFTER_DS.AFTER_FLAG  IS NULL
                    THEN 2  -- DELETE: RULE_ID disappeared from source; expire the existing row

                WHEN BEFORE_DS.BEFORE_FLAG IS NOT NULL
                 AND AFTER_DS.AFTER_FLAG  IS NOT NULL
                 AND (
                        BEFORE_DS.RULE_SHRT_DESC_TX  <> AFTER_DS.RULE_SHRT_DESC_TX  OR
                        BEFORE_DS.RULE_LNG_DESC_TX   <> AFTER_DS.RULE_LNG_DESC_TX   OR
                        BEFORE_DS.RULE_EDT_FL         <> AFTER_DS.RULE_EDT_FL         OR
                        BEFORE_DS.RULE_TXN_LEVL_FL    <> AFTER_DS.RULE_TXN_LEVL_FL    OR
                        BEFORE_DS.RULE_FND_LEVL_FL    <> AFTER_DS.RULE_FND_LEVL_FL    OR
                        BEFORE_DS.RULE_SRC_LEVL_FL    <> AFTER_DS.RULE_SRC_LEVL_FL    OR
                        BEFORE_DS.RULE_MULT_ALOWD_FL  <> AFTER_DS.RULE_MULT_ALOWD_FL  OR
                        BEFORE_DS.RULE_EXT_DSPLY_FL   <> AFTER_DS.RULE_EXT_DSPLY_FL   OR
                        BEFORE_DS.RULE_PLN_LEVL_FL    <> AFTER_DS.RULE_PLN_LEVL_FL    OR
                        BEFORE_DS.RULE_HDG1_TX        <> AFTER_DS.RULE_HDG1_TX        OR
                        BEFORE_DS.RULE_HDG2_TX        <> AFTER_DS.RULE_HDG2_TX        OR
                        BEFORE_DS.RULE_HDG3_TX        <> AFTER_DS.RULE_HDG3_TX        OR
                        BEFORE_DS.RULE_DTL1_TX        <> AFTER_DS.RULE_DTL1_TX        OR
                        BEFORE_DS.RULE_DTL2_TX        <> AFTER_DS.RULE_DTL2_TX        OR
                        BEFORE_DS.RULE_DTL3_TX        <> AFTER_DS.RULE_DTL3_TX        OR
                        BEFORE_DS.RULE_PRT_DESC_TX    <> AFTER_DS.RULE_PRT_DESC_TX    OR
                        BEFORE_DS.RULE_ASSOC_DESC_TX  <> AFTER_DS.RULE_ASSOC_DESC_TX
                    )
                    THEN 3  -- UPDATE: same RULE_ID but one or more attributes differ

                ELSE 0      -- NO CHANGE: same RULE_ID, no attribute differences
            END AS CHANGE_CODE
        FROM (SELECT *, 'Y' AS AFTER_FLAG  FROM {rd_stg_schema}.STG_AFTER_DS_TVISN_RULE_DEFNTN) AFTER_DS
        FULL OUTER JOIN
             (SELECT *, 'Y' AS BEFORE_FLAG FROM {rd_stg_schema}.STG_BEFORE_DS_TVISN_RULE_DEFNTN) BEFORE_DS
          ON AFTER_DS.RULE_ID = BEFORE_DS.RULE_ID
    """
    rd_cursor.execute(VALID_CDC_STG_TVISN_RULE_DEFNTN_sql)

    # ── Step 6: Prepare Redshift DML Statements (not yet executed) ───────────────────────────────

    # Step 6a: Soft-expire old active rows for UPDATE (3) and DELETE (2) cases
    # SCD2 principle: never overwrite history — set expiration date to process_date to "close" the row
    soft_delete_and_update_sql = f"""
        UPDATE {rd_tgt_schema}.{target_table} tgt
        SET IPRT_EXPIRN_DT = TO_DATE('{process_date}', 'YYYY-MM-DD')
        FROM {rd_stg_schema}.STG_CDC_TVISN_RULE_DEFNTN CD
        WHERE CD.CHANGE_CODE IN (2, 3)
          AND tgt.VISN_RULE_DEFNTN_KEY = CD.VISN_RULE_DEFNTN_KEY
    """

    # Step 6b: Insert new current versions for INSERT (1) and UPDATE (3) cases
    # - New surrogate key = pre-computed base + ROW_NUMBER() to ensure uniqueness across batch
    # - IPRT_EXPIRN_DT = 4096-12-31 (sentinel "open/current" indicator)
    # - IPRT_EFFTV_DT  = process_date (this version's start date)
    # - BUILD_DT       = current timestamp truncated to seconds (load audit field)
    insert_and_update_insert_sql = f"""
        INSERT INTO {rd_tgt_schema}.{target_table}
        SELECT
            AD.RULE_ID,
            AD.RULE_SHRT_DESC_TX,
            AD.RULE_LNG_DESC_TX,
            AD.RULE_EDT_FL,
            AD.RULE_TXN_LEVL_FL,
            AD.RULE_FND_LEVL_FL,
            AD.RULE_SRC_LEVL_FL,
            AD.RULE_MULT_ALOWD_FL,
            AD.RULE_EXT_DSPLY_FL,
            AD.RULE_HDG1_TX,
            AD.RULE_HDG2_TX,
            AD.RULE_HDG3_TX,
            AD.RULE_DTL1_TX,
            AD.RULE_DTL2_TX,
            AD.RULE_DTL3_TX,
            AD.RULE_PRT_DESC_TX,
            AD.RULE_ASSOC_DESC_TX,
            {visn_rule_defntn_key} + ROW_NUMBER() OVER () AS VISN_RULE_DEFNTN_KEY,
            TO_DATE('4096-12-31', 'YYYY-MM-DD')               AS IPRT_EXPIRN_DT,
            TO_DATE('{process_date}', 'YYYY-MM-DD')           AS IPRT_EFFTV_DT,
            CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS BUILD_DT,
            AD.RULE_PLN_LEVL_FL
        FROM {rd_stg_schema}.STG_CDC_TVISN_RULE_DEFNTN AD
        WHERE AD.CHANGE_CODE IN (1, 3)
    """

    # ── Step 7: Execute Redshift SCD2 Changes Atomically ─────────────────────────────────────────
    # Both expire + insert are wrapped in a single transaction to ensure no partial state:
    # If the insert fails, the expiry is also rolled back — leaving the table in a consistent state
    rd_cursor.execute("BEGIN;")
    rd_cursor.execute(soft_delete_and_update_sql)
    rd_cursor.execute(insert_and_update_insert_sql)
    rd_cursor.execute("COMMIT;")

    # ── Step 8: Propagate Changes to Oracle 20 ────────────────────────────────────────────────────
    # Oracle is the downstream consumer (IPORT20); it needs to reflect the same dimension state.
    # Strategy: delete-then-reinsert (upsert pattern) for all changed RULE_IDs

    # Step 8a: Fetch list of RULE_IDs that were updated or deleted (to remove from Oracle first)
    select_oracle_delete_sql = f"""
        SELECT TGT.RULE_ID
        FROM {rd_tgt_schema}.{target_table} TGT
        INNER JOIN {rd_stg_schema}.STG_CDC_TVISN_RULE_DEFNTN CD
            ON TGT.RULE_ID = CD.RULE_ID
        WHERE CHANGE_CODE IN (2, 3)
        GROUP BY TGT.RULE_ID
    """
    rd_cursor.execute(select_oracle_delete_sql)
    delete_result = rd_cursor.fetchall()
    RULE_ID_LIST = [str(row[0]) for row in delete_result]

    # Step 8b: Delete affected RULE_IDs from Oracle (only if there are any)
    if len(RULE_ID_LIST) > 0:
        rule_ids_csv = ','.join(RULE_ID_LIST)
        soft_delete_oracle_delete_records = f"""
            DELETE FROM {ora_schema}.{target_table}
            WHERE RULE_ID IN ({rule_ids_csv})
        """
        ora_cursor.execute(soft_delete_oracle_delete_records)

    # Step 8c: Re-insert all changed rows (INSERT + UPDATE + DELETE cases) from Redshift → Oracle
    # This acts as a full re-sync of affected RULE_IDs including their new versioned rows.
    # COALESCE(NULLIF(RULE_PLN_LEVL_FL, ' '), ' ') ensures empty-string safety for Oracle
    oracle_data_insert_sql = f"""
        SELECT
            TGT.RULE_ID,
            TGT.RULE_SHRT_DESC_TX,
            TGT.RULE_LNG_DESC_TX,
            TGT.RULE_EDT_FL,
            TGT.RULE_TXN_LEVL_FL,
            TGT.RULE_FND_LEVL_FL,
            TGT.RULE_SRC_LEVL_FL,
            TGT.RULE_MULT_ALOWD_FL,
            TGT.RULE_EXT_DSPLY_FL,
            TGT.RULE_HDG1_TX,
            TGT.RULE_HDG2_TX,
            TGT.RULE_HDG3_TX,
            TGT.RULE_DTL1_TX,
            TGT.RULE_DTL2_TX,
            TGT.RULE_DTL3_TX,
            TGT.RULE_PRT_DESC_TX,
            TGT.RULE_ASSOC_DESC_TX,
            TGT.VISN_RULE_DEFNTN_KEY,
            TGT.IPRT_EXPIRN_DT,
            TGT.IPRT_EFFTV_DT,
            TGT.BUILD_DT,
            COALESCE(NULLIF(TGT.RULE_PLN_LEVL_FL, ' '), ' ') AS RULE_PLN_LEVL_FL
        FROM {rd_tgt_schema}.{target_table} TGT
        INNER JOIN {rd_stg_schema}.STG_CDC_TVISN_RULE_DEFNTN CD
            ON TGT.RULE_ID = CD.RULE_ID
        WHERE CD.CHANGE_CODE IN (1, 2, 3)
    """
    # Utility copies Redshift query result rows into the Oracle target table
    glue_utils.copy_redshift_to_oracle(ora_schema, target_table, oracle_data_insert_sql, env)

    # ── Step 9: Recompute Oracle Statistics ───────────────────────────────────────────────────────
    # After DML, trigger COMPUTE_STATISTICS stored procedure so Oracle optimizer has
    # fresh metadata (table sizes, column distributions) for efficient query planning
    glue_utils.execute_plsql_procedure(
        env, ora_schema, 'PARTITION_MGT.COMPUTE_STATISTICS', [target_table]
    )

except Exception as e:
    # Log error details and exit with non-zero code to signal Glue job failure to Step Functions
    print(f"Error: {str(e)}")
    exit(1)

finally:
    # Always release all DB connections regardless of success or failure
    # Prevents connection leaks in the Glue container environment
    rd_cursor.close()
    rd_conn.close()
    ora_cursor.close()
    ora_conn.close()