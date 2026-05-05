# Notebooks

AWS Glue ETL scripts and supporting orchestration code for the Vanguard IIG DataStage modernization project. Scripts are organized into two sub-folders matching the two application pipelines: `Non-Iport/` and `Iport/`.

All Glue scripts are standard Python files runnable on AWS Glue 4.0 (PySpark 3.3, Python 3.10). Lambda and configuration files are standard Python modules deployable as Lambda function packages.

---

## Non-Iport/

These scripts handle the Non-IPORT pipeline: three sub-projects (BRD/BRN Bank Reconciliation, CST/CTQ Compliance Testing Service, OMG/PGX Pageflex ETL) covering financial data and money movement for institutional plan sponsors.

| File | Type | Purpose |
|------|------|---------|
| `Glue_Job_Normal.py` | AWS Glue ETL | Full three-stage PySpark ETL job. Parses a fixed-width mainframe file from S3, applies DataStage-equivalent transformations across Parallel, Common, and SOC stages, enriches with CLIENTID and POID reference files via LEFT JOINs, and writes the final SOC output to the `TLM_OUT/` S3 folder. Intermediate files are written to `Intermediate/` between stages and deleted in a `finally` block on completion. |
| `Glue_Job_DB2_to_S3.py` | AWS Glue ETL | Extracts data from an on-premises IBM DB2 table via JDBC, applies JDBC query pushdown to filter at source, and writes the result to S3 as a pipe-delimited flat file. Used to produce the POID reference file consumed by dependent ETL jobs. |
| `Glue_Job_DB2_to_S3_Config.json` | Glue job config | AWS Glue job definition JSON for the DB2 extract job. Includes worker type, Glue version, script path, Python library path, and static job parameters. |
| `Glue_Job_Oracle_to_S3.py` | AWS Glue ETL | Extracts data from an on-premises Oracle database via JDBC and writes the result to S3 as a pipe-delimited flat file. Used to produce the PageNumber reference file consumed by dependent ETL jobs. Connects via a Glue Connection within the VPC; credentials retrieved from AWS Secrets Manager. |
| `Glue_Utils.py` | Shared utility module | Imported by all Non-IPORT Glue jobs. Provides: Spark/GlueContext initialization (module-level singletons), S3 file discovery with pattern matching, fixed-width data splitting with byte-offset parsing and Decimal type handling, KMS-encrypted output writers (`output_store`, `dependent_output_store`), S3 file deletion, and schema dummy DataFrames (SOT, SOP, SOC) used as empty templates when source data is unavailable. |
| `Lambda_Main.py` | AWS Lambda | The `brn-glue-job-orchestrator` Lambda function. Routes across four invocation modes from a single `lambda_handler` entry point: (1) SQS/S3 event — parses the new file key, resolves companion file dependencies, validates timestamps, and triggers the Step Functions state machine; (2) file readiness check — paginated S3 scan to verify all companion files have arrived; (3) DynamoDB status update — upserts the final job run status after Glue completes; (4) Glue-Retry — scans DynamoDB for FAILED records and re-triggers Step Functions for each one. |
| `Lambda_Config.py` | Lambda config | File-to-Glue-job mapping dictionary (`file_mapping`). Each key is a tuple of filenames (trigger file + companion files); the value is the Glue job name to run. Standalone jobs have a single-element tuple. Multi-element tuples define a dependency group — the trigger file is the first element, remaining elements are required companions that must be present in S3 before the Glue job starts. |
| `Stepfunction.json` | Step Functions ASL | Amazon States Language definition for the orchestration state machine. Encodes the full dependency-check and retry loop: `CheckRequiredFiles` choice state routes standalone vs dependent jobs; wait states handle the 5-minute and 10-minute polling intervals; Lambda invocations handle file readiness checks and post-job status updates; Glue native integration (`glue:startJobRun.sync`) runs ETL jobs synchronously; catch blocks route failures to the SNS alert + DynamoDB update path. |

---

## Iport/

These scripts handle the IPORT pipeline: business reporting across Yearly, Quarterly, Monthly, and Daily cycles for institutional plan sponsor reporting. Data flows from on-premises sources (UNIX CSV files, DB2 tables) through Redshift staging into Redshift target tables, and then into Oracle 20 as the final downstream database.

| File | Type | Purpose |
|------|------|---------|
| `SFTP_Unix_to_S3.py` | AWS Glue ETL | Connects to an on-premises UNIX server via SFTP, retrieves the relevant CSV file for the current processing cycle, and uploads it to the designated S3 path. Acts as the ingestion step for source data that is not covered by AWS DataSync. |
| `S3_CSV_to_Redshift_Staging.py` | AWS Glue ETL | Reads CSV files from S3 (ingested by the SFTP job or DataSync) and loads them into Redshift staging tables. Handles column mapping, data type casting, and truncate-load or append-load strategies based on the table's reload pattern. Connects to Redshift Serverless via psycopg2 using credentials from AWS Secrets Manager. |
| `Redshift_Glue_Utils.py` | Shared utility module | Shared utilities for all IPORT Glue jobs. Provides: Glue argument parsing, environment-specific config extraction (Redshift host, Oracle ARN, JDBC URLs, driver paths), psycopg2 Redshift connection management, cx_Oracle connection management, and common helper functions used across IPORT transformation scripts. Mirrors the role of `Glue_Utils.py` for the Non-IPORT pipeline. |
| `Redshift_Table_DDL.sql` | SQL DDL | Data Definition Language scripts for IPORT Redshift staging and target tables. Includes `CREATE TABLE` statements with column definitions, data types, distribution styles (`AUTO`), and sort keys. Used to provision or re-provision the Redshift schema in any environment (ENG / SAT / Prod). |
| `SCD2_Implementation_Iport.py` | AWS Glue ETL | Implements SCD Type 2 change tracking for IPORT reporting tables. Reads current target data into a `BEFORE` staging table and fresh source data (from DB2 via JDBC) into an `AFTER` staging table. Derives a CDC staging table with change codes (0=no change, 1=insert, 2=delete, 3=update). Applies SCD2 changes to the Redshift target table: expires changed rows (sets expiration date to process_date) and inserts new active versions. Mirrors the final result to Oracle 20 via cx_Oracle and triggers an Oracle stored procedure to recompute statistics. Credentials for both Redshift and Oracle are retrieved from Secrets Manager at runtime. |
| `Oracle_Redshift_Data_Validation.py` | Validation script | Post-load validation script for IPORT sign-off. Connects to Oracle 20 (target) and Oracle 10 (legacy, via DB link) and runs three validation passes: (1) row count comparison using `SELECT COUNT(*)`; (2) uniqueness check using `SELECT DISTINCT` on key columns; (3) data difference check using Oracle `MINUS` queries to identify records present in Oracle 10 but missing from Oracle 20, and vice versa. Outputs a summary report to be reviewed during the client sign-off meeting. Requires a 95% match threshold for IPORT acceptance. |

---

## Credential Placeholders

Scripts that connect to Redshift, Oracle, or DB2 contain placeholder values that must be replaced before running in a non-production environment:

```python
# Redshift (Non-IPORT Glue Utils)
redshift_host     = "<your-redshift-endpoint>"
redshift_db       = "<your-database-name>"

# Oracle (IPORT scripts)
oracle_secret_arn = "<your-secrets-manager-arn>"

# DB2 (Non-IPORT Glue jobs)
jdbc_url          = "jdbc:db2://<host>:<port>/<database>"
```

In production, all secrets are retrieved from AWS Secrets Manager using Boto3:

```python
import boto3, json
client  = boto3.client('secretsmanager')
secret  = json.loads(client.get_secret_value(SecretId=secret_arn)['SecretString'])
password = secret['password']
```

S3 bucket names and schema names are passed as Glue job parameters (`--sourcebucketname`, `--schema_name`, etc.) so the same script runs across ENG, SAT, and Prod without code changes.
