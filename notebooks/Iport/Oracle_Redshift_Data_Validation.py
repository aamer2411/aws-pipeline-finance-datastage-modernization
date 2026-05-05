"""
Data Validation Script — Oracle vs Redshift

Purpose:
    Compares records between an Oracle source table and a Redshift target table.
    A record is considered "mismatched" if it exists in one system but not the other
    (full-row set difference comparison on the selected column tuple).

Validation Checks Performed:
    1. Column name/order alignment check (case-insensitive) before comparison
    2. Date/timestamp normalization — truncates time portion to prevent false mismatches
    3. Set difference check (bidirectional):
         - Rows in Oracle but missing in Redshift → tagged "Oracle-Redshift"
         - Rows in Redshift but missing in Oracle → tagged "Redshift-Oracle"
    4. Mismatch source tagging in CSV output for easy triage
    5. Oracle session NLS_DATE_FORMAT stabilization for consistent date string parsing

Systems Compared:
    Source: Oracle (schema: iportadm) — Production Oracle 10 / IPORT20
    Target: Amazon Redshift Serverless (schema: dre_schema)

Output:
    Per-table CSV file: <TABLE_NAME>_IDM10RS_mismatched_records.csv

NOTE:
    Credentials are currently hard-coded (for local dev/SAT use only).
    Replace with environment variables or AWS Secrets Manager before production use.
"""

import csv
from datetime import datetime
import oracledb           # Oracle client library (thin mode, no Oracle Instant Client needed)
import redshift_connector  # Amazon Redshift Python connector


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION — Modify these values per validation run
# ──────────────────────────────────────────────────────────────────────────────

# Comma-separated list of table names to validate (no schema prefix — added in fetch functions)
# Example for multiple tables: "TREPLCMT_SOC_SECTY,TCOMM_SEG,TVISN_RULE_DEFNTN"
tab_name_lst = "TREPLCMT_SOC_SECTY"

# Optional WHERE clause to filter rows on both sides before comparison
# Leave blank ("") to compare all rows
# Example: " WHERE dt_dimension_key = '11170'"
where_clause = ""

# Columns to SELECT from Oracle (must logically align with select_column1 for Redshift)
# Comma-separated, no spaces — e.g., "replcmt_soc_secty_key,soc_secty_no"
select_column = "replcmt_soc_secty_key,soc_secty_no"

# Columns to SELECT from Redshift (must match Oracle columns in name and order)
# Having separate variables allows minor aliasing differences between the two systems if needed
select_column1 = "replcmt_soc_secty_key,soc_secty_no"


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 1 — Helper Functions
# ──────────────────────────────────────────────────────────────────────────────

def truncate_time(value):
    """
    Strip the time component from a datetime or date-string value.

    Why: Oracle and Redshift may return the same date with different time components
    (e.g., 2024-01-15 00:00:00 vs 2024-01-15 09:30:00). Without normalization,
    these would appear as mismatches even though the date is identical.

    Handles:
        - datetime objects   → returns .date()
        - 'YYYY-MM-DD' str   → parses and returns date
        - Everything else    → returned unchanged (non-date fields pass through)
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.strptime(value, '%Y-%m-%d').date()
        except ValueError:
            # Not a date-formatted string; return as-is
            return value
    return value


def preprocess_row(row, column_names):
    """
    Normalize all date/timestamp columns in a single row to date-only objects.

    Column detection is heuristic: if the column name contains 'date' or 'timestamp'
    (case-insensitive), truncate_time() is applied to that field's value.

    Args:
        row          : Tuple of raw values from cursor.fetchall()
        column_names : List of column name strings (from cursor.description)

    Returns:
        Tuple with date/timestamp fields normalized — ready for set comparison
    """
    processed_row = []
    for col, value in zip(column_names, row):
        if "date" in col.lower() or "timestamp" in col.lower():
            processed_row.append(truncate_time(value))
        else:
            processed_row.append(value)
    return tuple(processed_row)


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 2 — Data Access Functions
# ──────────────────────────────────────────────────────────────────────────────

def fetch_oracle_records(tab_name):
    """
    Connect to Oracle and fetch records for the given table.

    Steps:
        1. Connect using oracledb (thin mode — no Instant Client needed)
        2. Set NLS_DATE_FORMAT so date values are consistently formatted as 'YYYY-MM-DD'
           when returned as strings (driver-version dependent behavior)
        3. Execute SELECT with configured columns and optional WHERE clause
        4. Normalize datetime values to date-only during fetch
        5. Close connection cleanly

    Args:
        tab_name : Table name (schema prefix 'iportadm.' is prepended internally)

    Returns:
        column_names (list) : Column names from cursor.description
        records      (list) : List of normalized row tuples
    """
    # ⚠️  Replace hard-coded credentials with Secrets Manager or env vars for production
    oracle_username = "<your-oracle-username>"
    oracle_password = "<your-oracle-password>"
    oracle_dsn      = "//<oracle-host>:1521/<oracle-service>"
    oracle_table    = f"iportadm.{tab_name}"

    connection = oracledb.connect(
        user=oracle_username,
        password=oracle_password,
        dsn=oracle_dsn
    )
    print("Oracle connection success")

    cursor = connection.cursor()

    # Stabilize date output format — ensures date values returned as strings use YYYY-MM-DD
    # This is a session-level setting (does not persist beyond this connection)
    cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")

    # Fetch only the explicitly configured columns (not SELECT *)
    # This prevents schema drift from causing false mismatches on unrelevant columns
    cursor.execute(f"SELECT {select_column} FROM {oracle_table}{where_clause}")

    # Extract column names from cursor metadata for later normalization and CSV headers
    column_names = [desc[0] for desc in cursor.description]

    # Fetch all rows and normalize datetime fields to date-only
    records = []
    for row in cursor.fetchall():
        processed_row = [
            value.date() if isinstance(value, datetime) else value
            for value in row
        ]
        records.append(tuple(processed_row))

    cursor.close()
    connection.close()
    print(f"Fetched {len(records)} records from Oracle table {oracle_table}")
    return column_names, records


def fetch_redshift_records(tab_name):
    """
    Connect to Redshift Serverless and fetch records for the given table.

    Note: Date normalization is NOT applied here at fetch time — it is handled
    uniformly inside compare_records() via preprocess_row() for both sides,
    ensuring consistent treatment regardless of driver-level type differences.

    Args:
        tab_name : Table name (schema prefix 'dre_schema.' is prepended internally)

    Returns:
        column_names (list) : Column names from cursor.description
        records      (list) : List of raw row tuples (normalized during comparison)
    """
    # ⚠️  Replace hard-coded credentials with Secrets Manager or env vars for production
    redshift_host     = "<your-redshift-endpoint>"
    redshift_port     = 5439
    redshift_dbname   = "<your-redshift-db>"
    redshift_username = "<your-redshift-username>"
    redshift_password = "<your-redshift-password>"
    redshift_table    = f"dre_schema.{tab_name}"

    connection = redshift_connector.connect(
        host=redshift_host,
        port=redshift_port,
        database=redshift_dbname,
        user=redshift_username,
        password=redshift_password
    )
    print("Redshift connection success")

    cursor = connection.cursor()

    # Use select_column1 (Redshift-side column list) — allows minor aliasing differences
    # between Oracle and Redshift column names if needed in future
    cursor.execute(f"SELECT {select_column1} FROM {redshift_table}{where_clause}")

    column_names = [desc[0] for desc in cursor.description]
    records = cursor.fetchall()

    cursor.close()
    connection.close()
    print(f"Fetched {len(records)} records from Redshift table {redshift_table}")
    return column_names, records


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 3 — Comparison Logic
# ──────────────────────────────────────────────────────────────────────────────

def compare_records(oracle_records, oracle_columns, redshift_records, redshift_columns):
    """
    Perform bidirectional set difference comparison between Oracle and Redshift records.

    Approach:
        - Convert both record lists to Python sets (after normalization)
        - Set subtraction finds rows unique to each side
        - O(1) average lookup per row → efficient even for large datasets

    Why set difference instead of row-by-row loop:
        The commented-out alternate version at the bottom of this file shows the O(n²)
        approach (nested loop). The set-based approach is significantly faster for
        large tables (e.g., 500M rows in IPORT as per project scope).

    Args:
        oracle_records    : List of raw row tuples from Oracle
        oracle_columns    : Column name list (for preprocess_row normalization)
        redshift_records  : List of raw row tuples from Redshift
        redshift_columns  : Column name list (for preprocess_row normalization)

    Returns:
        List of (row_tuple, direction_tag) where direction_tag is:
            "Oracle-Redshift" — row in Oracle but missing from Redshift
            "Redshift-Oracle" — row in Redshift but missing from Oracle
    """
    mismatched_records = []

    # Normalize both sets before comparison to avoid false mismatches on date formatting
    oracle_set   = set(preprocess_row(row, oracle_columns)   for row in oracle_records)
    redshift_set = set(preprocess_row(row, redshift_columns) for row in redshift_records)

    # Rows present in Oracle but absent in Redshift → data missing from Redshift
    oracle_to_redshift = oracle_set - redshift_set
    for record in oracle_to_redshift:
        mismatched_records.append((record, "Oracle-Redshift"))

    # Rows present in Redshift but absent in Oracle → extra rows in Redshift
    redshift_to_oracle = redshift_set - oracle_set
    for record in redshift_to_oracle:
        mismatched_records.append((record, "Redshift-Oracle"))

    return mismatched_records


# Alternate O(n²) comparison (commented out — retained for reference)
# Use only for very small datasets where set hashing is problematic (e.g., unhashable types)
# def compare_records(oracle_records, oracle_columns, redshift_records, redshift_columns):
#     mismatched_records = []
#     processed_oracle    = [preprocess_row(row, oracle_columns)   for row in oracle_records]
#     processed_redshift  = [preprocess_row(row, redshift_columns) for row in redshift_records]
#     for oracle_row in processed_oracle:
#         if oracle_row not in processed_redshift:
#             mismatched_records.append((oracle_row, "Oracle-Redshift"))
#     for redshift_row in processed_redshift:
#         if redshift_row not in processed_oracle:
#             mismatched_records.append((redshift_row, "Redshift-Oracle"))
#     return mismatched_records


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 4 — Output / Reporting
# ──────────────────────────────────────────────────────────────────────────────

def write_to_csv(mismatched_records, column_names, output_file):
    """
    Write all mismatched rows to a CSV file with a trailing 'Source' column.

    The 'Source' column indicates the direction of mismatch:
        "Oracle-Redshift" → present in Oracle, missing in Redshift
        "Redshift-Oracle" → present in Redshift, missing in Oracle

    Args:
        mismatched_records : List of (row_tuple, source_tag) from compare_records()
        column_names       : Column header list (from Oracle cursor.description)
        output_file        : Output CSV file path/name
    """
    # Append 'Source' as an extra column to identify mismatch direction
    column_names_with_source = column_names + ["Source"]

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(column_names_with_source)  # Write header row
        for record, source in mismatched_records:
            writer.writerow(list(record) + [source])

    print(f"Mismatched records written to {output_file}")
    print(f"Total mismatched records: {len(mismatched_records)}")


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 5 — Main Orchestration
# ──────────────────────────────────────────────────────────────────────────────

def main():
    """
    Main driver — orchestrates the full validation loop across all configured tables.

    Flow per table:
        1. Fetch Oracle records (with date normalization)
        2. Fetch Redshift records
        3. Validate column alignment (case-insensitive) — abort if mismatch detected
        4. Compare records using set difference
        5. Write mismatches to CSV file named: <TABLE>_IDM10RS_mismatched_records.csv
    """
    for tab_name in tab_name_lst.split(","):
        tab_name = tab_name.strip()

        # Skip empty tokens (e.g., trailing comma in tab_name_lst)
        if not tab_name:
            continue

        print(f"\n{'='*60}")
        print(f"Processing table: {tab_name}")
        print(f"{'='*60}")

        # Fetch data from both systems
        oracle_columns,   oracle_records   = fetch_oracle_records(tab_name)
        redshift_columns, redshift_records = fetch_redshift_records(tab_name)

        # Column alignment guard: if column names don't match (case-insensitive),
        # comparison results would be meaningless — stop early and report the issue
        if [col.lower() for col in oracle_columns] != [col.lower() for col in redshift_columns]:
            print("⚠️  Warning: Column names do not match between Oracle and Redshift!")
            print(f"   Oracle   Columns : {oracle_columns}")
            print(f"   Redshift Columns : {redshift_columns}")
            print("   Skipping comparison — fix column config before rerunning.")
            return  # Abort all tables; fix config and rerun

        # Perform bidirectional set difference comparison
        mismatched_records = compare_records(
            oracle_records,
            oracle_columns,
            redshift_records,
            redshift_columns
        )

        # Write mismatches to a per-table CSV for review
        # Filename format: <TABLE_NAME>_IDM10RS_mismatched_records.csv
        output_file = f"{tab_name}_IDM10RS_mismatched_records.csv"
        write_to_csv(mismatched_records, oracle_columns, output_file)


if __name__ == "__main__":
    main()