################################################################################
# app.py
#
# AWS Lambda Function: brn-glue-job-orchestrator
#
# Purpose:
#   Central orchestrator for the BRN (non-IPORT) ETL pipeline.
#   Handles four distinct invocation modes — all routed through lambda_handler:
#
#   MODE 1 — SQS/S3 Event (primary path):
#       Triggered by EventBridge → SQS → Lambda when a new file lands in S3.
#       Parses the S3 key, resolves required companion files, performs timestamp
#       validation, and triggers the appropriate AWS Step Function execution.
#
#   MODE 2 — File Readiness Check (called BY Step Functions during polling):
#       Step Functions periodically call this Lambda to check if companion
#       files have arrived in S3 yet. Returns 200 when all files are present,
#       400 while still waiting. Optionally publishes SNS alerts if files are
#       late (Send_sns flag).
#
#   MODE 3 — DynamoDB Status Update (called BY Step Functions post-job):
#       After a Glue job completes (success or failure), Step Functions call
#       this Lambda with the final status to persist in DynamoDB for audit/retry.
#
#   MODE 4 — Glue-Retry (manual re-trigger of FAILED jobs):
#       When invoked with action="start" + metadata.flag="Glue-Retry",
#       scans DynamoDB for all FAILED jobs and re-triggers their Step Functions.
#
# Environment Variables Required:
#   TriggerName     : S3 bucket name for incoming BRN files
#   OutputArn       : Step Function state machine ARN
#   NotificationArn : SNS topic ARN for missing-file alerts
#   SysLevel        : Deployment environment ('eng', 'sat', 'prd')
#   AWS_REGION      : AWS region (default: 'us-east-1')
#
# DynamoDB Table:
#   Name           : BRN-etl-orchestration
#   Partition Key  : brn-etl-partitionKey  (= job_name)
#   Sort Key       : brn-etl-sortKey       (= order_date YYMMDD)
#
# S3 File Key Pattern (BRN_IN path):
#   BRN_IN/<FILE_IDENTIFIER>_<YYMMDD>_<SEQUENCE>.txt
#   Example: BRN_IN/INSBN200.BALANCE_250115_001.txt
#
# S3 File Key Pattern (BRN_IN_BUSINESS path):
#   BRN_IN_BUSINESS/<FILENAME>
#   Fixed filenames: MCR.txt, QIK.PRN, DB_TRUSTEE_CHAR_NAV.txt
################################################################################

import json
import os
import logging
import re
import boto3
from botocore.exceptions import ClientError

# File-to-job mapping: imported from sibling config.py in the same Lambda package
# Structure: {(trigger_file, *required_files): glue_job_name}
from config import file_mapping

# Configure structured logging — output goes to CloudWatch Logs
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Module-level SNS client — reused across invocations (Lambda execution context reuse)
# Only instantiated once per warm container — avoids repeated client init overhead
sns_client = boto3.client('sns')


################################################################################
# HELPER: Trigger a Step Function execution
################################################################################

def stepfunction(arn, input_dict):
    """
    Triggers an AWS Step Function state machine execution with the given input.

    Called when:
      - A new BRN_IN file arrives (MODE 1) → triggers the ETL pipeline
      - Glue-Retry (MODE 4) → re-triggers previously FAILED pipelines

    Args:
        arn        : Full ARN of the Step Function state machine to execute
        input_dict : Dict payload to pass as input to the state machine.
                     Must contain: file_name, order_date, required_files,
                                   job_name, empty_required_files

    Returns:
        dict: Step Functions start_execution response, or None on error.
    """
    logging.info("Triggering Step Function with ARN: %s and input: %s", arn, input_dict)
    sf_client = boto3.client('stepfunctions')
    try:
        response = sf_client.start_execution(
            stateMachineArn=arn,
            input=json.dumps(input_dict)   # Step Functions requires JSON string, not dict
        )
        logging.info("Step Function triggered successfully: %s", response)
    except ClientError as error:
        logging.error(
            "Error triggering Step Function %s: %s",
            arn, error.response['Error']['Message']
        )
        return None
    return response


################################################################################
# HELPER: Parse S3 object key → incoming file identifier + order date
################################################################################

def file_info(object_key):
    """
    Extracts the file identifier and order date from a BRN_IN S3 object key.

    Expected S3 key pattern:
        BRN_IN/<FILE_IDENTIFIER>_<YYMMDD>_<SEQUENCE>.txt
        Example: BRN_IN/INSBN200.BALANCE_250115_001.txt
                 → incoming_file = 'INSBN200.BALANCE'
                 → order_date    = '250115'

    Args:
        object_key : Full S3 object key (e.g., 'BRN_IN/INSBN200.BALANCE_250115_001.txt')

    Returns:
        tuple: (incoming_file, order_date) if pattern matches, else (None, None)

    Note:
        Returns (None, None) for BRN_IN_BUSINESS files — those are handled
        separately in lambda_handler before this function is called.
    """
    logging.info("Extracting file information from object key: %s", object_key)
    order_date = None
    object_key = object_key.strip()

    # Regex breakdown:
    #   BRN_IN/         : literal path prefix (anchors match to BRN_IN folder)
    #   ([\w.]+)        : capture group 1 — file identifier (word chars + dots, e.g., 'INSBN200.BALANCE')
    #   _(\d{6})        : capture group 2 — 6-digit order date (YYMMDD, e.g., '250115')
    #   _\d+\.txt       : sequence number + extension (not captured — only used for validation)
    match = re.search(r"BRN_IN/([\w.]+)_(\d{6})_\d+\.txt", object_key)
    logging.info("match: %s", match)

    if match:
        incoming_file = match.group(1)   # e.g., 'INSBN200.BALANCE'
        order_date    = match.group(2)   # e.g., '250115'
        logging.info("Incoming file: %s", incoming_file)
        return incoming_file, order_date

    logging.info("No match found for object key: %s", object_key)
    return None, None


################################################################################
# HELPER: Resolve required companion files + Glue job name from config mapping
################################################################################

def get_required_files(incoming_file):
    """
    Looks up the file_mapping to determine:
      1. Which companion files must be present before the job runs
      2. Which Glue job to trigger

    Logic:
      - Iterates all keys in file_mapping (each key is a tuple of filenames)
      - If incoming_file appears in a key tuple:
          - If the tuple has only 1 element (just the trigger file): no dependencies
          - Otherwise: all OTHER files in the tuple are the required companion files

    Args:
        incoming_file : File identifier extracted from the S3 key (e.g., 'INSBN200.BALANCE')

    Returns:
        tuple: (required_files list, job_name string)
               required_files = [] if no companions needed
               job_name = None if file not in any mapping group
    """
    required_files = []
    job_name = None

    for file_group, job_name in file_mapping.items():
        if incoming_file in file_group:
            if len(file_group) == 1:
                # Only the trigger file in this group — standalone job, no waiting needed
                logging.info("Standalone file group: %s", file_group)
                required_files = []
            else:
                # Multi-file group — all files EXCEPT the trigger file are required companions
                logging.info("Multi-file group: %s", file_group)
                required_files = [f for f in file_group if f != incoming_file]
            return required_files, job_name

    # File not found in any mapping group
    logging.info("required_files: %s", required_files)
    logging.info("job_name: %s", job_name)
    return [], None


################################################################################
# HELPER: Get LastModified timestamp for a specific S3 object (exact key)
################################################################################

def get_last_modified_timestamps(bucket_name, object_key):
    """
    Returns the LastModified timestamp of the trigger file that just arrived.

    Used for timestamp comparison: companion files must have a LastModified
    timestamp >= the trigger file's timestamp to confirm they are the correct
    day's files (not stale files from a previous run).

    Args:
        bucket_name : S3 bucket name
        object_key  : Full S3 key of the trigger file

    Returns:
        datetime: LastModified timestamp from S3 head_object response

    Raises:
        NoSuchKey   : If the object doesn't exist (shouldn't happen — file just arrived)
        ClientError : On any other S3 API error
        TypeError   : On unexpected response structure
    """
    s3_client = boto3.client('s3')
    logging.info("inside get last modified timestamps")
    logging.info("bucket_name: %s", bucket_name)
    logging.info("object_key: %s", object_key)

    try:
        # head_object is cheaper than get_object — only fetches metadata, not the file body
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        logging.info("response: %s", response)
        last_modified_timestamp = response['LastModified']
        logging.info("last_modified_timestamp: %s", last_modified_timestamp)
        return last_modified_timestamp

    except s3_client.exceptions.NoSuchKey:
        logging.error("Object not found in S3 bucket: %s", object_key)
        raise
    except ClientError as e:
        logging.error("Error getting last modified timestamp: %s", e)
        raise
    except TypeError as e:
        logging.error("TypeError: %s", e)
        raise


################################################################################
# HELPER: Get latest LastModified timestamp for each required companion file
################################################################################

def get_required_files_timestamp(bucket_name, required_files, order_date):
    """
    For each required companion file, finds the most recently modified matching
    object in S3 (using prefix listing) and returns its LastModified timestamp.

    Why prefix listing (not exact key):
        Companion files follow the same naming convention as trigger files:
        <FILE_IDENTIFIER>_<YYMMDD>_<SEQUENCE>.txt
        The sequence number varies — we list all files matching
        'BRN_IN/<FILE>_<YYMMDD>_' and pick the newest one.

    Args:
        bucket_name    : S3 bucket name
        required_files : List of file identifiers to check (e.g., ['NSCC.SETL.TLM.MATCH'])
        order_date     : 6-digit date string YYMMDD (e.g., '250115')

    Returns:
        dict: {file_identifier: LastModified_datetime} for each found file.
              Files not found in S3 are omitted from the result dict.
              An empty dict means none of the required files are present yet.
    """
    logging.info("inside get required files timestamps")
    s3 = boto3.client('s3')
    last_modified_timestamps = {}
    logging.info("required_files:%s", required_files)

    for file in required_files:
        # Prefix: 'BRN_IN/<FILE>_<YYMMDD>_' — matches all sequence variants of this file+date
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"BRN_IN/{file}_{order_date}_"
        )
        logging.info("inside response:%s", response)

        if 'Contents' in response:
            # Pick the most recently modified version if multiple sequences exist
            latest_file = max(response['Contents'], key=lambda obj: obj['LastModified'])
            last_modified_timestamps[file] = latest_file['LastModified']
            logging.info("latest_file: %s", latest_file)

    return last_modified_timestamps


################################################################################
# HELPER: Check if all required companion files are present in S3
# Called by Step Functions during the polling loop (MODE 2)
################################################################################

def check_files_in_s3(bucket_name, order_date, required_files, input_dict):
    """
    Checks whether all required companion files have arrived in S3 for the
    current order date. Searches both BRN_IN (incoming) and Intermediate prefixes.

    Called by Step Functions (not directly by S3 events) during the polling
    wait loop — Step Functions call this repeatedly until all files are present.

    File matching logic:
        - Lists all files under 'BRN_IN/' and 'Intermediate/' prefixes (paginated)
        - Normalizes each filename to '<FILE_IDENTIFIER>_<YYMMDD>' by stripping
          the sequence number and extension (regex: (<FILE>_\d{6})\d+\.txt)
        - Checks each required file against normalized list

    SNS alert behavior:
        If input_dict contains "Send_sns": true AND files are still missing,
        publishes a notification to the SNS topic (env var: NotificationArn).
        This is set by Step Functions on the second+ polling attempt (~15 min delay).

    Args:
        bucket_name    : S3 bucket name
        order_date     : 6-digit date YYMMDD
        required_files : List of file identifiers to verify presence
        input_dict     : Original event payload (used for Send_sns flag + job_name for alert)

    Returns:
        dict: {
            "status_code": 200,                       ← All files present
            "required_files": "FILE1,FILE2",          ← Comma-joined list for Step Functions
            "message": "All required files present"
        }
        OR
        dict: {
            "status_code": 400,                       ← Files still missing
            "message": "Missing files: <list>"
        }
        OR
        dict: {
            "status_code": 500,                       ← S3 access error
            "message": "Error accessing S3 bucket"
        }
    """
    logging.info("Checking required files in S3 bucket: %s", bucket_name)
    missing_files = []
    found_files   = 0
    s3 = boto3.client('s3')

    # --- Inner helper: paginated S3 listing ---
    # Handles buckets with >1000 objects under a prefix (list_objects_v2 max page = 1000)
    def fetch_all_files(bucket_name, prefix):
        """
        Paginates through S3 list_objects_v2 to retrieve all object keys under a prefix.
        Returns a flat list of filenames (last path segment only, no folder prefix).
        """
        all_files          = []
        continuation_token = None

        while True:
            # Use ContinuationToken on subsequent pages if result was truncated
            if continuation_token:
                response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    ContinuationToken=continuation_token
                )
            else:
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            # Extract only the filename portion (strip the prefix path)
            all_files.extend([
                content['Key'].split('/')[-1]
                for content in response.get('Contents', [])
            ])

            # IsTruncated = True means there are more pages to fetch
            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
            else:
                break   # All pages retrieved

        return all_files

    # Fetch from both source folders and merge into one list
    try:
        logging.info("inside the try block: %s", bucket_name)
        files_in_intermediate = fetch_all_files(bucket_name, "Intermediate")
        files_in_incoming     = fetch_all_files(bucket_name, "BRN_IN")
        # Combine both prefix results — companion files could be in either location
        files_in_bucket = files_in_incoming + files_in_intermediate
    except ClientError as e:
        logging.info(f"Error accessing S3 bucket: {e}")
        return {"status_code": 500, "message": "Error accessing S3 bucket"}

    # --- Normalize filenames: strip sequence number + extension ---
    # Pattern: (<FILE_IDENTIFIER>_YYMMDD)\d+\.txt
    # Example: 'NSCC.SETL.TLM.MATCH_250115_001.txt' → 'NSCC.SETL.TLM.MATCH_250115'
    cleaned_files = []
    print("files_in_bucket:", files_in_bucket)
    for file in files_in_bucket:
        match = re.match(r"(.+_\d{6})\d+\.txt", file)
        if match:
            cleaned_file = match.group(1)   # Normalized: '<FILE>_YYMMDD'
            cleaned_files.append(cleaned_file)
        else:
            logging.info("No match found.")

    # --- Check each required file against normalized list ---
    for req_file in required_files:
        expected_file   = f"{req_file}_{order_date}"   # e.g., 'NSCC.SETL.TLM.MATCH_250115'
        matching_files  = [f for f in cleaned_files if f == expected_file]
        if matching_files:
            found_files += 1
        else:
            missing_files.append(expected_file)

    # --- Helper: send SNS alert if files are missing and alerting is requested ---
    def _notify_and_return_missing(missing_files, input_dict):
        """
        Publishes an SNS alert listing missing files + job name, then returns a 400 response.
        Only fires when input_dict['Send_sns'] is truthy (set by Step Functions on 2nd+ poll).
        """
        if input_dict.get("Send_sns", False):
            sns_client      = boto3.client('sns')
            sns_topic_arn   = os.environ['NotificationArn']
            job_name        = input_dict.get("job_name", "Unknown")
            message = (
                f"Required files are still missing in S3 bucket after 15 mins: "
                f"{', '.join(missing_files)}\njob name: {job_name}"
            )
            sns_client.publish(TopicArn=sns_topic_arn, Message=message)
        return {"status_code": 400, "message": f"Missing files: {', '.join(missing_files)}"}

    # No files found at all (all missing)
    if found_files == 0:
        return _notify_and_return_missing(missing_files, input_dict)

    # Some files found but not all (partial presence)
    if missing_files:
        return _notify_and_return_missing(missing_files, input_dict)

    # --- All required files present → return 200 to Step Functions ---
    # required_files list is joined to comma-separated string for the Step Functions payload
    # (Step Functions passes this back to the Glue job as --required_files argument)
    input_dict["required_files"] = ",".join(input_dict["required_files"])
    remaining_files = input_dict["required_files"]
    return {
        "status_code": 200,
        "required_files": str(remaining_files),
        "message": "All required files are present in the bucket."
    }


################################################################################
# HELPER: Insert / update job status in DynamoDB orchestration table
################################################################################

def update_dynamodb_table(order_date, job_name, status):
    """
    Inserts or updates a job status record in the BRN-etl-orchestration DynamoDB table.

    Table schema:
        Partition key : brn-etl-partitionKey = job_name  (e.g., 'BRNETL-UD027')
        Sort key      : brn-etl-sortKey      = order_date (e.g., '250115')
        Attribute     : status               = 'SUCCEEDED' | 'FAILED' | 'RUNNING'

    Cleanup behavior:
        After inserting the current entry, checks the PREVIOUS day's entry for the
        same job. If the previous entry has status 'SUCCEEDED', it is deleted.
        This keeps the table lean — only current + recent failures are retained.
        FAILED records are intentionally retained for the Glue-Retry mechanism (MODE 4).

    Args:
        order_date : 6-digit date string YYMMDD (e.g., '250115')
        job_name   : Glue job name (e.g., 'BRNETL-UD027')
        status     : Final job run state (e.g., 'SUCCEEDED', 'FAILED')

    Returns:
        dict: {status_code: 200, message: ...} on success
              {status_code: 500, message: ...} on DynamoDB error
    """
    region   = os.environ.get('AWS_REGION', 'us-east-1')
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table    = dynamodb.Table('BRN-etl-orchestration')

    try:
        # Upsert current job run status into DynamoDB
        response = table.put_item(
            Item={
                'brn-etl-partitionKey': job_name,
                'brn-etl-sortKey':      order_date,
                'status':               status
            }
        )
        LOGGER.info("Inserted current entry into DynamoDB: %s", response)

        # Cleanup: check previous day's entry and delete if already SUCCEEDED
        previous_order_date = get_previous_order_date(order_date)
        previous_status     = get_dynamodb_status(job_name, previous_order_date)
        LOGGER.info(
            "Previous status for %s on %s: %s",
            job_name, previous_order_date, previous_status
        )

        if previous_status == "SUCCEEDED":
            # Safe to delete — SUCCEEDED records are no longer needed for retry
            table.delete_item(
                Key={
                    'brn-etl-partitionKey': job_name,
                    'brn-etl-sortKey':      previous_order_date
                }
            )
            LOGGER.info("Deleted previous entry for %s on %s", job_name, previous_order_date)

        return {
            "status_code": 200,
            "message": f"DynamoDB updated for job {job_name} with status {status} on {order_date}"
        }

    except ClientError as e:
        LOGGER.error("Error updating DynamoDB: %s", e.response['Error']['Message'])
        return {"status_code": 500, "message": "Error updating DynamoDB"}


################################################################################
# HELPER: Compute the previous processing date (YYMMDD - 1 calendar day)
################################################################################

def get_previous_order_date(order_date):
    """
    Returns the YYMMDD date string for the day before the given order_date.

    Used by update_dynamodb_table to identify the previous day's DynamoDB entry
    for cleanup (deleting SUCCEEDED records to keep the table compact).

    Args:
        order_date : 6-digit date string in YYMMDD format (e.g., '250115')

    Returns:
        str: Previous date in YYMMDD format (e.g., '250114')

    Note:
        Uses datetime arithmetic — correctly handles month/year boundaries.
    """
    from datetime import datetime, timedelta
    return (
        datetime.strptime(order_date, "%y%m%d") - timedelta(days=1)
    ).strftime("%y%m%d")


################################################################################
# HELPER: Read a single job's status from DynamoDB
################################################################################

def get_dynamodb_status(job_name, order_date):
    """
    Reads and returns the 'status' attribute for a specific job + date combination.

    Used by:
      - update_dynamodb_table: to check if the previous day's run SUCCEEDED
        (and can therefore be cleaned up)

    Args:
        job_name   : Glue job name (DynamoDB partition key)
        order_date : 6-digit date YYMMDD (DynamoDB sort key)

    Returns:
        str: Status value (e.g., 'SUCCEEDED', 'FAILED'), or None if not found / error.
    """
    region   = os.environ.get('AWS_REGION', 'us-east-1')
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table    = dynamodb.Table('BRN-etl-orchestration')

    try:
        response = table.get_item(
            Key={
                'brn-etl-partitionKey': job_name,
                'brn-etl-sortKey':      order_date
            }
        )
        # Returns None if the item doesn't exist (no previous run record)
        return response.get('Item', {}).get('status', None)

    except ClientError as e:
        LOGGER.error("Error reading DynamoDB: %s", e.response['Error']['Message'])
        return None


################################################################################
# MAIN ENTRYPOINT: Lambda handler — routes to one of four processing modes
################################################################################

def lambda_handler(event, context):
    """
    AWS Lambda entry point for the BRN Glue ETL Orchestrator.

    Routes to one of four processing modes based on event structure:

    ┌───────────────────────────────────────────────────────────────────────────┐
    │ MODE 1 — SQS/S3 Event                                                    │
    │   Trigger : EventBridge → SQS → Lambda (new file in S3)                 │
    │   Input   : event["Records"][0]["eventSource"] == "aws:sqs"              │
    │   Action  : Parse key → resolve files → validate timestamps → trigger SF │
    ├───────────────────────────────────────────────────────────────────────────┤
    │ MODE 2 — File Readiness Check (called by Step Functions polling loop)    │
    │   Trigger : Step Functions Lambda invoke (direct)                        │
    │   Input   : event has "order_date" + "required_files" (no "status")     │
    │   Action  : check_files_in_s3 → return 200 or 400                       │
    ├───────────────────────────────────────────────────────────────────────────┤
    │ MODE 3 — DynamoDB Status Update (called by Step Functions post-job)      │
    │   Trigger : Step Functions Lambda invoke (direct)                        │
    │   Input   : event has "order_date" + "job_name" + "status"              │
    │   Action  : update_dynamodb_table → persist final job state              │
    ├───────────────────────────────────────────────────────────────────────────┤
    │ MODE 4 — Glue-Retry (manual re-trigger of all FAILED jobs)              │
    │   Trigger : Manual invoke or scheduled EventBridge rule                  │
    │   Input   : event["action"] == "start" and                              │
    │             event["metadata"]["flag"] == "Glue-Retry"                   │
    │   Action  : Scan DynamoDB for FAILED → re-trigger Step Functions        │
    └───────────────────────────────────────────────────────────────────────────┘
    """
    LOGGER.info("Received event: %s", json.dumps(event))
    bucket_name = os.environ['TriggerName']   # S3 bucket for BRN_IN files
    LOGGER.info("Bucket name: %s", bucket_name)
    LOGGER.info("context: %s", context)

    # ──────────────────────────────────────────────────────────────────────────
    # MODE 1: SQS-wrapped S3 PutObject event
    # ──────────────────────────────────────────────────────────────────────────
    if "Records" in event and event["Records"][0]["eventSource"] == "aws:sqs":
        # Extract the nested S3 event details from the SQS message body
        # EventBridge wraps the S3 notification inside the SQS message body as JSON
        event_body     = json.loads(event["Records"][0]["body"])
        request_params = event_body["detail"].get("requestParameters", {})

        if "key" in request_params:
            object_key  = request_params["key"]
            bucket_name = request_params["bucketName"]  # Override env var with actual bucket
            LOGGER.info("Bucket: %s", bucket_name)
            LOGGER.info("Key: %s", object_key)

        # --- Resolve Step Function ARN based on deployment environment ---
        # All three environments use the same OutputArn env var — the variable
        # value is set differently per environment at deploy time
        # TODO: Consider using a single check (env var is already env-specific)
        if os.environ['SysLevel'] == "eng":
            arn = os.environ['OutputArn']
        elif os.environ['SysLevel'] == "sat":
            arn = os.environ['OutputArn']
        elif os.environ['SysLevel'] == "prd":
            arn = os.environ['OutputArn']
        else:
            return {"status_code": 400, "message": "Invalid environment"}

        # --- Sub-branch A: BRN_IN_BUSINESS files (fixed-name external files) ---
        # These files don't follow the YYMMDD naming convention — they are direct
        # Glue job triggers with no dependency checking or Step Function orchestration
        if object_key.startswith("BRN_IN_BUSINESS/"):
            LOGGER.info("Processing file in BRN_IN_BUSINESS prefix")
            incoming_file = object_key.split('/')[-1]   # e.g., 'MCR.txt'
            LOGGER.info("Incoming File: %s", incoming_file)

            LOGGER.info("inside the external process...")

            # Map fixed business filenames to their Glue job names
            if incoming_file == "MCR.txt":
                glue_job_name = "BRNETL-UDB4B"
                LOGGER.info("MCR.txt file detected. Triggering Glue job...")
            elif incoming_file == "QIK.PRN":
                glue_job_name = "BRNETL-UDB3B"
                LOGGER.info("QIK.PRN file detected. Triggering Glue job...")
            elif incoming_file == "DB_TRUSTEE_CHAR_NAV.txt":
                # Note: source PDF showed "DR TRUSTEE CHAR NAV.txt" on line 348 —
                # corrected to "DB_TRUSTEE_CHAR_NAV.txt" per line 349 (consistent name)
                glue_job_name = "BRNETL-UD67B"
            else:
                # Unrecognized BRN_IN_BUSINESS file — no job required
                LOGGER.info("No Glue job required for file: %s", incoming_file)
                glue_job_name = None

            # Fire the Glue job directly (no Step Function orchestration for BUSINESS files)
            glue_client = boto3.client('glue')
            try:
                response = glue_client.start_job_run(JobName=glue_job_name)
                LOGGER.info("Glue job triggered successfully: %s", response)
                return {"status_code": 200, "message": "Glue job triggered successfully"}
            except Exception as e:
                LOGGER.error("Error triggering Glue job: %s", str(e))
                return {"status_code": 500, "message": "Error triggering Glue job"}

        # --- Sub-branch B: Standard BRN_IN file path ---
        # Parse the S3 key → extract file identifier + order date
        incoming_file, order_date = file_info(object_key)
        if not incoming_file:
            return {"status_code": 400, "message": "Invalid file format"}

        LOGGER.info("Incoming File: %s", incoming_file)

        # Look up required companion files and Glue job name from config
        required_files, job_name = get_required_files(incoming_file)
        logging.info("required_files: %s", required_files)

        if job_name is None:
            # File arrived but has no corresponding job mapping — skip silently
            return {"status_code": 400, "message": "job_name not found in mapping"}

        # ── Case 1: No companion files needed (standalone job) ────────────────
        # Trigger Step Function immediately with empty_required_files = "true"
        # Step Functions will skip the polling loop and run the Glue job directly
        if not required_files:
            logging.info("No required files found for %s", incoming_file)
            input_dict = {
                "file_name":            incoming_file,
                "order_date":           order_date,
                "required_files":       required_files,
                "job_name":             job_name,
                # Signal to Step Functions: skip dependency polling
                "empty_required_files": "true"
            }
            logging.info("Triggering Step Function for Standalone job: %s", job_name)
            response = stepfunction(arn, input_dict)
            logging.info("response: %s", response)
            return {"status_code": 200, "message": "Step Function triggered successfully"}

        logging.info("Required Files: %s", required_files)
        logging.info("Job Name: %s", job_name)

        # Get the trigger file's own LastModified timestamp for comparison
        # (companion files must be >= this timestamp to be considered "same day's files")
        file_timestamp = get_last_modified_timestamps(bucket_name, object_key)
        logging.info("file_timestamp: %s", file_timestamp)
        if file_timestamp is None:
            return {"status_code": 400, "message": "File not found in S3 bucket"}

        # ── Case 2: Single dependency = 'common_files' ────────────────────────
        # 'common_files' is resolved at check time (in check_files_in_s3) to:
        #   ['CLIENTID.DATA.RFM', 'POID.DATA.RFM', 'PageNumber']
        # No timestamp comparison needed — trigger Step Function to handle the wait
        if len(required_files) == 1 and required_files[0] == 'common_files':
            input_dict = {
                "file_name":            incoming_file,
                "order_date":           order_date,
                "required_files":       required_files,
                "job_name":             job_name,
                # Step Functions will enter the polling loop for common_files
                "empty_required_files": "false"
            }
            response = stepfunction(arn, input_dict)
            logging.info("response: %s", response)
            return {"status_code": 200, "message": "Step function triggered with common files"}

        # ── Case 3: Single non-common dependency ──────────────────────────────
        if len(required_files) == 1 and required_files[0] != 'common_files':
            last_modified_timestamps = get_required_files_timestamp(
                bucket_name, required_files, order_date
            )

            if not last_modified_timestamps:
                # Companion file not yet in S3 — trigger Step Function to poll
                input_dict = {
                    "file_name":            incoming_file,
                    "order_date":           order_date,
                    "required_files":       required_files,
                    "job_name":             job_name,
                    "empty_required_files": "false"
                }
                response = stepfunction(arn, input_dict)
            else:
                for req_file, req_timestamp in last_modified_timestamps.items():
                    if req_timestamp < file_timestamp:
                        # Companion file is OLDER than trigger → stale file, do not proceed
                        return {
                            "status_code": 400,
                            "message": f"File {req_file} has a later timestamp"
                        }
                    elif req_timestamp == file_timestamp:
                        # Same timestamp edge case — certain files require strict ordering
                        # These files must NOT share a timestamp (sequencing dependency)
                        if incoming_file in (
                            "NSCC.ASOF.TLM.MATCH",
                            "RECJD193.FUND.REMAP",
                            "TLMPGNUM.DATEB.RFM",
                            "TLM.VANG.DETS.OUT.BKUP"
                        ):
                            return {
                                "status_code": 400,
                                "message": f"File {req_file} has same timestamp"
                            }
                        # For all other files, same timestamp is acceptable — proceed
                        input_dict = {
                            "file_name":            incoming_file,
                            "order_date":           order_date,
                            "required_files":       required_files,
                            "job_name":             job_name,
                            "empty_required_files": "false"
                        }
                        response = stepfunction(arn, input_dict)

        # ── Case 4: Multi-file dependency including 'common_files' ────────────
        # Temporarily remove 'common_files' for timestamp checking (can't timestamp-check
        # a virtual token), then re-add before passing to Step Functions
        if 'common_files' in required_files:
            required_files.remove('common_files')

            last_modified_timestamps = get_required_files_timestamp(
                bucket_name, required_files, order_date
            )

            # Re-add 'common_files' so Step Functions knows to check them too
            required_files.append('common_files')

            if not last_modified_timestamps:
                # Real companion files not yet present — trigger polling
                input_dict = {
                    "file_name":            incoming_file,
                    "order_date":           order_date,
                    "required_files":       required_files,
                    "job_name":             job_name,
                    "empty_required_files": "false"
                }
                response = stepfunction(arn, input_dict)
            else:
                logging.info(
                    "inside else last_modified_timestamps:%s",
                    last_modified_timestamps
                )
                for req_file, req_timestamp in last_modified_timestamps.items():
                    if req_timestamp < file_timestamp:
                        # Stale companion file detected — abort
                        return {
                            "status_code": 400,
                            "message": f"File {req_file} has a later timestamp"
                        }
                    elif req_timestamp == file_timestamp:
                        # Same-timestamp guard for sequencing-sensitive files
                        if incoming_file in (
                            "NSCC.ASOF.TLM.MATCH",
                            "RECJD193.FUND.REMAP",
                            "TLMPGNUM.DATEB.RFM",
                            "TLM.VANG.DETS.OUT.BKUP"
                        ):
                            return {
                                "status_code": 400,
                                "message": f"File {req_file} has same timestamp"
                            }
                        # Proceed for non-sensitive files with matching timestamps
                        input_dict = {
                            "file_name":            incoming_file,
                            "order_date":           order_date,
                            "required_files":       required_files,
                            "job_name":             job_name,
                            "empty_required_files": "false"
                        }
                        response = stepfunction(arn, input_dict)
                    else:
                        # req_timestamp > file_timestamp → companion is newer → invalid state
                        return {"status_code": 400, "message": "Invalid file format"}
                else:
                    # for-else: loop completed without break — no valid case matched
                    return {"status_code": 400, "message": "Invalid event format"}

    # ──────────────────────────────────────────────────────────────────────────
    # MODE 2: File Readiness Check (called by Step Functions polling loop)
    # Event shape: { "order_date": "250115", "required_files": [...], ... }
    # ──────────────────────────────────────────────────────────────────────────
    elif "order_date" in event and "required_files" in event:
        input_dict     = event
        order_date     = event["order_date"]
        required_files = event["required_files"]

        # Resolve 'common_files' token → actual reference file names
        # This substitution only happens at check time (not at trigger time)
        if 'common_files' in required_files:
            required_files.remove('common_files')
            required_files.extend(['CLIENTID.DATA.RFM', 'POID.DATA.RFM', 'PageNumber'])

        if not order_date or not required_files:
            return {
                "status_code": 400,
                "message": "order_date and required_files are required"
            }

        # Delegate to file presence checker — returns 200 or 400
        return check_files_in_s3(bucket_name, order_date, required_files, input_dict)

    # ──────────────────────────────────────────────────────────────────────────
    # MODE 3: DynamoDB Status Update (called by Step Functions post-Glue-job)
    # Event shape: { "order_date": "...", "job_name": "...", "status": "SUCCEEDED" }
    # ──────────────────────────────────────────────────────────────────────────
    elif "order_date" in event and "job_name" in event and "status" in event:
        order_date = event["order_date"]
        job_name   = event["job_name"]
        status     = event["status"]

        update_response = update_dynamodb_table(order_date, job_name, status)
        LOGGER.info("DynamoDB update response: %s", update_response)
        return {
            "status_code": 200,
            "message": (
                f"DynamoDB updated for job {job_name} "
                f"with status {status} on {order_date}"
            )
        }

    # ──────────────────────────────────────────────────────────────────────────
    # MODE 4: Glue-Retry — re-trigger all FAILED jobs from DynamoDB
    # Event shape: { "action": "start", "metadata": { "flag": "Glue-Retry" } }
    # ──────────────────────────────────────────────────────────────────────────
    elif (
        event.get("action") == "start"
        and event.get("metadata", {}).get("flag") == "Glue-Retry"
    ):
        region   = os.environ.get('AWS_REGION', 'us-east-1')
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table    = dynamodb.Table('BRN-etl-orchestration')

        try:
            # Scan DynamoDB for all items where status = 'FAILED'
            # NOTE: Table scan is acceptable here — BRN-etl-orchestration is small
            # (only current + recent failure records are retained)
            response = table.scan(
                FilterExpression='#st = :val',
                # 'status' is a reserved word in DynamoDB — must use ExpressionAttributeNames
                ExpressionAttributeNames={'#st': 'status'},
                ExpressionAttributeValues={':val': 'FAILED'}
            )

            for item in response.get('Items', []):
                job_name   = item.get('brn-etl-partitionKey')
                order_date = item.get('brn-etl-sortKey')
                print(f"Failed Job - Name: {job_name}, Order Date: {order_date}")

                # Reverse-lookup the file_mapping to reconstruct the input_dict
                # for each failed job (need file_name + required_files to re-trigger)
                for file_group, mapped_job_name in file_mapping.items():
                    if mapped_job_name == job_name:
                        # First element of the tuple = the trigger file
                        incoming_file  = file_group[0]
                        # Remaining elements = companion files (empty list if standalone)
                        required_files = list(file_group[1:]) if len(file_group) > 1 else []

                        input_dict = {
                            "file_name":      incoming_file,
                            "order_date":     order_date,
                            "required_files": required_files,
                            "job_name":       job_name,
                            # No dependencies if required_files is empty
                            "empty_required_files": "true" if not required_files else "false"
                        }
                        print(f"Constructed input_dict: {input_dict}")
                        # Re-trigger Step Function for this failed job
                        response = stepfunction(os.environ['OutputArn'], input_dict)
                        print(f"Step Function response: {response}")

        except Exception as e:
            print(f"Error scanning DynamoDB or triggering Step Function: {str(e)}")

    # ──────────────────────────────────────────────────────────────────────────
    # Fallback: unrecognised event shape
    # ──────────────────────────────────────────────────────────────────────────
    else:
        return {"status_code": 400, "message": "Invalid event format"}