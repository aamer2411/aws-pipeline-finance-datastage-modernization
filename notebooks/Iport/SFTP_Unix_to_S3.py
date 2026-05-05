################################################################################
# Table       : SFTP From Unix to AWS
# Description : SFTP From Unix to AWS
# Changes     : Jira #
################################################################################

import sys
import os
import subprocess  # Currently unused; retained to match original
import traceback

import paramiko
import boto3       # Currently unused directly; retained as in original
from awsglue.utils import getResolvedOptions

import glue_utils  # Local utility module available in the Glue job environment


def main(env, filepattern, remote_directory_path, sourcebucket):
    """
    Connects to an SFTP server, filters specific files by exact match against
    the provided file pattern, downloads them to a temporary directory in the
    Glue job environment, and triggers an S3 upload via glue_utils.upload_file_to_s3.

    Parameters:
        env (str)                   : Environment identifier (e.g., dev, qa, prod).
        filepattern (str | list[str]): A single filename or list of exact filenames to fetch.
        remote_directory_path (str) : Remote SFTP directory path to list and pull files from.
        sourcebucket (str)          : Target S3 bucket prefix/path passed to glue_utils.
    """
    try:
        # Normalize filepattern to a list if a single string is passed
        if isinstance(filepattern, str):
            filepattern = [filepattern]

        print(f"Remote directory path : {remote_directory_path}")

        # Retrieve SFTP configuration (hostname, port, secret ARN) from utilities
        hostname, port, secret_arn = glue_utils.get_config_SFTP(env)

        # Retrieve credentials (expects keys 'u' for username and 'p' for password)
        credentials = glue_utils.get_secret(secret_arn)
        username = credentials.get('u', '')
        password = credentials.get('p', '')

        # Initialize SSH client and connect to SFTP host
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, port, username, password)

        # Open SFTP session
        sftp = ssh.open_sftp()

        # List files in the remote directory
        remote_files = sftp.listdir(remote_directory_path)
        print("Remote files:", remote_files)

        # Collect files that exactly match any of the patterns (case-sensitive, stripped)
        matched_files = []
        for file in remote_files:
            for pattern in filepattern:
                if file.strip() == pattern.strip():
                    matched_files.append(file)

        # Prepare local temporary directory for staging downloads
        temp_dir = '/tmp/glue_transfer'
        os.makedirs(temp_dir, exist_ok=True)

        print("Listed Items:", matched_files)

        # Download each matched file to the temporary directory
        for file in matched_files:
            source_path = f"{remote_directory_path}/{file}"
            temp_file_path = f"{temp_dir}/{file}"
            print(f"Transferring {file}...")
            sftp.get(source_path, temp_file_path)

        # Delegate upload to S3 (implementation inside glue_utils)
        glue_utils.upload_file_to_s3(env, sourcebucket)

    except Exception as e:
        # Log full error details and exit with non-zero status to signal Glue job failure
        print(f"Error occurred: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # Retrieve environment (e.g., 'dev', 'prd', etc.)
    args = getResolvedOptions(sys.argv, ['env'])
    env = args['env']

    # ── First SFTP pull (M_13 monthly job) ──────────────────────────────────
    filepath = '/data/ins/prd/dstage/IPT/IADM/datain'

    args = getResolvedOptions(sys.argv, ['filepattern'])
    filepattern = args['filepattern']

    args = getResolvedOptions(sys.argv, ['sourcebucket'])
    bucket = args['sourcebucket']
    sourcebucket = f"{bucket}M13/"

    main(env, filepattern, filepath, sourcebucket)

    # ── Second one-time load (TCOMM_SEG Job — File 1-time Load) ─────────────
    filepath1 = '/data/ins/prd/dstage/IPT/IADM2/dataout'
    filepattern1 = ['ILY_PSRA_TRNX_IN_AUG_RUN.csv']

    main(env, filepattern1, filepath1, sourcebucket)