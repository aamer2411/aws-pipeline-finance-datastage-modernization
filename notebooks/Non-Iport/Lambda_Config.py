################################################################################
# config.py
#
# Purpose:
#   Centralized file-to-Glue-job mapping for the BRN ETL orchestration pipeline.
#   This module is imported by app.py (Lambda handler) to determine which
#   AWS Glue job to trigger when a specific BRN_IN source file arrives in S3.
#
# Mapping Key:
#   A tuple of file identifiers — the FIRST element is the "trigger" file
#   (the file whose arrival initiates the pipeline).
#   Subsequent elements are "required companion files" that must also be
#   present in S3 before the Glue job can be safely triggered.
#   The special token 'common_files' is a placeholder meaning the job
#   requires the standard set of common reference files:
#       ['CLIENTID.DATA.RFM', 'POID.DATA.RFM', 'PageNumber']
#   (resolved at runtime in check_files_in_s3)
#
# Mapping Value:
#   The AWS Glue job name (BRNETL-UDXXX format) to execute.
#
# Examples:
#   ('INSBN200.BALANCE', 'common_files') → 'BRNETL-UD027'
#       Trigger file   : INSBN200.BALANCE
#       Required also  : common reference files
#       Glue job       : BRNETL-UD027
#
#   ('NSCC.ASOF.TLM.MATCH', 'NSCC.SETL.TLM.MATCH', 'common_files') → 'BRNETL-UD013'
#       Trigger file   : NSCC.ASOF.TLM.MATCH
#       Required also  : NSCC.SETL.TLM.MATCH + common reference files
#       Glue job       : BRNETL-UD013
#
# Adding new jobs:
#   Add a new key-value pair. Key = tuple of (trigger_file, *required_files).
#   If no dependencies: key = (trigger_file, 'common_files') for common refs only,
#   or (trigger_file,) with no extras for a fully standalone job.
################################################################################

file_mapping = {
    # -------------------------------------------------------------------
    # Single-trigger files that only require common reference files
    # -------------------------------------------------------------------
    ('INSBN200.BALANCE',          'common_files'): 'BRNETL-UD027',
    ('NET.EFFECT',                'common_files'): 'BRNETL-UD008',
    ('OMNI.CUST.INSBN008.ERRFILE','common_files'): 'BRNETL-UD010',
    ('NSCC.XPECT.SETTLE.RECON',   'common_files'): 'BRNETL-UD009',
    ('INSBN200.MONYMVMT',         'common_files'): 'BRNETL-UD016',
    ('CONFIRM.EXTSHR',            'common_files'): 'BRNETL-UD026',
    ('NSCC.DIVIDEND.SRT.BKUP',    'common_files'): 'BRNETL-UD028',
    ('NSCC.CONFIRM.RPT.SRT.BKUP', 'common_files'): 'BRNETL-UD030',
    ('OSF.EXSHRDIV',              'common_files'): 'BRNETL-UD033',
    ('NSCC.ACTV.RPT2.SRT.BKUP',   'common_files'): 'BRNETL-UD031',
    ('OWNI.VSTB2314.DCCONACH',    'common_files'): 'BRNETL-UD038',
    ('RECON.BKOUT.FEED',          'common_files'): 'BRNETL-UD039',
    ('RECON.MICRO.FEED',          'common_files'): 'BRNETL-UD041',
    ('ID.INSBY132.REVERSAL.RF',   'common_files'): 'BRNETL-UD042',

    # -------------------------------------------------------------------
    # Multi-trigger files: ALL listed files must arrive before job runs
    # -------------------------------------------------------------------

    # VISTA share balance + IBRPORT sort backup — both needed before UD068 runs
    ('VISTA.SHAREBAL.IBRBKP', 'IBRPORT.SRTBLBKP', 'common_files'): 'BRNETL-UD068',

    # OSF dividend/trade group — any one of these triggers UD017
    # (remaining files in the group become the "required companion" set)
    ('OSF.MERGEDIV',       'common_files'): 'BRNETL-UD017',
    ('OSF.PENDIV',         'common_files'): 'BRNETL-UD017',
    ('OSF.VGI.SHARE1',     'common_files'): 'BRNETL-UD017',
    ('OSF.TRADE.DATE.DIS', 'common_files'): 'BRNETL-UD017',

    # TLM match files — BOTH ASOF and SETL match files required before UD013
    ('NSCC.ASOF.TLM.MATCH', 'NSCC.SETL.TLM.MATCH', 'common_files'): 'BRNETL-UD013'
}