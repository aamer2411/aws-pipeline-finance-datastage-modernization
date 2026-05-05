# AWS DataStage Modernization

Migration of IBM DataStage batch ETL pipelines to AWS for an Institutional Investor Group. Over 110 PySpark Glue jobs replace legacy DataStage sequences, with a fully event-driven orchestration layer built on S3, Lambda, Step Functions, and Redshift.

This is production delivery work completed as part of a client engagement — scripts have been sanitized of credentials and internal identifiers before being shared here.

---

## Architecture Overview

### Current State vs Future State

![Architecture: Current vs Future State](screenshots/architecture_current_vs_future_state.png)

| | Current State | Future State |
|---|---|---|
| ETL Engine | IBM DataStage (on-premises) | AWS Glue (PySpark) |
| File Transfer | Control-M SFTP to Windows File Share | AWS DataSync to S3 |
| Orchestration | Control-M job scheduler | Step Functions + Lambda + EventBridge |
| Data Warehouse | Redshift (DataStage-loaded) | Redshift (Glue-loaded) |
| Downstream DB | Oracle 10 | Oracle 20 (Oracle 10 decommissioned post-cutover) |

---

## Application Scope

DataStage supports two major batch applications for Institutional Plan Sponsor reporting:

| | Non-IPORT | IPORT |
|---|---|---|
| Purpose | Financial data and money movement | Business reporting tool |
| Sign-off requirement | 100% data match | 95% data match |
| Sub-projects | BRD/BRN, CST/CTQ, OMG/PGX | Yearly, Quarterly, Monthly, Daily |
| Glue jobs | ~110 | 7 builds |
| Daily data volume | 200-250 GB | Up to 33M records per cycle |

---

## Non-IPORT Pipeline

### Architecture

![Non-IPORT Architecture](screenshots/non_iport_architecture_overview.png)

The Non-IPORT pipeline handles three sub-projects: Bank Reconciliation (BRD/BRN), Compliance Testing Service (CST/CTQ), and Pageflex ETL (OMG/PGX).

### Data Flow

```
Mainframe (fixed-width flat files)
        |
        v
Windows File Share  ──(Control-M SFTP)──>  [WFS]
        |
AWS DataSync (every 5 min)
        |
        v
Amazon S3 (BRN_IN)  ──── Glacier after 30d, deleted after 90d
        |
EventBridge (ObjectCreated)
        |
        v
SQS Queue
        |
        v
Lambda (brn-glue-job-orchestrator)    ← Parses filename, resolves job + dependencies
        |
        v
Step Functions State Machine          ← Orchestrates dependency wait + Glue execution
        |
     +--+--+
     |     |
     |   Wait loop (5min → 10min → SNS alert → loop until 14h timeout)
     |     |
     +--+--+
        |
        v
AWS Glue ETL Job (PySpark)
        |
        v
Amazon S3 (TLM_OUT)                   ← Output consumed by downstream TLM system
        |
DynamoDB (job status audit)
        |
SNS (failure alerts + PagerDuty)
```

### Lambda Orchestration

![Lambda SQS Trigger](screenshots/lambda_sqs_trigger_configuration.png)

The Lambda function (`brn-glue-job-orchestrator`) handles four invocation modes:

| Mode | Trigger | Action |
|------|---------|--------|
| SQS/S3 Event | New file lands in S3 | Parse key, resolve companions, start Step Function |
| File Readiness Check | Step Functions polling | Check if dependency files are present (200/400) |
| Status Update | Step Functions post-job | Upsert job run status in DynamoDB |
| Glue-Retry | Manual/scheduled invoke | Scan DynamoDB for FAILED jobs, re-trigger Step Functions |

### Step Functions Orchestration

![Step Functions State Machine](screenshots/step_functions_state_machine.png)

The state machine handles standalone and dependent jobs differently:
- **Standalone jobs:** Directly invoke the Glue job with no dependency wait.
- **Dependent jobs:** Poll every 5 minutes (then every 10 minutes) for companion files. After 15 minutes of waiting, an SNS alert fires. The loop continues until files arrive or the execution times out at ~14 hours.

### Medallion Architecture

| Layer | S3 Bucket | Description |
|-------|-----------|-------------|
| Bronze (Raw) | `BRN_IN` | Exact replicas from mainframe via DataSync. Source of Truth. |
| Silver (Cleansed) | `BRN_INT` | Cleansed and standardized for downstream transforms |
| Gold (Curated) | `TLM_OUT` | Final ETL output consumed by TLM downstream applications |

### Glue Job Configuration

| Setting | Value |
|---------|-------|
| Glue Version | 4.0 (Python 3.10, Spark 3.3) |
| Worker Type | G.4x (16 vCPU, 64 GB RAM) |
| Max Workers | 2 per job |
| Max Capacity | 8 DPUs |
| Timeout | 300 min |

---

## IPORT Pipeline

### Architecture

![IPORT Architecture](screenshots/iport_architecture_overview.png)

IPORT handles business reporting across Yearly, Quarterly, Monthly, and Daily cycles. Data volumes range from 5K to 500M records per table.

### Data Sources

| Source | Ingestion Method |
|--------|-----------------|
| CSV files (on-prem UNIX server) | SFTP / AWS DataSync to S3, then Glue to Redshift staging |
| Redshift tables (previous builds) | Read directly in Glue |
| On-prem DB2 tables | JDBC read in Glue |

### Data Flow

```
Source Data (CSV / Redshift / DB2)
        |
        v
Glue ETL (SQL-based transformations on DataStage business rules)
        |
        v
Redshift Staging Tables
        |
        v
Redshift Target Tables
        |
        v
Oracle 20 (on-prem, final downstream DB)
```

### Data Validation (Oracle 20 vs Oracle 10)

```sql
-- Row count comparison
SELECT COUNT(*) FROM iportadm.table_name;
SELECT COUNT(*) FROM iportadm.table_name@<oracle-db-link>;

-- Difference check (records in Oracle 10 missing from Oracle 20)
SELECT * FROM iportadm.table_name@<oracle-db-link>
MINUS
SELECT * FROM iportadm.table_name;
```

---

## Key Engineering Challenges Solved

### 1. StackOverflow from Chained withColumn Operations
Spark's lazy evaluation caused `StackOverflowError` on jobs with 100+ chained `.withColumn()` calls. Using `.cache()` + `.count()` as an intermediate action broke the execution plan chain cleanly.

### 2. DataFrame Union Column Misalignment
Spark's `union()` matches by position, not by name. Two DataFrames with identical schemas but different column order produced silently incorrect output. Added an explicit `.select()` reorder before all union operations.

### 3. S3 Pagination for File Listing
`list_objects_v2()` returns at most 1,000 objects per call. Jobs started failing with "File not found" when a bucket exceeded that threshold. Implemented `ContinuationToken` pagination in all file-listing functions.

### 4. DB2 Read Performance (8x Improvement)
Loading a full DB2 table into Spark then filtering in memory pulled millions of unnecessary rows over the network. Replaced with JDBC query pushdown (subquery in the source parameter), limiting DB2 to return only the required rows. Result: 8x speed improvement.

### 5. Broadcast Join Optimization (5.5x Improvement)
A join between a 20M-row fact table and a 15 MB dimension table took 22 minutes using Spark's default Sort-Merge Join. Forcing an explicit `broadcast()` hint dropped runtime to 4 minutes.

---

## Technology Stack

| Component | Service |
|-----------|---------|
| ETL compute | AWS Glue 4.0 (PySpark 3.3) |
| File storage | Amazon S3 (Medallion: Bronze / Silver / Gold) |
| Event trigger | Amazon EventBridge + SQS |
| Orchestration | AWS Step Functions + AWS Lambda |
| Audit / status | Amazon DynamoDB |
| Alerting | Amazon SNS + PagerDuty |
| Data warehouse | Amazon Redshift Serverless |
| Source databases | Oracle 10/20 (on-prem), IBM DB2 (on-prem) |
| File sync | AWS DataSync |
| Secrets | AWS Secrets Manager |
| Monitoring | Amazon CloudWatch |
| Scheduling | Control-M (legacy jobs), AWS Glue Triggers |
| Language | Python 3.10, PySpark, SQL |

---

## Repository Structure

```
.
├── README.md
├── PROJECT_DETAILS.md               Full technical writeup
│
├── notebooks/
│   ├── README.md                    Script inventory and descriptions
│   ├── Non-Iport/
│   │   ├── Glue_Job_Normal.py       Full PySpark ETL job (fixed-width mainframe → TLM output)
│   │   ├── Glue_Job_DB2_to_S3.py    DB2 JDBC extract with query pushdown → S3
│   │   ├── Glue_Job_DB2_to_S3_Config.json  Glue job config for DB2 extract
│   │   ├── Glue_Job_Oracle_to_S3.py Oracle JDBC extract → S3
│   │   ├── Glue_Utils.py            Shared utility module (Spark init, S3 ops, output writers)
│   │   ├── Lambda_Main.py           Lambda orchestrator (4-mode event router)
│   │   ├── Lambda_Config.py         File-to-Glue-job mapping config
│   │   └── Stepfunction.json        Step Functions state machine definition (ASL)
│   │
│   └── Iport/
│       ├── S3_CSV_to_Redshift_Staging.py        SFTP CSV → S3 → Redshift staging
│       ├── Redshift_Glue_Utils.py               Shared utils for IPORT Glue jobs
│       ├── Redshift_Table_DDL.sql               DDL for Redshift staging + target tables
│       ├── Oracle_Redshift_Data_Validation.py   Validation script (Oracle 20 vs Oracle 10)
│       ├── SCD2_Implementation_Iport.py         SCD Type 2 MERGE logic (DB2 → Redshift → Oracle)
│       └── SFTP_Unix_to_S3.py                   UNIX server SFTP file pull → S3
│
└── screenshots/
    ├── architecture_current_vs_future_state.png
    ├── non_iport_architecture_overview.png
    ├── lambda_sqs_trigger_configuration.png
    ├── step_functions_state_machine.png
    ├── iport_architecture_overview.png
    ├── iport_dependent_job_execution_flow.png
    ├── glue_job_parameters_configuration.png
    └── jira_kanban_board.png
```

---

## Data Volumes

| Metric | Value |
|--------|-------|
| Files received per job per day | 1.5 to 2 GB |
| Reference files (ClientID, POID) | ~500 MB each |
| Total per job per day | ~2 to 2.5 GB |
| Total across all Glue jobs | 200 to 250 GB/day |
| IPORT M_13 initial load | ~33M records, 20 columns, ~24 GB CSV |

---

## Project Management

Work was organized in 3-sprint Builds (each sprint = 2 weeks). For example, Non-IPORT Build 1 delivered 34 Glue jobs across 3 sprints. Code only moves to Production after a complete Build is signed off.

| Tool | Purpose |
|------|---------|
| Confluence | Project documentation and runbooks |
| Jira | Task tracking with sprint boards |
| ServiceNow | Production change requests (RITM) |
| PagerDuty | On-call alerting for Glue failures |

---

## What This Demonstrates

- End-to-end ETL modernization from IBM DataStage to AWS Glue
- Event-driven pipeline orchestration with S3 triggers, EventBridge, SQS, Lambda, and Step Functions
- Production-grade PySpark ETL replicating mainframe fixed-width file processing logic
- JDBC integration with Oracle (on-prem) and DB2 databases, including query pushdown optimization
- Medallion architecture (Bronze/Silver/Gold) on Amazon S3
- SCD Type 2 implementation for IPORT reporting tables in Redshift
- Multi-environment deployment (ENG / SAT / Prod) with parameterized Glue jobs and Secrets Manager
- Redshift Serverless data warehousing with psycopg2 connection management
- Data validation framework comparing Oracle 20 vs Oracle 10 using MINUS queries and row counts
- Real-world Spark performance debugging: broadcast joins, DataFrame lineage breaks, S3 pagination fixes
