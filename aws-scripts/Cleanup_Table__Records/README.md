# Data Lake Cleanup Automation (SRE Utility)

## Overview

This Python automation utility performs controlled cleanup of Data Lake resources across AWS services.

It removes metadata, storage objects, and pipeline configurations associated with specific tables provided via CSV input.

The script supports multiple ingestion workflows:

- v1
- v2
- europa

All deletions require manual confirmation for safety.

---

## AWS Services Used

- DynamoDB  
- S3  
- Glue  
- SSM Parameter Store  
- SQS  
- CloudWatch Logs  

---

## Input File

The script reads a CSV file:

## Format:

| DatabaseName | TableName |
|-------------|-----------|
| db_name     | table_name |


## Execution Flow

CSV Input
   ↓
Detect Workflow Type
   ↓
DynamoDB Cleanup
   ↓
S3 Cleanup
   ↓
Glue Cleanup
   ↓
Pipeline Deletion
   ↓
ISR Trigger (SQS)

