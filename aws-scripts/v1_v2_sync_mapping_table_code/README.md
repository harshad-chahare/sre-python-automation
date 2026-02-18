# V1 → V2 Sync Mapping Validation & Snapshot Automation

##  Problem Statement

The DataLake team owns V1 (Airflow) tables.

When access is requested for V1 tables:
- DataLake receives multiple notifications
- Operational noise increases
- Many V1 tables already have V2 (Etleap) equivalents

Goal:
Automate validation and maintain a V1 → V2 mapping dataset, detect changes, and generate incremental/deletion batches automatically.

---

# Architecture Overview

This solution performs:

1. Fetch mapping data from DynamoDB
2. Validate Glue table existence
3. Generate validated V1-V2 mapping snapshot
4. Store snapshot CSV in S3
5. Compare latest 2 snapshots
6. Generate:
   - Incremental batch (new mappings)
   - Deletion batch (removed mappings)
7. Generate manifest files
8. Send notification if deletions detected

---


##  Glue Catalog Validation

Before accepting a mapping:

The system validates:
- V2 database exists
- V2 table exists in Glue

If Glue table does NOT exist:
- Mapping is ignored

This ensures:
- No stale mappings
- No broken references
- Only valid resources stored

---

# Transformation Logic

IDs are transformed using:

```
destination_id.split('|')
id.split('|')
```

Transformed format:

```
schema__env.table_name
```

Example:

platform|equity__stg|table_name  
→ platform__equity__stg.table_name

---

# Snapshot Generation

After validation:

Valid mappings are stored as CSV:

```
source_pipeline_resource_name
destination_pipeline_resource_name
```

Uploaded to:

```
s3://{datalk_bucket}-{env}/v1_v2_mapping_validation/{timestamp}/glue_tables_{timestamp}.csv
```

---

#  Snapshot Comparison Engine

The system compares:

- Latest CSV
- Previous CSV

From:

```
v1_v2_mapping_validation/
```

---

# Incremental Handling

If NEW mappings detected:

- incremental.csv generated
- Uploaded to drop bucket
- Manifest file created

Path:

```
{schema}/datasets/{dataset_value}/{database}__{env}/pipeline_migration_table_mapping/{snapshot_id}/incremental_{uuid}/
```

Manifest:

```
.manifest.json
```

---

#  Deletion Handling

If mappings REMOVED:

- deletion.csv generated
- Uploaded under:

```
__deleted__/deletion.csv
```

- Deletion manifest generated
- Email notification sent with HTML table of deleted records

This ensures:
- Downstream systems can process deletes
- Visibility to DataLake Ops

---

# Alerting Strategy

## STG
- SES email if Dynamo table empty

## PRD
- SNS critical alarm if Dynamo table empty

Deletion events:
- SES email with formatted HTML summary

---

# Snapshot ID Usage

Snapshot ID fetched from:

DynamoDB:
```
etleap-dl-{env}-{dynamodb_table_name}
```

Used to build dataset S3 path.

Ensures:
- Versioned dataset tracking
- Snapshot lineage control

---

# IAM Permissions Required

Minimum required permissions:

- dynamodb:Scan
- dynamodb:GetItem
- glue:GetTable
- s3:PutObject
- s3:GetObject
- s3:ListBucket
- ses:SendEmail
- sns:Publish (PRD only)

No infrastructure modification permissions required.

---

# Business Impact

✅ Eliminates unnecessary access notifications  
✅ Ensures V1-V2 migration visibility  
✅ Prevents granting access to deprecated V1 tables  
✅ Automates dataset sync  
✅ Maintains snapshot lineage  
✅ Supports incremental & delete processing  
✅ Production-grade validation before publish  

---

# Execution Flow

Lambda Handler:

```
handler → pipeline_migration_table_mapping()
```

High-Level Steps:

1. Fetch DynamoDB mappings
2. Validate Glue existence
3. Generate snapshot CSV
4. Upload to S3
5. Compare with previous snapshot
6. Generate incremental/deletion batches
7. Create manifest
8. Send alerts if required

---

# Conclusion

This solution ensures:

- Clean V1 → V2 migration tracking
- No stale mappings
- Automated snapshot comparison
- Incremental & deletion awareness
- Controlled alerting
- Reduced operational noise

It is production-ready, validation-driven, and governance-aligned.

---

Author: DataLake Engineering  
