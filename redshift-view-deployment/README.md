# EC2-Based Redshift View Deployment

This document describes the secure and least-privilege setup for deploying Redshift views using an EC2 instance.

---

## Architecture Overview

Deployment Flow:

1. Developer SSH into EC2
2. EC2 retrieves database credentials (from Secrets Manager)
3. EC2 connects to Redshift
4. Views are created or replaced

Target service:
- Amazon Redshift

---

## IAM Role – Least Privilege Policy

Attach the following IAM policy to the EC2 instance role.

This policy allows:
- Reading create records in dynamodb
- Reading database credentials from Secrets Manager
- (Optional) IAM authentication to Redshift

### Production-Ready IAM Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [

    {
      "Sid": "AllowReadSQLFromS3",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::your-sql-bucket/path/*"
    },

    {
      "Sid": "AllowReadDBSecret",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:region:account-id:secret:your-redshift-secret-*"
    },

    {
      "Sid": "AllowRedshiftIAMAuth",
      "Effect": "Allow",
      "Action": [
        "redshift:GetClusterCredentials"
      ],
      "Resource": "*"
    }

  ]
}





## Deployment Steps (High Level)

1. SSH into EC2
2. Retrieve secret
3. Connect to Redshift
4.Create view
5. Run:

```sql
CREATE OR REPLACE VIEW schema.view_name AS
SELECT ...
```

Author: Deployment Engineering
