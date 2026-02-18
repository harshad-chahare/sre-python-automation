import boto3
import csv
import os
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from projects.functions.v1_v2_sync_mapping_table_code.csv_campare import main
import sys
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
glue = boto3.client('glue')
s3 = boto3.client('s3')
ses = boto3.client('ses')
sns = boto3.client('sns')

env = os.environ.get('STACK_ENV')

# Environment variables (required for deployment)
ALERT_EMAIL = os.environ.get("ALERT_EMAIL")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
S3_BUCKET_STG = os.environ.get("S3_BUCKET_STG")
S3_BUCKET_PRD = os.environ.get("S3_BUCKET_PRD")

S3_FOLDER = 'v1_v2_mapping_validation'
existing_tables = []


def ses_publish(subject, message):
    logger.info("SES notification triggered.")

    if not ALERT_EMAIL:
        raise ValueError("ALERT_EMAIL environment variable not set")

    response = ses.send_email(
        Destination={'ToAddresses': [ALERT_EMAIL]},
        Message={
            'Body': {'Html': {'Charset': 'UTF-8', 'Data': message}},
            'Subject': {'Charset': 'UTF-8', 'Data': subject},
        },
        Source=ALERT_EMAIL,
    )
    logger.info(response)


def fetch_items_from_dynamodb():
    logger.info("Fetching items from DynamoDB tables...")

    table1 = dynamodb.Table(os.environ.get("AIRFLOW_SYNC_TABLE"))
    table2 = dynamodb.Table(os.environ.get("MANUAL_MAPPING_TABLE"))

    try:
        response1 = table1.scan()
        response2 = table2.scan()

        return response1.get('Items', []), response2.get('Items', [])

    except ClientError as e:
        logger.warning(f"Unable to fetch items from DynamoDB: {e}")
        raise


def transform_destination_id(destination_id, id):
    try:
        parts1 = destination_id.split('|')
        parts2 = id.split('|')

        transformed_id = f"{parts1[1]}__{parts1[2]}.{parts1[3]}"
        v2_id = f"{parts2[1]}__{parts2[2]}.{parts2[3]}"

        return transformed_id, v2_id

    except IndexError as e:
        logger.warning(f"Invalid ID format: {destination_id}, {id}")
        raise


def check_glue_table_exists(database_name, table_name):
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False
        raise


def upload_to_s3(data):
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    csv_file_name = f"glue_tables_{timestamp}.csv"
    csv_file_path = f"/tmp/{csv_file_name}"

    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['source_pipeline_resource_name', 'destination_pipeline_resource_name'])
        for row in data:
            writer.writerow(row)

    s3_bucket = S3_BUCKET_STG if env == "stg" else S3_BUCKET_PRD
    if not s3_bucket:
        raise ValueError("S3 bucket environment variable not set")

    s3_key = f"{S3_FOLDER}/{timestamp}/{csv_file_name}"
    s3.upload_file(csv_file_path, s3_bucket, s3_key)

    logger.info(f"CSV uploaded to s3://{s3_bucket}/{s3_key}")

    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)


def pipeline_migration_table_mapping():

    items1, items2 = fetch_items_from_dynamodb()

    for item in items1:
        check_glue(item.get('destination_id'), item.get('id'), item)

    if not existing_tables:
        if env == 'stg':
            ses_publish("Dynamo table empty - STG", "No valid records found.")
            sys.exit("No valid records.")
        elif env == 'prd':
            if not SNS_TOPIC_ARN:
                raise ValueError("SNS_TOPIC_ARN not set")

            body = {
                "AlarmName": "DynamoDB table empty",
                "NewStateValue": "ALARM",
                "StateChangeTime": datetime.utcnow().isoformat() + 'Z',
                "state_message": "No valid v1-v2 sync records"
            }

            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject='No valid v1-v2 sync records',
                Message=json.dumps(body)
            )

            sys.exit("No valid records.")

    for item in items2:
        check_glue(item.get('destination_id'), item.get('id'), item)

    if existing_tables:
        upload_to_s3(existing_tables)

    main()


def handler(event, context):
    pipeline_migration_table_mapping()
    logger.info("Pipeline migration table mapping completed successfully.")
