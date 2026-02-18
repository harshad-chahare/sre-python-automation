import csv
import json
import uuid
import os
import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')

env = os.environ.get('STACK_ENV')

ALERT_EMAIL = os.environ.get("ALERT_EMAIL")
SNAPSHOT_TABLE = os.environ.get("SNAPSHOT_TABLE")
SNAPSHOT_ITEM_ID = os.environ.get("SNAPSHOT_ITEM_ID")
DROP_BUCKET = os.environ.get("DROP_BUCKET")


def fetch_snapshot_id():
    table = dynamodb.Table(SNAPSHOT_TABLE)
    response = table.get_item(Key={'id': SNAPSHOT_ITEM_ID})

    if 'Item' in response:
        return response['Item'].get('snapshot_id')

    return None


def send_email(html_content, subject):
    if not ALERT_EMAIL:
        raise ValueError("ALERT_EMAIL not configured")

    ses.send_email(
        Source=ALERT_EMAIL,
        Destination={'ToAddresses': [ALERT_EMAIL]},
        Message={
            'Subject': {'Data': subject},
            'Body': {'Html': {'Data': html_content}}
        }
    )


def main():

    s3_prefix = 'v1_v2_mapping_validation/'
    csv_bucket_name = os.environ.get("OPERATIONS_BUCKET")

    snapshot_id = fetch_snapshot_id()
    if not snapshot_id:
        raise ValueError("Snapshot ID not found")

    base_s3_path = f"datasets/{env}/pipeline_migration_table_mapping/{snapshot_id}/"

    response = s3.list_objects_v2(Bucket=csv_bucket_name, Prefix=s3_prefix)
    files = [obj['Key'] for obj in response.get('Contents', [])]
    files.sort(reverse=True)

    if len(files) < 2:
        raise ValueError("Not enough files for comparison")

    new_file, old_file = files[0], files[1]

    s3.download_file(csv_bucket_name, old_file, '/tmp/old_file.csv')
    s3.download_file(csv_bucket_name, new_file, '/tmp/new_file.csv')

    with open('/tmp/old_file.csv') as f:
        old_data = set(tuple(row) for row in csv.reader(f) if row)

    with open('/tmp/new_file.csv') as f:
        new_data = set(tuple(row) for row in csv.reader(f) if row)

    added = new_data - old_data
    removed = old_data - new_data

    if removed:
        html = "<html><body><h3>Deleted Records</h3></body></html>"
        send_email(html, "Deleted V1-V2 Mappings")

    os.remove('/tmp/old_file.csv')
    os.remove('/tmp/new_file.csv')
