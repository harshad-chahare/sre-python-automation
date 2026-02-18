import boto3
import pandas as pd
import json
import re
import argparse
import os
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# ================= CLI ARGUMENTS =================

parser = argparse.ArgumentParser(description="Replay TRANSFORMATION_COMPLETE events")

parser.add_argument("--start", required=True, help="Start datetime (YYYY-MM-DD HH:MM:SS)")
parser.add_argument("--end", required=True, help="End datetime (YYYY-MM-DD HH:MM:SS)")
parser.add_argument("--csv", required=True, help="Path to input CSV file")
parser.add_argument("--env", required=True, choices=["stg", "prd"], help="Environment (stg/prd)")

args = parser.parse_args()

START_DATE = args.start
END_DATE = args.end
CSV_PATH = os.path.normpath(args.csv)
ENV = args.env

# ================= CONFIG =================
# Resource identifiers are loaded from environment variables.
# Set the following before running:
#
#   STG_LOG_GROUP   - CloudWatch log group name for staging
#   PRD_LOG_GROUP   - CloudWatch log group name for production
#   STG_SNS_TOPIC   - SNS topic ARN for staging
#   PRD_SNS_TOPIC   - SNS topic ARN for production
#   AWS_REGION      - AWS region (default: us-east-1)
#
# Example (.env or shell export):
#   export STG_LOG_GROUP="/aws/lambda/data-ingestion-stg-insightsLambda-XXXX"
#   export PRD_LOG_GROUP="/aws/lambda/data-ingestion-prd-insightsLambda-XXXX"
#   export STG_SNS_TOPIC="arn:aws:sns:us-east-1:<account-id>:etleap-stg-EtleapInputSNS-XXXX"
#   export PRD_SNS_TOPIC="arn:aws:sns:us-east-1:<account-id>:etleap-prd-EtleapInputSNS-XXXX"

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

LOG_GROUP_MAP = {
    "stg": os.environ["STG_LOG_GROUP"],
    "prd": os.environ["PRD_LOG_GROUP"],
}

SNS_TOPIC_MAP = {
    "stg": os.environ["STG_SNS_TOPIC"],
    "prd": os.environ["PRD_SNS_TOPIC"],
}

LOG_GROUP_NAME = LOG_GROUP_MAP[ENV]
SNS_TOPIC_ARN = SNS_TOPIC_MAP[ENV]

# ================= TIME WINDOW =================

start_time = int(datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
end_time = int(datetime.strptime(END_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

# ================= AWS CLIENTS =================

logs_client = boto3.client("logs", region_name=AWS_REGION)
sns_client = boto3.client("sns", region_name=AWS_REGION)

# ================= HELPERS =================

def extract_json_from_log(message_text):
    match = re.search(r'message=(\{.*\})', message_text)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def fetch_transformation_complete(trace_id):
    next_token = None

    while True:
        params = {
            "logGroupName": LOG_GROUP_NAME,
            "startTime": start_time,
            "endTime": end_time,
            "filterPattern": f'"TRANSFORMATION_COMPLETE" "{trace_id}"'
        }

        if next_token:
            params["nextToken"] = next_token

        response = logs_client.filter_log_events(**params)

        for event in response.get("events", []):
            parsed_json = extract_json_from_log(event["message"])
            if not parsed_json:
                continue

            if parsed_json.get("eventType") != "TRANSFORMATION_COMPLETE":
                continue

            if parsed_json.get("sourceHighWatermark") != trace_id:
                continue

            return parsed_json

        next_token = response.get("nextToken")
        if not next_token:
            break

    return None


def validate_database_table(event_json, csv_database, csv_table):
    pipeline_name = event_json.get("pipelineName")

    if not pipeline_name:
        return False

    match = re.match(r"\{(.+?)\}__\{(.+?)\}", pipeline_name)
    if not match:
        return False

    json_database = match.group(1).strip()
    json_table = match.group(2).strip()

    return (
        json_database == csv_database.strip()
        and json_table == csv_table.strip()
    )


def publish_to_sns(payload):
    try:
        return sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(payload, ensure_ascii=False),
            MessageAttributes={
                "eventType": {
                    "DataType": "String",
                    "StringValue": "TRANSFORMATION_COMPLETE"
                }
            }
        )
    except ClientError as e:
        logger.info(f"SNS Publish FAILED: {e.response['Error']['Message']}")
        return None


# ================= MAIN =================

def main():
    process_failed = False
    sns_success = False

    logger.info("=" * 80)
    logger.info(f"Environment : {ENV.upper()}")
    logger.info(f"Log Group   : {LOG_GROUP_NAME}")
    logger.info(f"SNS Topic   : {SNS_TOPIC_ARN}")
    logger.info(f"Time Range  : {START_DATE} → {END_DATE}")
    logger.info(f"CSV Path    : {CSV_PATH}")
    logger.info("-" * 80)

    try:
        df = pd.read_csv(CSV_PATH, dtype=str)
    except Exception as e:
        logger.info(f"Failed to read CSV: {str(e)}")
        process_failed = True
        df = None

    if process_failed:
        logger.info("\nProcess halted due to CSV read error.")
        logger.info("=" * 80)
        return

    required_columns = {"trace_id", "dataset", "database", "table"}
    if not required_columns.issubset(df.columns):
        logger.info("CSV must contain: trace_id, dataset, database, table")
        process_failed = True

    if process_failed:
        logger.info("\nProcess halted due to validation error.")
        logger.info("=" * 80)
        return

    for idx, row in df.iterrows():
        try:
            trace_id = row["trace_id"].strip()
            dataset = row["dataset"].strip()
            database_part = row["database"].strip()
            table = row["table"].strip()

            database = f"{dataset}__{database_part}"

            logger.info(f"\n[{idx+1}/{len(df)}] Processing for trace_id: {trace_id}")
            logger.info(f"Database : {database}")
            logger.info(f"Table    : {table}")

            logger.info("Fetching transformation complete event...")
            event = fetch_transformation_complete(trace_id)

            if not event:
                logger.info("No matching event found")
                continue

            logger.info("Event fetched successfully")

            if not validate_database_table(event, database, table):
                logger.info("Validation failed")
                continue

            response = publish_to_sns(event)

            if response:
                logger.info(f"SNS MessageId: {response.get('MessageId')}")
                sns_success = True
            else:
                logger.info("SNS publish failed\n")
                process_failed = True

        except Exception as e:
            logger.info(f"Error processing row: {str(e)}")
            process_failed = True

    logger.info("")
    if process_failed:
        logger.info("Process halted due to error.")
    elif sns_success:
        logger.info("Process completed successfully. SNS triggered.")
    else:
        logger.info("No SNS messages were triggered.")

    logger.info("=" * 80)

if __name__ == "__main__":
    main()
