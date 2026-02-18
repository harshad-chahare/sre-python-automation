import os
import json
import boto3
from datetime import datetime
import requests
import time
import logging

# ---------------------------------------------------------------------------
# CONFIGURATION — update these values to match your environment before running
# ---------------------------------------------------------------------------
SSM_PATHS = {
    "access_key":  "/pipeline/api/{env}/access_key",
    "secret_key":  "/pipeline/api/{env}/secret_key",
    "jira_token":  "/your-team/support/jira/api/token",
}

DYNAMODB_TABLES = {
    "prd": "your-ops-registry-table-prd",
    # add other environments as needed
}

SNS_TOPICS = {
    "prd": "arn:aws:sns:{region}:{account_id}:YOUR-CRITICAL-ALERTS-prd",
    "non_prd": "arn:aws:sns:{region}:{account_id}:YOUR-CRITICAL-ALERTS-{env}",
}

JIRA_CONFIG = {
    "project_key": "YOUR_PROJECT_KEY",
    "issue_type":  "Support",
}

PIPELINE_RECORD_ID_TEMPLATE = "/pipeline/{env}/parsing_error_pipeline"
# ---------------------------------------------------------------------------


# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
region_name = os.environ.get("AWS_REGION", "us-east-1")
ssm_client  = boto3.client("ssm", region_name=region_name)

# Environment setup
env = os.environ.get("STACK_ENV")

if env == "prd":
    pipeline_api_url = os.environ.get("PIPELINE_API_URL_PRD")
else:
    pipeline_api_url = os.environ.get("PIPELINE_API_URL_NONPRD")

# Retry counter for transient API failures
_retry_count = 0
MAX_RETRIES  = 5
RETRY_DELAY  = 2.4  # seconds


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def handler(event, context):
    logger.info(f"Received event: {event}")
    for record in event.get("Records", []):
        payload    = json.loads(record["body"])
        event_type = payload["eventType"]
        logger.info(payload)
        process_event(event_type, payload, retry_count=0)


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def get_pipeline_creds() -> tuple:
    """Fetch pipeline API credentials from SSM Parameter Store."""
    access_key_path = SSM_PATHS["access_key"].format(env=env)
    secret_key_path = SSM_PATHS["secret_key"].format(env=env)

    response = ssm_client.get_parameters(
        Names=[access_key_path, secret_key_path],
        WithDecryption=True,
    )
    creds = {param["Name"]: param["Value"] for param in response["Parameters"]}
    return creds[access_key_path], creds[secret_key_path]


def get_jira_token() -> str:
    """Fetch Jira API token from SSM Parameter Store."""
    response = ssm_client.get_parameter(
        Name=SSM_PATHS["jira_token"],
        WithDecryption=True,
    )
    return response["Parameter"]["Value"]


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------

def publish_sns_alert(alarm_name: str, message: str, payload: dict):
    """Publish an alert to the configured SNS topic."""
    if env == "prd":
        topic_arn = SNS_TOPICS["prd"].format(region=region_name)
    else:
        topic_arn = SNS_TOPICS["non_prd"].format(region=region_name, env=env)

    sns_client = boto3.client("sns", region_name=region_name)
    msg = {
        "AlarmName":       alarm_name,
        "NewStateValue":   "ALARM",
        "StateChangeTime": datetime.now().isoformat(timespec="microseconds") + "Z",
        "event":           payload,
        "state_message":   message,
    }

    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=f"DataLake-Operations-NotificationEvent-Error-{env}",
        Message=json.dumps(msg),
    )
    logger.info(f"SNS message published: {response}")


# ---------------------------------------------------------------------------
# Jira
# ---------------------------------------------------------------------------

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token: str):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = f"Bearer {self.token}"
        return r


def create_jira_ticket(summary: str, description: str, priority: str = "P2") -> str:
    """Create a Jira support ticket and return the issue key."""
    jira_token = get_jira_token()
    jira_url   = os.environ.get("JIRA_API_URL")

    headers = {
        "Accept":       "application/json",
        "Content-Type": "application/json",
    }
    payload = {
        "fields": {
            "project":     {"key":  JIRA_CONFIG["project_key"]},
            "issuetype":   {"name": JIRA_CONFIG["issue_type"]},
            "priority":    {"name": priority},
            "summary":     summary,
            "description": description,
        }
    }

    response = requests.post(
        jira_url,
        data=json.dumps(payload),
        headers=headers,
        auth=BearerAuth(jira_token),
    )

    if response.status_code == 201:
        issue_key = response.json()["key"]
        logger.info(f"Jira ticket created: {issue_key}")
        return issue_key

    logger.error(f"Failed to create Jira ticket: {response.status_code} - {response.text}")
    raise Exception(f"Failed to create Jira ticket: {response.status_code} - {response.text}")


# ---------------------------------------------------------------------------
# DynamoDB — parsing error pipeline tracking
# ---------------------------------------------------------------------------

def _get_ops_table(dynamodb):
    table_name = DYNAMODB_TABLES.get(env)
    if not table_name:
        raise ValueError(f"No DynamoDB table configured for environment: {env}")
    return dynamodb.Table(table_name)


def get_existing_parsing_error_pipelines(dynamodb) -> list:
    """Return the list of pipeline entries already tracked in DynamoDB."""
    record_id = PIPELINE_RECORD_ID_TEMPLATE.format(env=env)
    table     = _get_ops_table(dynamodb)
    response  = table.get_item(Key={"id": record_id})

    if "Item" not in response:
        logger.warning(f"No existing record found for '{record_id}'. A new one will be created when needed.")
        return []

    return response["Item"].get("pipeline_ids", [])


def update_parsing_error_record(dynamodb, pipeline_id: str, pipeline_name: str, ticket_id: str):
    """Append a new pipeline entry to the parsing error tracking record in DynamoDB."""
    record_id = PIPELINE_RECORD_ID_TEMPLATE.format(env=env)
    table     = _get_ops_table(dynamodb)

    new_entry = {
        "pipeline_id":   pipeline_id,
        "pipeline_name": pipeline_name,
        "ticket_id":     ticket_id,
    }

    table.update_item(
        Key={"id": record_id},
        UpdateExpression="SET pipeline_ids = list_append(if_not_exists(pipeline_ids, :empty_list), :new_pipeline)",
        ExpressionAttributeValues={
            ":new_pipeline": [new_entry],
            ":empty_list":   [],
        },
    )
    logger.info(f"Recorded pipeline {pipeline_id} in parsing error tracking table.")


# ---------------------------------------------------------------------------
# Core event processing
# ---------------------------------------------------------------------------

def _handle_parsing_error(auth, payload: dict, entity_id: str, entity_name: str,
                           entity_type: str, message: str, notification_type: str):
    """Handle PIPELINE_PARSING_ERRORS notification type."""
    response = requests.get(
        f"{pipeline_api_url}/{entity_id}?pageSize=0&timeout=900",
        auth=auth,
    )
    logger.info(f"Pipeline API response: {response.status_code}")

    if response.status_code != 200:
        return False  # Signal caller to retry

    result           = response.json()
    parsing_settings = result.get("parsingErrorSettings", {})
    action           = parsing_settings.get("action")
    stop_reason      = result.get("stopReason")

    logger.info(f"parsingErrorSettings: action={action}, threshold={parsing_settings.get('threshold')}")

    if stop_reason == "PARSING_ERRORS" or action in ["STOP", "NOTIFY"]:
        logger.info(f"Pipeline '{entity_name}' has active parsing errors — taking action.")

        if env == "prd":
            dynamodb          = boto3.resource("dynamodb", region_name=region_name)
            existing_pipelines = get_existing_parsing_error_pipelines(dynamodb)

            if any(p["pipeline_id"] == entity_id for p in existing_pipelines):
                logger.info(f"Jira ticket already exists for pipeline {entity_id}. Skipping.")
                return True

            summary     = f"Pipeline Parsing Error - {entity_name}"
            description = (
                f"Pipeline '{entity_name}' (ID: {entity_id}) has been failing due to parsing errors. "
                "Please raise this with the data producer.\n\nThanks!"
            )
            try:
                ticket_id = create_jira_ticket(summary, description, priority="2-High")
                logger.info(f"Jira ticket {ticket_id} created for pipeline {entity_id}.")
                update_parsing_error_record(dynamodb, entity_id, entity_name, ticket_id)
            except Exception as ex:
                logger.error(f"Jira ticket creation failed for {entity_id}: {ex}")
                alarm_name = f"{env}-DataLake-{notification_type}-{entity_type}-{entity_id}"
                publish_sns_alert(alarm_name, f"Jira ticket creation failed for {entity_id}", payload)
        else:
            alarm_name = f"{env}-DataLake-{notification_type}-{entity_type}-{entity_id}"
            publish_sns_alert(alarm_name, message, payload)
    else:
        logger.info(f"Pipeline '{entity_name}' has no active parsing errors. No action taken.")

    return True


def process_event(event_type: str, payload: dict, retry_count: int = 0):
    auth    = get_pipeline_creds()
    subject = payload.get("subject", "INVALID_SUBJECT")

    if event_type != "NOTIFICATION" or subject == "INVALID_SUBJECT" or subject.startswith("OK:"):
        logger.info("Event is not an actionable NOTIFICATION. Skipping.")
        return

    message           = payload.get("message")
    notification_type = payload.get("notificationType")
    entity_id         = payload.get("entityId")
    entity_type       = payload.get("entityType")
    entity_name       = payload.get("entityName")

    if notification_type == "CONNECTION_DOWN":
        alarm_name = f"{env}-DataLake-{notification_type}-{entity_type}"
        publish_sns_alert(alarm_name, message, payload)

    elif notification_type == "PIPELINE_PARSING_ERRORS":
        success = _handle_parsing_error(
            auth, payload, entity_id, entity_name, entity_type, message, notification_type
        )
        if not success:
            if retry_count < MAX_RETRIES:
                logger.warning(f"Retrying event processing (attempt {retry_count + 1}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY)
                process_event(event_type, payload, retry_count + 1)
            else:
                logger.error(f"Max retries reached for pipeline {entity_id}. Giving up.")

    elif notification_type == "REJECTED_EVENT":
        alarm_name = f"{env}-DataLake-{notification_type}-{entity_type}"
        publish_sns_alert(alarm_name, message, payload)

    else:
        alarm_name = f"{env}-DataLake-{notification_type}-{entity_type}-{entity_id}"
        publish_sns_alert(alarm_name, message, payload)
