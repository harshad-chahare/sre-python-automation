import os
import json
import boto3
import requests
import logging
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIGURATION — update these values to match your environment before running
# ---------------------------------------------------------------------------
SSM_PATHS = {
    "jira_token": "/your-team/support/jira/api/token",
}

JIRA_CONFIG = {
    "project_key": "YOUR_PROJECT_KEY",
    "issue_type":  "Support",
    "runbook_url": "https://your-wiki.example.com/runbook",
}

DYNAMODB_RECORD_IDS = {
    "default":   "/{env}/load-failure/ignore-list",
    "streaming": "/{env}/load-failure/streaming-ingestion-failure/ignore-list",
}

LOWER_ENVS = ["stage", "stg", "qa", "dev"]

# Keywords that identify a parsing error from the failure message
PARSING_ERROR_KEYWORDS = [
    "parsing error",
    "parse error",
    "pipeline parsing error",
    "failed to parse",
    "syntax error",
]

# Error messages that should be treated as informational (no alert needed)
INFORMATIONAL_ERRORS = [
    "Pipeline creation only supported in non-prod environments.",
    "Pipeline configuration not found",
]
# ---------------------------------------------------------------------------


# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment
env             = os.environ.get("STACK_ENV")
region_name     = os.environ.get("AWS_REGION", "us-east-1")
jira_api_url    = os.environ.get("JIRA_API_URL")
dynamo_table_name = os.environ.get("DYNAMODB_TABLE")

# AWS clients
dynamodb_resource = boto3.resource("dynamodb")
dynamo_table      = dynamodb_resource.Table(dynamo_table_name)


# ---------------------------------------------------------------------------
# Notification stub
# Replace this with your own alerting integration (e.g. PagerDuty, SNS, Slack)
# ---------------------------------------------------------------------------

def send_notification(payload: dict):
    """
    Send an alert payload to your on-call notification system.
    Implement this function using your preferred alerting provider.

    Example integrations:
      - SNS:       boto3.client('sns').publish(TopicArn=..., Message=json.dumps(payload))
      - PagerDuty: requests.post('https://events.pagerduty.com/v2/enqueue', json=payload)
      - Slack:     requests.post(webhook_url, json={"text": str(payload)})
    """
    raise NotImplementedError("Implement send_notification() with your alerting provider.")


# ---------------------------------------------------------------------------
# Jira
# ---------------------------------------------------------------------------

def _get_jira_token() -> str:
    ssm_client = boto3.client("ssm")
    response   = ssm_client.get_parameter(Name=SSM_PATHS["jira_token"], WithDecryption=True)
    return response["Parameter"]["Value"]


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token: str):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = f"Bearer {self.token}"
        return r


def create_jira_ticket(summary: str, description: str, priority: str = "P2") -> str:
    """Create a Jira support ticket and return the issue key."""
    token   = _get_jira_token()
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    payload = {
        "fields": {
            "project":   {"key":  JIRA_CONFIG["project_key"]},
            "issuetype": {"name": JIRA_CONFIG["issue_type"]},
            "priority":  {"name": priority},
            "summary":   summary,
            "description": description,
        }
    }

    response = requests.post(
        jira_api_url,
        data=json.dumps(payload),
        headers=headers,
        auth=BearerAuth(token),
    )

    if response.status_code == 201:
        issue_key = response.json()["key"]
        logger.info(f"Jira ticket created: {issue_key}")
        return issue_key

    logger.error(f"Failed to create Jira ticket: {response.status_code} - {response.text}")
    raise Exception(f"Jira ticket creation failed: {response.status_code} - {response.text}")


# ---------------------------------------------------------------------------
# DynamoDB — ignore list / deduplication
# ---------------------------------------------------------------------------

def _get_pipeline_records(record_id: str) -> list:
    """Return the pipeline_ids list from a DynamoDB item, or [] if not found."""
    response = dynamo_table.get_item(Key={"id": record_id})
    if "Item" not in response:
        logger.warning(f"No existing record found for '{record_id}'.")
        return []
    return response["Item"].get("pipeline_ids", [])


def is_event_ignored(database: str, table: str, dataset: str = None,
                     list_type: str = "default") -> bool:
    """Return True if this pipeline already has an open ticket in the ignore list."""
    key       = DYNAMODB_RECORD_IDS.get(list_type, DYNAMODB_RECORD_IDS["default"])
    record_id = key.format(env=env)
    try:
        existing  = _get_pipeline_records(record_id)
        pipeline_name = f"{dataset}__{database}.{table}"
        if any(p.get("pipeline_name") == pipeline_name for p in existing):
            logger.info(f"Existing entry found for '{pipeline_name}' in '{record_id}'.")
            return True
    except Exception as ex:
        logger.warning(f"Error checking ignore list: {ex}")
    return False


def update_pipeline_record(record_id: str, dataset: str, database: str,
                           table_val: str, ticket_id: str, pipeline_id: str = None):
    """Append a new pipeline entry to the DynamoDB deduplication record."""
    pipeline_name     = f"{dataset}__{database}.{table_val}"
    new_entry         = {"pipeline_name": pipeline_name, "ticket_id": ticket_id}
    if pipeline_id:
        new_entry["pipeline_id"] = pipeline_id

    dynamo_table.update_item(
        Key={"id": record_id},
        UpdateExpression="SET pipeline_ids = list_append(if_not_exists(pipeline_ids, :empty_list), :new_pipeline)",
        ExpressionAttributeValues={
            ":new_pipeline": [new_entry],
            ":empty_list":   [],
        },
    )
    logger.info(f"Recorded '{pipeline_name}' with ticket '{ticket_id}' in '{record_id}'.")


# ---------------------------------------------------------------------------
# Alert payload builder
# ---------------------------------------------------------------------------

def build_alert_payload(alarm_name: str, pipeline_name: str, failure_details,
                        failed_batch: dict, error_type: str, jira_error: Exception,
                        alert_message: str, action_required: str) -> dict:
    return {
        "AlarmName":       alarm_name,
        "NewStateValue":   "ALARM",
        "StateChangeTime": datetime.utcnow().isoformat(timespec="microseconds") + "Z",
        "alert_type":      "CRITICAL",
        "state_message": {
            "error_type":               error_type,
            "jira_error":               str(jira_error),
            "original_failure_details": failure_details,
            "failed_batch_info":        failed_batch,
            "pipeline_name":            pipeline_name,
        },
        "event_details": {
            "failed-batch":         failed_batch,
            "failure-details":      failure_details,
            "jira-creation-error":  str(jira_error),
            "pipeline_name":        pipeline_name,
            "action_required":      action_required,
        },
        "runbook": JIRA_CONFIG["runbook_url"],
        "description": alert_message,
    }


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

def is_parsing_error(failure_details) -> bool:
    """Return True if the failure message contains parsing-error keywords."""
    if isinstance(failure_details, dict):
        error_message = failure_details.get("message", "")
    else:
        error_message = str(failure_details) if failure_details else ""
    return any(kw.lower() in error_message.lower() for kw in PARSING_ERROR_KEYWORDS)


def is_informational_error(error_message: str) -> bool:
    """Return True if the error should be treated as informational (no alert needed)."""
    return any(msg in error_message for msg in INFORMATIONAL_ERRORS)


# ---------------------------------------------------------------------------
# Failure handlers
# ---------------------------------------------------------------------------

def _send_fallback_alert(alarm_name: str, pipeline_name: str, failure_details,
                         failed_batch: dict, error_type: str, jira_error: Exception,
                         alert_message: str, action_required: str):
    """Send an on-call alert when Jira ticket creation fails."""
    payload = build_alert_payload(
        alarm_name, pipeline_name, failure_details, failed_batch,
        error_type, jira_error, alert_message, action_required,
    )
    logger.info(f"Sending fallback alert: {json.dumps(payload, default=str)}")
    try:
        send_notification(payload)
        logger.info("Fallback alert sent successfully.")
    except Exception as notify_error:
        logger.error(f"Failed to send fallback alert: {notify_error}", exc_info=True)


def handle_streaming_ingestion_failure(failure_details: dict, failed_batch: dict,
                                       worker_name: str = None):
    """Handle a streaming ingestion failure — create Jira ticket or fall back to alert."""
    database      = failed_batch.get("database")
    table_val     = failed_batch.get("table")
    dataset       = failed_batch.get("dataset")
    schema        = failed_batch.get("schema")
    drop_location = failed_batch.get("drop-location")
    pipeline_id   = failed_batch.get("pipeline_id")
    pipeline_name = f"{dataset}__{database}.{table_val}"
    error_message = failure_details.get("message", "No error message provided")
    failure_type  = failure_details.get("failure-type", "UNKNOWN")

    if is_event_ignored(database, table_val, dataset, list_type="streaming"):
        logger.info(f"Streaming ingestion failure already tracked: {pipeline_name}")
        return

    priority  = "3-Medium" if env and env.lower() in LOWER_ENVS else "2-High"
    summary   = f"Streaming Ingestion Failure - {pipeline_name}"
    description = (
        f"*Streaming ingestion failure detected*\n\n"
        f"*Details:*\n"
        f"- Database: {database}\n"
        f"- Table: {table_val}\n"
        f"- Dataset: {dataset}\n"
        f"- Schema: {schema}\n"
        f"- Failure Type: {failure_type}\n"
        f"- Drop Location: {drop_location}\n\n"
        f"*Error Message:* {error_message}\n\n"
        f"*Runbook:* {JIRA_CONFIG['runbook_url']}"
    )
    record_id = DYNAMODB_RECORD_IDS["streaming"].format(env=env)

    try:
        ticket_id = create_jira_ticket(summary, description, priority=priority)
        update_pipeline_record(record_id, dataset, database, table_val, ticket_id, pipeline_id)
    except Exception as ex:
        logger.error(f"Jira ticket creation failed for streaming ingestion failure: {ex}", exc_info=True)
        worker_str = worker_name or "streaming_batch_processor"
        alarm_name = f"{env}-DataLake-LoadFailureEvent-StreamingIngestion-{worker_str}"
        _send_fallback_alert(
            alarm_name, pipeline_name, failure_details, failed_batch,
            "Streaming Ingestion Jira Ticket Creation Failed", ex,
            f"Jira ticket creation failed for streaming ingestion failure: {pipeline_name}. Error: {ex}",
            "Create a support ticket and investigate the streaming ingestion failure.",
        )


def handle_parsing_error(failure_details, failed_batch: dict, worker_name: str = None):
    """Handle a pipeline parsing error — create Jira ticket or fall back to alert."""
    database      = failed_batch.get("database")
    table_val     = failed_batch.get("table")
    dataset       = failed_batch.get("dataset")
    pipeline_id   = failed_batch.get("pipeline_id")
    pipeline_name = f"{dataset}__{database}.{table_val}"
    error_message = (failure_details.get("message", "No error message provided")
                     if isinstance(failure_details, dict) else str(failure_details))
    failure_type  = (failure_details.get("failure-type", "PARSING_ERROR")
                     if isinstance(failure_details, dict) else "PARSING_ERROR")

    if env and env.lower() in LOWER_ENVS:
        logger.info(f"Skipping Jira/alert for parsing error in lower environment: {pipeline_name}")
        return

    if is_event_ignored(database, table_val, dataset, list_type="default"):
        logger.info(f"Parsing error already tracked: {pipeline_name}")
        return

    summary     = f"Pipeline Parsing Error - {pipeline_name}"
    description = (
        f"*Pipeline parsing error detected*\n\n"
        f"*Details:*\n"
        f"- Database: {database}\n"
        f"- Table: {table_val}\n"
        f"- Dataset: {dataset}\n"
        f"- Failure Type: {failure_type}\n\n"
        f"*Error Message:* {error_message}\n\n"
        f"*Action Required:* Review and fix the pipeline configuration.\n\n"
        f"*Runbook:* {JIRA_CONFIG['runbook_url']}"
    )
    record_id = DYNAMODB_RECORD_IDS["default"].format(env=env)

    try:
        ticket_id = create_jira_ticket(summary, description, priority="2-High")
        update_pipeline_record(record_id, dataset, database, table_val, ticket_id, pipeline_id)
    except Exception as ex:
        logger.error(f"Jira ticket creation failed for parsing error: {ex}", exc_info=True)
        worker_str = worker_name or "load_failure_processor"
        alarm_name = f"{env}-DataLake-LoadFailureEvent-ParsingError-{worker_str}"
        _send_fallback_alert(
            alarm_name, pipeline_name, failure_details, failed_batch,
            "Parsing Error - Jira Ticket Creation Failed", ex,
            f"Jira ticket creation failed for parsing error: {pipeline_name}. Error: {ex}",
            "Create a support ticket and investigate the parsing error.",
        )


# ---------------------------------------------------------------------------
# Event parsing and routing
# ---------------------------------------------------------------------------

def parse_message(event: dict) -> dict | None:
    """
    Parse a load failure event, route it to the appropriate handler,
    and return an alert payload for general failures or None otherwise.
    """
    content = event.get("content")
    worker  = event.get("event", {})
    worker_name = worker.get("worker-name") if worker else None

    if not content:
        inner   = json.loads(event.get("Message", "{}"))
        content = inner.get("content", {})

    logger.info(f"content: {content}")

    failure_details = content.get("failure-details") or content.get("error-details", {})
    failed_batch    = content.get("failed-batch", {})
    database        = failed_batch.get("database")
    table           = failed_batch.get("table")
    dataset         = failed_batch.get("dataset")

    point_of_failure   = failure_details.get("point-of-failure", "") if isinstance(failure_details, dict) else ""
    error_message_text = failure_details.get("message", "") if isinstance(failure_details, dict) else str(failure_details or "")

    # --- Parsing error ---
    if is_parsing_error(failure_details):
        logger.info(f"Parsing error detected for {dataset}__{database}.{table}")
        if env and env.lower() in LOWER_ENVS:
            logger.info("Skipping Jira/alert for parsing error in lower environment.")
            return None
        if database and table and is_event_ignored(database, table, dataset, list_type="default"):
            logger.info(f"Parsing error already tracked: {dataset}__{database}.{table}")
            return None
        handle_parsing_error(failure_details, failed_batch, worker_name)
        return None

    # --- Streaming ingestion failure ---
    if point_of_failure == "Streaming ingestion":
        if database and table and is_event_ignored(database, table, dataset, list_type="streaming"):
            logger.info(f"Streaming ingestion failure already tracked: {dataset}__{database}.{table}")
            return None
        handle_streaming_ingestion_failure(failure_details, failed_batch, worker_name)
        return None

    # --- General failure ---
    if database and table and is_event_ignored(database, table, dataset, list_type="default"):
        logger.info(f"Event ignored (default ignore list): {dataset}__{database}.{table}")
        return None

    if is_informational_error(error_message_text):
        logger.info(f"Informational error — no alert required: {error_message_text[:80]}")
        return None

    tsid       = worker.get("tsid", "internal") if worker else "internal"
    alarm_name = f"{env}-DataLake-LoadFailureEvent-{worker_name}"
    logger.info(f"General failure — building CRITICAL alert: {alarm_name}")

    return {
        "AlarmName":       alarm_name,
        "NewStateValue":   "ALARM",
        "StateChangeTime": datetime.utcnow().isoformat(timespec="microseconds") + "Z",
        "alert_type":      "CRITICAL",
        "state_message":   failure_details,
        "event_details":   content,
        "runbook":         JIRA_CONFIG["runbook_url"],
    }


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def handler(event, context):
    logger.info(f"Received SQS event: {event}")
    for record in event["Records"]:
        body = json.loads(record["body"])
        msg  = parse_message(body)
        if msg is not None:
            logger.info(f"Sending notification: {msg}")
            send_notification(msg)
