import os
import json
import boto3
import logging
import requests
from boto3.dynamodb.conditions import Key
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --------------------------------------------------
# AWS CLIENTS
# --------------------------------------------------
dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")
sns = boto3.client("sns")

ENV = os.environ.get("STACK_ENV", "dev")

# --------------------------------------------------
# ENV CONFIG (COMPLIANCE SAFE)
# --------------------------------------------------
SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL", "support@example.com")
OPS_EMAIL = os.environ.get("OPS_EMAIL", "ops@example.com")
OPS_SNS_TOPIC_ARN = os.environ.get("OPS_SNS_TOPIC_ARN")
INCIDENT_API_BASE_URL = os.environ.get("INCIDENT_API_BASE_URL")
RUNBOOK_URL = os.environ.get("RUNBOOK_URL", "https://runbook.example.com")

# --------------------------------------------------
# LOGGER
# --------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --------------------------------------------------
# CENTRALIZED ERROR HANDLING
# --------------------------------------------------
_ALERT_SENT = False


def _send_ops_alert(context, payload):
    global _ALERT_SENT
    if _ALERT_SENT:
        logger.warning("Duplicate alert suppressed")
        return False

    _ALERT_SENT = True
    logger.error(f"[OPS ALERT] {context}")

    if not OPS_SNS_TOPIC_ARN:
        logger.error("OPS SNS Topic ARN not configured")
        return False

    sns.publish(
        TopicArn=OPS_SNS_TOPIC_ARN,
        Subject=context,
        Message=json.dumps(payload, default=str)
    )
    return True


# --------------------------------------------------
# METADATA FETCH
# --------------------------------------------------
def fetch_metadata(resource_id):
    table_name = f"alert-routing-metadata-{ENV}"

    try:
        resp = dynamodb.Table(table_name).query(
            KeyConditionExpression=Key("id").eq(resource_id)
        )

        if resp.get("Count", 0) == 0:
            _send_ops_alert(
                "Metadata not found",
                {"resource_id": resource_id}
            )
            return None

        item = resp["Items"][0]

        required = [
            "service_id",
            "owner_email"
        ]

        missing = [f for f in required if not item.get(f)]

        vo_flag = item.get("incident_tool_enabled", False)
        item["incident_tool_enabled"] = str(vo_flag).lower() == "true"

        if item["incident_tool_enabled"] and not item.get("incident_routing_key"):
            missing.append("incident_routing_key")

        if missing:
            _send_ops_alert(
                "Metadata validation failed",
                {"resource_id": resource_id, "missing": missing}
            )
            return None

        return item

    except Exception as e:
        _send_ops_alert(
            "Metadata fetch exception",
            {"resource_id": resource_id, "error": str(e)}
        )
        return None


# --------------------------------------------------
# PILOT CHECK
# --------------------------------------------------
def is_routing_enabled(service_id):
    table_name = f"alert-routing-registry-{ENV}"
    registry_id = "routing_enabled_services"

    resp = dynamodb.Table(table_name).get_item(Key={"id": registry_id})
    item = resp.get("Item", {})

    enabled_list = [str(x).lower() for x in item.get("services", [])]
    return service_id.lower() in enabled_list


# --------------------------------------------------
# EMAIL NOTIFICATION
# --------------------------------------------------
def send_email(html, recipients, resource_id, service_id):
    subject = f"{ENV} | Processing failure | {resource_id}"

    ses.send_email(
        Source=SUPPORT_EMAIL,
        Destination={
            "ToAddresses": recipients,
            "CcAddresses": [OPS_EMAIL]
        },
        Message={
            "Subject": {"Data": subject},
            "Body": {"Html": {"Data": html}}
        }
    )
    return True


# --------------------------------------------------
# INCIDENT MANAGEMENT TOOL
# --------------------------------------------------
def send_incident(routing_key, payload, entity):
    if not INCIDENT_API_BASE_URL:
        logger.warning("Incident API base URL not configured")
        return False

    url = f"{INCIDENT_API_BASE_URL}/{routing_key}"

    payload["entity"] = entity
    payload["severity"] = "CRITICAL"
    payload["runbook"] = RUNBOOK_URL

    resp = requests.post(url, json=payload, verify=False)
    return resp.status_code in (200, 201)


# --------------------------------------------------
# ROUTING LOGIC
# --------------------------------------------------
def route_alert(
    dataset,
    database,
    table,
    html_message,
    api_payload
):
    resource_id = f"{dataset}|{database}|{table}"

    metadata = fetch_metadata(resource_id)
    if not metadata:
        return {"error": "metadata_missing"}

    service_id = metadata["service_id"]
    owner_email = metadata["owner_email"]

    if not is_routing_enabled(service_id):
        logger.info(f"Routing disabled | service_id={service_id}")
        return {"routing": "disabled"}

    email_success = send_email(
        html_message,
        [owner_email],
        resource_id,
        service_id
    )

    incident_success = True
    routing_key = None

    if metadata.get("incident_tool_enabled"):
        routing_key = metadata.get("incident_routing_key")
        entity = f"{dataset}.{database}.{table}"
        incident_success = send_incident(
            routing_key,
            api_payload,
            entity
        )

    if not email_success or not incident_success:
        _send_ops_alert(
            "Producer routing failure",
            api_payload
        )

    return {
        "email_sent": email_success,
        "incident_sent": incident_success,
        "service_id": service_id,
        "owner_email": owner_email
    }
