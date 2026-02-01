etleap_notification_translatorimport os
import json
import logging
from datetime import datetime

import boto3

from producer_alert_routing.Alert_routing_utility import (
    route_alert,
    fetch_metadata,
    _send_ops_alert
)

# --------------------------------------------------
# LOGGER
# --------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------------------------------
# ENVIRONMENT
# --------------------------------------------------
STACK_ENV = os.environ.get("STACK_ENV", "dev")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

LOWER_ENVS = ["dev", "qa", "stage", "stg"]

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def env_prefix():
    return {
        "prd": "Prod",
        "stage": "Stage",
        "stg": "Stage",
        "qa": "QA",
        "dev": "Dev"
    }.get(STACK_ENV, STACK_ENV.upper())


def parse_entity(entity_name):
    """
    Parses pipeline entity name into dataset, database, table
    """
    if not entity_name:
        return None, None, None

    clean = entity_name.replace("{", "").replace("}", "")
    parts = clean.split("__")

    dataset = parts[0]
    table = parts[-1]
    database = "__".join(parts[1:-1]) if len(parts) > 2 else None

    return dataset, database, table


def build_api_payload(event_name, message, payload):
    return {
        "AlarmName": event_name,
        "NewStateValue": "ALARM",
        "StateChangeTime": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "message": message,
        "event": payload
    }


def sanitize_payload(api_payload):
    """
    Incident tools do not support HTML.
    Converts payload to plain text where needed.
    """
    event = api_payload.get("event", {})
    if not isinstance(event, dict):
        return api_payload

    for k, v in event.items():
        if isinstance(v, str):
            event[k] = v.replace("<br>", "\n").replace("<b>", "").replace("</b>", "")

    return api_payload


def build_html_message(dataset, database, table, api_payload, action=None):
    rows = "".join(
        f"<tr><td><b>{k}</b></td><td>{v}</td></tr>"
        for k, v in api_payload.get("event", {}).items()
        if v and k != "Action"
    )

    if action:
        rows += f"""
        <tr>
            <td><b>Action</b></td>
            <td>{action}</td>
        </tr>
        """

    severity = "CRITICAL" if STACK_ENV == "prd" else "INFO"
    color = "red" if STACK_ENV == "prd" else "orange"

    return f"""
    <html>
      <body>
        <h3 style="color:{color};">
          {api_payload.get("AlarmName")} [{severity}]
        </h3>
        <p>
          <b>Environment:</b> {STACK_ENV}<br>
          <b>Dataset:</b> {dataset}<br>
          <b>Database:</b> {database}<br>
          <b>Table:</b> {table}
        </p>
        <table border="1" cellpadding="6" cellspacing="0">
          {rows}
        </table>
      </body>
    </html>
    """


# --------------------------------------------------
# LAMBDA HANDLER
# --------------------------------------------------
def handler(event, context):
    logger.info(f"Incoming event: {json.dumps(event)}")

    records = event.get("Records", [])

    for record in records:
        payload = json.loads(record.get("body", "{}"))

        event_type = payload.get("eventType")
        subject = payload.get("subject", "")

        if event_type != "NOTIFICATION" or subject.startswith("OK:"):
            logger.info("Skipping non-actionable event")
            continue

        notification_type = payload.get("notificationType")
        message = payload.get("message", "")
        entity_name = payload.get("entityName")
        entity_id = payload.get("entityId")

        dataset, database, table = parse_entity(entity_name)
        resource_id = f"{dataset}|{database}|{table}"

        metadata = fetch_metadata(resource_id) or {}
        service_id = str(metadata.get("service_id", "SERVICE")).upper()

        alert_name = (
            f"{env_prefix()}-{service_id}-Pipeline Alert:"
            f"{notification_type}"
        )

        api_payload = build_api_payload(alert_name, message, payload)

        # --------------------------------------------------
        # CONNECTION ISSUE → OPS ONLY
        # --------------------------------------------------
        if notification_type == "CONNECTION_DOWN":
            sanitize_payload(api_payload)
            _send_ops_alert("Connection failure detected", api_payload)
            continue

        # --------------------------------------------------
        # DUPLICATE EVENT → PRODUCER + OPS
        # --------------------------------------------------
        if notification_type == "REJECTED_EVENT":
            api_payload["event"]["Action"] = (
                "Ensure event timestamps are unique.\n"
                "Reprocess the rejected batch after validation."
            )

            html_action = (
                "<ol>"
                "<li>Validate event timestamps.</li>"
                "<li>Reprocess the failed batch.</li>"
                "</ol>"
            )

            html_message = build_html_message(
                dataset, database, table, api_payload, action=html_action
            )

            route_alert(dataset, database, table, html_message, api_payload)
            sanitize_payload(api_payload)
            _send_ops_alert("Rejected event detected", api_payload)
            continue

        # --------------------------------------------------
        # SCHEMA CHANGE → PRODUCER + OPS
        # --------------------------------------------------
        if notification_type == "PIPELINE_SCHEMA_CHANGE":
            api_payload["event"]["Action"] = (
                "Review schema changes and validate compatibility.\n"
                "Proceed with reprocessing after approval."
            )

            html_action = (
                "<ol>"
                "<li>Review schema compatibility.</li>"
                "<li>Approve and reprocess data.</li>"
                "</ol>"
            )

            html_message = build_html_message(
                dataset, database, table, api_payload, action=html_action
            )

            route_alert(dataset, database, table, html_message, api_payload)
            sanitize_payload(api_payload)
            _send_ops_alert("Schema change detected", api_payload)
            continue

        # --------------------------------------------------
        # ALL OTHER EVENTS → OPS
        # --------------------------------------------------
        sanitize_payload(api_payload)
        _send_ops_alert("Unhandled pipeline notification", api_payload)

    return {
        "statusCode": 200,
        "message": "Pipeline notifications processed successfully"
    }
