import requests
import json
import boto3
import os
import logging
from datetime import datetime
import pg8000.native

# ---------------------------------------------------------------------------
# CONFIGURATION — update these values to match your environment before running
# ---------------------------------------------------------------------------
SSM_PATHS = {
    "access_key": "/pipeline/api/{env}/access_key",
    "secret_key": "/pipeline/api/{env}/secret_key",
}

SNS_TOPICS = {
    "prd":     "arn:aws:sns:{region}:{account_id}:YOUR-CRITICAL-ALERTS-prd",
    "non_prd": "arn:aws:sns:{region}:{account_id}:YOUR-CRITICAL-ALERTS-{env}",
}

DYNAMODB_TABLES = {
    "prd": "your-ops-registry-table-prd",
    # add other environments as needed: "stg": "your-ops-registry-table-stg"
}

SECRETS_MANAGER_SECRET_NAME = "your-service-{env}-rds-credentials"
RDS_DATABASE_NAME           = "your_database_name"
RDS_SCHEMA_NAME             = "your_schema_name"      # used in the SQL query

DYNAMODB_KEYS = {
    "pipeline_ignore_list":    "{env}/pipeline_latent_ignore_list",
    "pipeline_thresholds":     "{env}/pipeline_latent_thresholds",
    "streaming_ignore_list":   "streaming-latent-pipeline-ignore-list",
    "streaming_thresholds":    "streaming-latent-pipeline-threshold",
    "default_latency":         "{env}/default_latency_threshold",
}

DEFAULT_STREAMING_LATENCY_THRESHOLD_HRS = 1.0
# ---------------------------------------------------------------------------


# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
region_name = os.environ.get("AWS_REGION", "us-east-1")
sns_client  = boto3.client("sns")
ssm_client  = boto3.client("ssm")

env = os.environ.get("STACK_ENV")

# Resolve DynamoDB table name for current environment
ops_table_name = DYNAMODB_TABLES.get(env, DYNAMODB_TABLES.get("prd"))


# ---------------------------------------------------------------------------
# Credentials & parameter helpers
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


def get_rds_secret() -> dict:
    """Fetch RDS credentials from Secrets Manager."""
    secret_name = SECRETS_MANAGER_SECRET_NAME.format(env=env)
    client      = boto3.client("secretsmanager", region_name=region_name)
    secret      = client.get_secret_value(SecretId=secret_name)["SecretString"]
    return json.loads(secret)


def _get_dynamodb_item(key_value: str) -> dict:
    """
    Generic DynamoDB get_item by partition key 'id'.
    Replace this with your own utility if you have one.
    """
    client   = boto3.client("dynamodb", region_name=region_name)
    response = client.get_item(
        TableName=ops_table_name,
        Key={"id": {"S": key_value}},
    )
    return response.get("Item", {})


def get_param_list(param_key: str) -> str:
    """
    Return a comma-separated string or JSON string from a DynamoDB 'list' attribute.
    Raises if the parameter is not found.
    """
    item = _get_dynamodb_item(param_key)
    if not item or "list" not in item:
        raise Exception(f"Parameter not found in DynamoDB for ID: {param_key}")

    param_list = item["list"]
    if isinstance(param_list, list):
        return ",".join(param_list) if param_list else "-"
    if isinstance(param_list, dict):
        return json.dumps(param_list) if param_list else "-"
    raise Exception(f"Unsupported type for 'list' in parameter '{param_key}': {type(param_list)}")


def get_param_value(param_key: str) -> str:
    """Return the scalar 'value' attribute from a DynamoDB item."""
    item = _get_dynamodb_item(param_key)
    if not item or "value" not in item:
        raise Exception(f"Parameter not found in DynamoDB for ID: {param_key}")
    return item["value"]


# ---------------------------------------------------------------------------
# Pipeline API (batch ingestion) latency check
# ---------------------------------------------------------------------------

def check_pipeline_latency() -> list:
    """
    Fetch all pipelines from the REST API and return those whose latency
    exceeds their configured threshold (or the default threshold).
    """
    base_url    = os.environ.get("PIPELINE_BASE_URL", "")
    api_url     = f"{base_url}/api/v2/pipelines?pageSize=0&timeout=300"
    user, passwd = get_pipeline_creds()

    response  = requests.get(api_url, auth=(user, passwd), verify=True)
    response.raise_for_status()
    json_data = response.json()

    ignore_list_key  = DYNAMODB_KEYS["pipeline_ignore_list"].format(env=env)
    threshold_key    = DYNAMODB_KEYS["pipeline_thresholds"].format(env=env)
    default_key      = DYNAMODB_KEYS["default_latency"].format(env=env)

    ignore_list       = get_param_list(ignore_list_key)
    threshold_map     = json.loads(get_param_list(threshold_key))
    default_threshold = int(get_param_value(default_key))

    latent_pipelines = []

    for pipeline in json_data.get("pipelines", []):
        pid = pipeline["id"]

        if pid in ignore_list:
            logger.info(f"Ignoring pipeline id={pid} (on ignore list).")
            continue

        if "latency" not in pipeline:
            continue

        latency_hrs       = pipeline["latency"] / 3600
        pname             = pipeline["name"]
        triggered_by_event = pipeline["source"].get("triggeredByEvent", "false")
        threshold         = float(threshold_map.get(pid, default_threshold))

        logger.info(f"Pipeline id={pid} latency={latency_hrs:.2f}h threshold={threshold}h")

        if latency_hrs > threshold:
            latent_pipelines.append({
                "id":                pid,
                "name":              pname,
                "latency_in_hrs":    latency_hrs,
                "triggered_by_event": triggered_by_event,
                "source":            "batch",
            })

    return latent_pipelines


# ---------------------------------------------------------------------------
# Streaming ingestion latency check (via RDS)
# ---------------------------------------------------------------------------

def connect_to_db():
    """Open and return a pg8000 connection using Secrets Manager credentials."""
    secret = get_rds_secret()
    return pg8000.native.Connection(
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=int(secret["port"]),
        database=RDS_DATABASE_NAME,
    )


def _get_streaming_ignore_list() -> list:
    """Return pipeline IDs to exclude from the streaming latency check."""
    try:
        item = _get_dynamodb_item(DYNAMODB_KEYS["streaming_ignore_list"])
        return item.get("pipeline", {}).get("SS", [])
    except Exception as ex:
        logger.error(f"Error fetching streaming ignore list: {ex}")
        return []


def _get_streaming_thresholds() -> dict:
    """Return a dict of {pipeline_id: threshold_hrs} for streaming pipelines."""
    try:
        item         = _get_dynamodb_item(DYNAMODB_KEYS["streaming_thresholds"])
        pipeline_map = item.get("pipeline", {}).get("M", {})
        return {k: float(v["N"]) for k, v in pipeline_map.items()}
    except Exception as ex:
        logger.error(f"Error fetching streaming thresholds: {ex}")
        return {}


def check_streaming_latency() -> list:
    """
    Query the RDS events table for in-progress batches older than 1 hour and
    return those whose latency exceeds their configured threshold.
    """
    ignore_list = _get_streaming_ignore_list()
    thresholds  = _get_streaming_thresholds()

    # Selects the oldest in-progress batch per (dataset, database, table) combination
    # that has been waiting for more than 1 hour, to identify latent streaming pipelines.
    query = f"""
        WITH in_progress_batches AS (
            SELECT
                dl_dataset,
                dl_database,
                dl_table,
                trace_id,
                s3_event_time AT TIME ZONE 'UTC'  AS s3_event_time_utc,
                batch_status,
                etl,
                ids_batch_location,
                ids_batch_size,
                record_update_time,
                ROUND(
                    (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                     - EXTRACT(EPOCH FROM s3_event_time)) / 3600.0,
                    2
                ) AS latency_in_hrs,
                CASE
                    WHEN CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
                         - s3_event_time AT TIME ZONE 'UTC' > INTERVAL '1 hour'
                    THEN TRUE
                    ELSE FALSE
                END AS is_latent
            FROM {RDS_SCHEMA_NAME}.events
            WHERE
                etl = 'streaming'
                AND batch_status = 'in-progress'
                AND CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
                    - s3_event_time AT TIME ZONE 'UTC' > INTERVAL '1 hour'
        ),
        ranked_batches AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY dl_dataset, dl_database, dl_table
                    ORDER BY s3_event_time_utc ASC
                ) AS rn
            FROM in_progress_batches
        )
        SELECT
            dl_dataset,
            dl_database,
            dl_table,
            trace_id,
            s3_event_time_utc,
            batch_status,
            etl,
            ids_batch_location,
            ids_batch_size,
            latency_in_hrs,
            is_latent
        FROM ranked_batches
        WHERE rn = 1;
    """

    conn = connect_to_db()
    latent_pipelines = []
    try:
        rows = conn.run(query)
        for row in (rows or []):
            pipeline_id    = f"{row[0]}|{row[1]}|{row[2]}"
            latency_hrs    = row[9]
            trace_id       = row[3]

            if pipeline_id in ignore_list:
                logger.info(f"Ignoring streaming pipeline id={pipeline_id}.")
                continue

            threshold = thresholds.get(pipeline_id, DEFAULT_STREAMING_LATENCY_THRESHOLD_HRS)
            logger.info(f"Streaming pipeline id={pipeline_id} latency={latency_hrs}h threshold={threshold}h")

            if latency_hrs >= threshold:
                latent_pipelines.append({
                    "pipeline_id":   pipeline_id,
                    "latency_in_hrs": latency_hrs,
                    "trace_id":      trace_id,
                    "source":        "streaming",
                })
    finally:
        conn.close()

    return latent_pipelines


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------

def _publish_sns_alert(latent_pipelines: list):
    """Publish an SNS alert listing all latent pipelines."""
    if env == "prd":
        topic_arn = SNS_TOPICS["prd"].format(region=region_name)
    else:
        topic_arn = SNS_TOPICS["non_prd"].format(region=region_name, env=env)

    message = {
        "AlarmName":       f"{env}-DataLake-Ingestion-Latent-Pipelines",
        "NewStateValue":   "ALARM",
        "StateChangeTime": datetime.now().isoformat(timespec="microseconds") + "Z",
        "Pipelines":       latent_pipelines,
        "state_message":   f"{len(latent_pipelines)} pipeline(s) are latent.",
    }

    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=f"Latent Pipelines Alert — {env}",
        Message=json.dumps(message, indent=4, default=str),
    )
    logger.info(f"SNS alert published: {response}")


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def handler(event, context):
    logger.info("Latent pipeline check started.")

    batch_latent     = check_pipeline_latency()
    streaming_latent = check_streaming_latency()
    all_latent       = batch_latent + streaming_latent

    if all_latent:
        logger.info(f"Found {len(all_latent)} latent pipeline(s). Publishing alert.")
        _publish_sns_alert(all_latent)
    else:
        logger.info("No latent pipelines detected.")

    logger.info("Latent pipeline check completed.")
