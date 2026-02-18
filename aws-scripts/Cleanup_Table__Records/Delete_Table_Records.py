import re
import json
import uuid
import boto3
import getpass
import logging
import requests
import pandas as pd
from datetime import datetime
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# CONFIGURATION — replace these with your own resource names or load them
# from environment variables / a config file before running.
# ---------------------------------------------------------------------------
RESOURCE_MAP = {
    "dynamodb": {
        "datasets":                       {"dev": "dev-datasets-table",        "prod": "prod-datasets-table"},
        "reference_data":                 {"dev": "dev-reference-data-table",  "prod": "prod-reference-data-table"},
        "batch_audit":                    {"dev": "dev-batch-audit-table",      "prod": "prod-batch-audit-table"},
        "pipeline_config":                {"dev": "dev-pipeline-config-table",  "prod": "prod-pipeline-config-table"},
        "event_translator_pipeline_config":{"dev": "dev-et-pipeline-config",   "prod": "prod-et-pipeline-config"},
    },
    "s3_buckets": {
        "resource_files": {"dev": "dev-resource-files-bucket", "prod": "prod-resource-files-bucket"},
    },
    "sqs_queues": {
        "ingestion_reference_data": {"dev": "https://sqs.us-east-1.amazonaws.com/123456789/dev-queue",
                                     "prod": "https://sqs.us-east-1.amazonaws.com/123456789/prod-queue"},
    },
    "pipeline_domain": {
        # Add your pipeline API domains here; remove environments that don't support auto-delete
        "dev": "your-pipeline-api.example.com",
    },
}
# ---------------------------------------------------------------------------


class DataLakeCleanupHandler:
    """
    Cleans up all AWS resources associated with a data lake table across
    DynamoDB, S3, and AWS Glue.  Supports three ingestion workflow types:
      - v1  : legacy batch ingestion
      - v2  : pipeline-based ingestion
      - europa : dual-region streaming ingestion
    """

    def __init__(self, env: str, region: str):
        self.handler = None
        self.pipeline_id = None
        self.schema = None
        self.region = region
        self.env = env

        self.dynamodb_client = boto3.client("dynamodb", region_name=self.region)
        self.glue_client     = boto3.client("glue",     region_name=self.region)
        self.s3_resource     = boto3.resource("s3",     region_name=self.region)
        self.s3_client       = boto3.client("s3",       region_name=self.region)
        self.ssm_client      = boto3.client("ssm",      region_name=self.region)

        # SSM parameter paths for pipeline API credentials
        self.access_key_name = f"/pipeline/api/{env}/access_key"
        self.secret_key_name = f"/pipeline/api/{env}/secret_key"

        self.dyn_serializer   = TypeSerializer()
        self.dyn_deserializer = TypeDeserializer()

        # Per-instance logger to avoid handler bleed between instances
        self.logger = logging.getLogger(f"{__name__}_{region}_{id(self)}")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            self.logger.addHandler(logging.StreamHandler())
        self.logger.propagate = False

        self.todays_date  = datetime.today().strftime("%Y-%m-%d")
        self.timestamp    = int(pd.Timestamp.now().timestamp() * 1000)
        self.user         = getpass.getuser()
        self.run_id       = uuid.uuid4()
        self.total_size   = 0
        self.isr_input    = pd.DataFrame(columns=["Schema", "Dataset", "Database"])

    # ------------------------------------------------------------------
    # DynamoDB helpers
    # ------------------------------------------------------------------

    def _serialize(self, value):
        return self.dyn_serializer.serialize(value)

    def _deserialize(self, value):
        return self.dyn_deserializer.deserialize(value)

    def _deserialize_item(self, item: dict) -> dict:
        return {k: self._deserialize(v) for k, v in item.items()}

    def serialize_record(self, record: dict) -> dict:
        return {k: self._serialize(v) for k, v in record.items() if v != ""}

    # ------------------------------------------------------------------
    # Pipeline API credentials
    # ------------------------------------------------------------------

    def pipeline_creds(self):
        """Fetch pipeline API credentials from SSM Parameter Store."""
        try:
            params = self.ssm_client.get_parameters(
                Names=[self.access_key_name, self.secret_key_name],
                WithDecryption=True,
            )["Parameters"]
            param_map = {p["Name"]: p["Value"] for p in params}

            if self.access_key_name not in param_map or self.secret_key_name not in param_map:
                raise ValueError(
                    f"SSM parameters missing: {self.access_key_name}, {self.secret_key_name}"
                )
            return param_map[self.access_key_name], param_map[self.secret_key_name]
        except Exception as ex:
            self.logger.error(f"Failed to fetch pipeline credentials: {ex}")
            raise

    # ------------------------------------------------------------------
    # Workflow type detection
    # ------------------------------------------------------------------

    @staticmethod
    def get_workflow_type(env: str, database: str, table: str) -> str:
        """
        Determine the ingestion workflow type for a given table.
        Replace this stub with your own orchestration logic.
        Returns one of: 'v1', 'v2', 'europa'
        """
        raise NotImplementedError(
            "Implement get_workflow_type() using your orchestration library."
        )

    # ------------------------------------------------------------------
    # Glue
    # ------------------------------------------------------------------

    def delete_glue_record(self, database: str, table_name: str):
        self.logger.info(
            f"\nGlue record to delete — Database: {database}  Table: {table_name}  Region: {self.region}"
        )
        if input("Delete the above Glue record? (y/n): ").strip().lower() != "y":
            self.logger.info("Skipping Glue deletion.")
            return
        try:
            self.glue_client.delete_table(DatabaseName=database, Name=table_name)
            self.logger.info("Glue record deleted.")
        except ClientError as ex:
            code = ex.response["Error"]["Code"]
            if code == "EntityNotFoundException":
                self.logger.warning(f"Glue table not found: {database}.{table_name}. Skipping.")
            else:
                self.logger.error(f"Glue deletion error [{database}.{table_name}]: {ex}")

    # ------------------------------------------------------------------
    # Generic DynamoDB record deletion
    # ------------------------------------------------------------------

    def _confirm_and_delete_item(self, table_name: str, key: dict):
        """Prompt the user, then delete a single DynamoDB item."""
        if input(f"Delete record from '{table_name}'? (y/n): ").strip().lower() != "y":
            self.logger.info("Skipping DynamoDB deletion.")
            return
        try:
            self.dynamodb_client.delete_item(TableName=table_name, Key=key)
            self.logger.info(f"Deleted record from {table_name}.")
        except Exception as ex:
            self.logger.error(f"Error deleting from {table_name}: {ex}")

    def delete_records(self, table_name: str, record: dict):
        self.logger.info(f"\nRecord to delete — Table: {table_name}  ID: {record['id']}")
        key = {"id": self._serialize(record["id"])}
        self._confirm_and_delete_item(table_name, key)

    def select_record_to_delete(self, records: list, table_name: str):
        for idx, item in enumerate(records):
            self.logger.info(f"  [{idx}] {item['id']}")
        if input(f"Delete a record from '{table_name}'? (y/n): ").strip().lower() != "y":
            self.logger.info("Skipping.")
            return
        choice = int(input("Enter index of record to delete: "))
        self.delete_records(table_name, records[choice])

    # ------------------------------------------------------------------
    # Paginated DynamoDB scans
    # ------------------------------------------------------------------

    def _scan_by_contains(self, table_name: str, search: str) -> list:
        paginator = self.dynamodb_client.get_paginator("scan")
        pages = paginator.paginate(
            TableName=table_name,
            FilterExpression="contains(id, :id)",
            ExpressionAttributeValues={":id": self._serialize(search)},
            ProjectionExpression="#id",
            ExpressionAttributeNames={"#id": "id"},
        )
        return [self._deserialize_item(item) for page in pages for item in page["Items"]]

    def _scan_by_exact(self, table_name: str, search: str, extra_projection: str = "") -> list:
        projection = "#id" + (f", {extra_projection}" if extra_projection else "")
        expr_names = {"#id": "id"}
        if extra_projection:
            expr_names[f"#{extra_projection}"] = extra_projection
        paginator = self.dynamodb_client.get_paginator("scan")
        pages = paginator.paginate(
            TableName=table_name,
            FilterExpression="#id = :id",
            ExpressionAttributeValues={":id": self._serialize(search)},
            ProjectionExpression=projection,
            ExpressionAttributeNames=expr_names,
        )
        return [self._deserialize_item(item) for page in pages for item in page["Items"]]

    def get_data_pagination_dataset_record(self, table_name: str, search: str):
        values = self._scan_by_contains(table_name, search)
        if len(values) > 1:
            return self.select_record_to_delete(values, table_name)
        self.delete_records(table_name, values[0])
        return values[0]

    def get_data_pagination_reference_record(self, table_name: str, search: str):
        values = self._scan_by_exact(table_name, search)
        if len(values) > 1:
            return self.select_record_to_delete(values, table_name)
        self.delete_records(table_name, values[0])
        return values[0]

    def get_data_pagination_pipeline_configuration(self, table_name: str, search: str):
        values = self._scan_by_exact(table_name, search, extra_projection="pipeline_id")
        if len(values) > 1:
            return self.select_record_to_delete(values, table_name)
        self.pipeline_id = values[0]["pipeline_id"]
        self.delete_records(table_name, values[0])
        return values[0]

    # ------------------------------------------------------------------
    # High-level DynamoDB deletion methods
    # ------------------------------------------------------------------

    def delete_dynamo_dataset_record(self, dataset: str, database: str, table_name: str):
        dynamo_table = RESOURCE_MAP["dynamodb"]["datasets"][self.env]
        search = f"/ds/{dataset}/db/{database}/t/{table_name}/"
        try:
            result = self.get_data_pagination_dataset_record(dynamo_table, search)
            self.schema = re.split(r"/", str(result.values()), maxsplit=3)
            self.isr_input = self.isr_input._append(
                {"Schema": self.schema[2], "Dataset": dataset, "Database": database},
                ignore_index=True,
            )
            return dynamo_table, result
        except Exception as ex:
            self.logger.error(f"Error deleting dataset record [{database}.{table_name}]: {ex}")

    def delete_dynamo_reference_data(self, dataset: str, database: str, table_name: str):
        dynamo_table = RESOURCE_MAP["dynamodb"]["reference_data"][self.env]
        search = f"{dataset}|{database}|{table_name}"
        try:
            result = self.get_data_pagination_reference_record(dynamo_table, search)
            return dynamo_table, result
        except Exception as ex:
            self.logger.error(f"Error deleting reference record [{database}.{table_name}]: {ex}")

    def delete_dynamo_batch_audit_records(self, dataset: str, database: str, table_name: str):
        """Delete all batch audit records for a given pipeline (paginated query)."""
        dynamo_table = RESOURCE_MAP["dynamodb"]["batch_audit"][self.env]
        search_id    = f"{dataset}|{database}|{table_name}"
        self.logger.info(f"\nSearching batch audit records in '{dynamo_table}' — ID: '{search_id}'")
        try:
            paginator     = self.dynamodb_client.get_paginator("query")
            page_iterator = paginator.paginate(
                TableName=dynamo_table,
                KeyConditionExpression="id = :id_val",
                ExpressionAttributeValues={":id_val": self._serialize(search_id)},
            )
            all_items = [item for page in page_iterator for item in page.get("Items", [])]

            if not all_items:
                self.logger.info("No batch audit records found.")
                return

            self.logger.info(f"Found {len(all_items)} batch audit records.")
            if input(f"Delete all {len(all_items)} records from '{dynamo_table}'? (y/n): ").strip().lower() != "y":
                self.logger.info("Skipping batch audit deletion.")
                return

            deleted = 0
            for raw_item in all_items:
                item = self._deserialize_item(raw_item)
                try:
                    self.dynamodb_client.delete_item(
                        TableName=dynamo_table,
                        Key={
                            "id":         self._serialize(item["id"]),
                            "batch_time": self._serialize(item["batch_time"]),
                        },
                    )
                    deleted += 1
                except Exception as ex:
                    self.logger.error(f"Error deleting batch audit item (batch_time={item.get('batch_time')}): {ex}")

            self.logger.info(f"Deleted {deleted}/{len(all_items)} batch audit records.")
        except Exception as ex:
            self.logger.error(f"Error processing batch audit records: {ex}")

    def delete_dynamo_pipeline_config_record(self, dataset: str, database: str, table_name: str):
        """Delete a pipeline configuration record (get_item, not scan)."""
        dynamo_table = RESOURCE_MAP["dynamodb"]["pipeline_config"][self.env]
        search_id    = f"{dataset}|{database}|{table_name}"
        self.logger.info(f"\nSearching pipeline config in '{dynamo_table}' — ID: '{search_id}'")
        try:
            response = self.dynamodb_client.get_item(
                TableName=dynamo_table,
                Key={"id": self._serialize(search_id)},
            )
            if "Item" not in response:
                self.logger.info("No pipeline config record found.")
                return
            item = self._deserialize_item(response["Item"])
            self.logger.info(f"Found pipeline config record: {item.get('id')}")
            self._confirm_and_delete_item(dynamo_table, {"id": self._serialize(search_id)})
        except Exception as ex:
            self.logger.error(f"Error processing pipeline config record: {ex}")

    def delete_dynamo_pipeline_configuration_record(self, dataset: str, database: str, table_name: str):
        dynamo_table = RESOURCE_MAP["dynamodb"]["event_translator_pipeline_config"][self.env]
        search = f"{dataset}|{database}|{table_name}"
        try:
            self.get_data_pagination_pipeline_configuration(dynamo_table, search)
        except Exception as ex:
            self.logger.error(f"Error processing pipeline configuration record [{database}.{table_name}]: {ex}")

    # ------------------------------------------------------------------
    # S3
    # ------------------------------------------------------------------

    def calculate_folder_size(self, bucket_name: str, path: str):
        try:
            response = self.s3_client.list_objects(Bucket=bucket_name, Prefix=path)
            for obj in response.get("Contents", []):
                self.total_size += obj["Size"]
            self.logger.info(f"Calculated size — Bucket: {bucket_name}  Path: {path}")
        except Exception as ex:
            self.logger.error(f"Unable to calculate size [{bucket_name}/{path}]: {ex}")

    def delete_s3_files(self, bucket: str, path: str):
        try:
            self.logger.info(f"Deleting objects — Bucket: {bucket}  Path: {path}")
            self.s3_resource.Bucket(bucket).objects.filter(Prefix=path).delete()
            self.logger.info("S3 objects deleted.")
        except Exception as ex:
            self.logger.error(f"Failed to delete S3 objects [{bucket}/{path}]: {ex}")

    def delete_s3_qds_record(self, database: str, table: str, table_type: str):
        """
        Resolve the S3 location from Glue and delete the corresponding objects.
        Europa tables use the full path; v1/v2 strip the trailing partition component.
        """
        try:
            self.logger.info(f"Fetching S3 location — Database: {database}  Table: {table}  Region: {self.region}")
            response    = self.glue_client.get_table(DatabaseName=database, Name=table)
            s3_location = response["Table"]["StorageDescriptor"]["Location"]
            parts       = re.split(r"/", s3_location, maxsplit=3)
            bucket      = parts[2]
            path        = parts[3] if table_type == "europa" else parts[3].rsplit("/", 1)[0]

            self.logger.info(f"S3 path — Bucket: {bucket}  Path: {path}")
            if input("Delete the above S3 records? (y/n): ").strip().lower() != "y":
                self.logger.info("Skipping S3 deletion.")
                return
            self.calculate_folder_size(bucket, path)
            self.delete_s3_files(bucket, path)

        except ClientError as ex:
            code = ex.response["Error"]["Code"]
            if code == "EntityNotFoundException":
                self.logger.warning(f"Glue table not found: {database}.{table}. Skipping S3 deletion.")
            else:
                self.logger.error(f"S3 deletion error [{database}.{table}]: {ex}")
        except Exception as ex:
            self.logger.error(f"S3 deletion error [{database}.{table}]: {ex}")

    def delete_s3_resource_files_record(self, dataset: str, database: str, table_name: str):
        bucket = RESOURCE_MAP["s3_buckets"]["resource_files"][self.env]
        path   = f"{self.schema[2]}/datasets/{dataset}/{database}/{table_name}"
        self.logger.info(f"S3 resource files — Bucket: {bucket}  Path: {path}")
        if input("Delete the above S3 resource file records? (y/n): ").strip().lower() != "y":
            self.logger.info("Skipping S3 resource file deletion.")
            return
        self.calculate_folder_size(bucket, path)
        self.delete_s3_files(bucket, path)

    # ------------------------------------------------------------------
    # Pipeline API deletion
    # ------------------------------------------------------------------

    def pipeline_delete(self, database: str, table_name: str):
        pipeline_name = f"{{{database}}}__{{{table_name}}}"
        self.logger.info(f"Pipeline name: {pipeline_name}")

        if self.env not in RESOURCE_MAP["pipeline_domain"]:
            self.logger.info(
                f"No auto-delete domain configured for '{self.env}'. "
                f"Please delete pipeline '{pipeline_name}' manually."
            )
            return

        domain = RESOURCE_MAP["pipeline_domain"][self.env]
        try:
            auth = self.pipeline_creds()
            self.logger.info(f"Pipeline ID: {self.pipeline_id}  Name: {pipeline_name}")
            if input("Delete the above pipeline? (y/n): ").strip().lower() != "y":
                self.logger.info("Skipping pipeline deletion.")
                return
            url      = f"https://{domain}/api/v2/pipelines/{self.pipeline_id}"
            response = requests.delete(url, auth=auth)
            self.logger.info(f"Pipeline deletion — Status: {response.status_code}  Body: {response.text}")
        except Exception as ex:
            self.logger.error(f"Error deleting pipeline [{pipeline_name}]: {ex}")

    # ------------------------------------------------------------------
    # ISR trigger (SQS)
    # ------------------------------------------------------------------

    @staticmethod
    def _build_sqs_message(schema: str, dataset: str, database: str,
                           table: str = None, trace_id: str = None) -> str:
        trace_id   = trace_id or str(uuid.uuid4())
        event_type = "LedgerBatchCompleteEvent" if table else "ProcessInformationSchemaReferenceSnapshot"
        attributes = {
            "schema":      schema,
            "dataset":     dataset,
            "database":    database,
            "table":       table or "ALL",
            "event-type":  event_type,
        }
        return json.dumps({
            "Message": json.dumps({
                "content": {"trace-id": trace_id, "attributes": attributes}
            })
        })

    def _send_sqs_message(self, message: str, group_id: str, trace_id: str):
        sqs_client = boto3.client("sqs")
        sqs_client.send_message(
            QueueUrl=RESOURCE_MAP["sqs_queues"]["ingestion_reference_data"][self.env],
            MessageBody=message,
            MessageGroupId=group_id,
            MessageDeduplicationId=trace_id,
        )

    def trigger_isr_database(self):
        """Trigger an Information Schema Reference snapshot for each affected database."""
        details = self.isr_input.drop_duplicates(subset=["Database"])
        for _, row in details.iterrows():
            self.logger.info(
                f"\nTrigger ISR — Schema: {row['Schema']}  Dataset: {row['Dataset']}  Database: {row['Database']}"
            )
            if input("Trigger ISR for the above database? (y/n): ").strip().lower() != "y":
                self.logger.info("Skipping ISR trigger.")
                continue
            try:
                trace_id = str(uuid.uuid4())
                self._send_sqs_message(
                    self._build_sqs_message(row["Schema"], row["Dataset"], row["Database"], trace_id=trace_id),
                    group_id="database_snapshot",
                    trace_id=trace_id,
                )
                self.logger.info(f"ISR triggered — Trace ID: {trace_id}")
            except Exception as ex:
                self.logger.error(f"Error triggering ISR: {ex}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close_handlers(self):
        """Release logging handlers to prevent resource leaks."""
        if self.handler:
            self.handler.close()
            self.logger.removeHandler(self.handler)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print("Data Lake Table Cleanup Utility")
    env = input("\nEnter the environment (dev/prod): ").strip().lower()

    handler_east = DataLakeCleanupHandler(env, region="us-east-1")
    handler_west = DataLakeCleanupHandler(env, region="us-west-2")

    df = pd.read_csv("delete_table_details.csv")

    for _, row in df.iterrows():
        db_name    = row["DatabaseName"]
        table_name = row["TableName"]
        try:
            table_type = DataLakeCleanupHandler.get_workflow_type(env, db_name, table_name)
            handler_east.logger.info(f"Database: {db_name}  Table: {table_name}  Type: {table_type}")

            # Convention: database column is "<dataset>__<database>"
            dataset, database = re.split(r"__", db_name, maxsplit=1)

            if table_type == "v1":
                handler_east.delete_dynamo_dataset_record(dataset, database, table_name)
                handler_east.delete_s3_qds_record(db_name, table_name, table_type)
                handler_east.delete_glue_record(db_name, table_name)
                handler_east.delete_s3_resource_files_record(dataset, database, table_name)

            elif table_type == "v2":
                handler_east.delete_dynamo_dataset_record(dataset, database, table_name)
                handler_east.delete_dynamo_pipeline_configuration_record(dataset, database, table_name)
                handler_east.delete_dynamo_reference_data(dataset, database, table_name)
                handler_east.delete_s3_qds_record(db_name, table_name, table_type)
                handler_east.delete_s3_resource_files_record(dataset, database, table_name)
                handler_east.delete_glue_record(db_name, table_name)
                handler_east.pipeline_delete(db_name, table_name)

            elif table_type == "europa":
                # us-east-1 cleanup
                handler_east.logger.info("Starting us-east-1 cleanup.")
                handler_east.delete_dynamo_dataset_record(dataset, database, table_name)
                handler_east.delete_dynamo_reference_data(dataset, database, table_name)
                handler_east.delete_dynamo_batch_audit_records(dataset, database, table_name)
                handler_east.delete_dynamo_pipeline_config_record(dataset, database, table_name)
                handler_east.delete_s3_qds_record(db_name, table_name, table_type)
                handler_east.delete_s3_resource_files_record(dataset, database, table_name)
                handler_east.delete_glue_record(db_name, table_name)

                # us-west-2 cleanup
                handler_west.run_id = handler_east.run_id
                handler_west.logger.info("Starting us-west-2 cleanup.")
                handler_west.delete_s3_qds_record(db_name, table_name, table_type)
                handler_west.delete_glue_record(db_name, table_name)

            else:
                handler_east.logger.warning(f"Unknown table type '{table_type}' for {db_name}/{table_name}")

        except Exception as ex:
            handler_east.logger.error(f"Error processing {db_name}/{table_name}: {ex}")

    handler_east.trigger_isr_database()
    send_email_ses(dynamodb_records_east.total_size, dynamodb_records_east.user, env)
    handler_east.close_handlers()
    handler_west.close_handlers()


if __name__ == "__main__":
    main()
