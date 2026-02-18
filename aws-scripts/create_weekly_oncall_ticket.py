import boto3
import requests
import json
import logging
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIGURATION — update these values to match your environment before running
# ---------------------------------------------------------------------------
SSM_PATHS = {
    "api_key":          "/your-team/oncall-tool/api-key",
    "api_id":           "/your-team/oncall-tool/api-id",
    "jira_token":       "/your-team/jira/service-account-key",
    "ticket_template":  "/your-team/weekly/oncall/ticket-template",
}

ONCALL_TOOL_CONFIG = {
    "schedule_url":  "https://api.your-oncall-tool.com/api-public/v2/team/{team_id}/oncall/schedule",
    "team_id":       "your-team-id",                    # replace with your on-call tool team ID
    "policy_name":   "Your Team - Critical",            # the on-call policy name to filter by
}

JIRA_CONFIG = {
    "base_url":        "https://your-org.atlassian.net",
    "epic_field":      "customfield_10014",             # update to your Jira instance's Epic Link field ID
    "epic_summary":    "Your Team On-call tracking - {year}",
    "project_key":     "YOUR_PROJECT_KEY",
}

SHIFT_LABELS = {
    # Map shift names to Jira labels — update to match your rotation names
    "Morning Shift":   ["team-squad-morning"],
    "Evening Shift":   ["team-squad-evening"],
    # default label used when shift name is not in the map:
    "default":         ["team-squad-oncall"],
}
# ---------------------------------------------------------------------------


# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm_client = boto3.client("ssm")


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def handler(event, context):
    main()


# ---------------------------------------------------------------------------
# SSM helpers
# ---------------------------------------------------------------------------

def fetch_ssm_value(key: str) -> str:
    """Fetch a single value from SSM Parameter Store."""
    return ssm_client.get_parameter(Name=key, WithDecryption=True)["Parameter"]["Value"]


# ---------------------------------------------------------------------------
# On-call schedule
# ---------------------------------------------------------------------------

def fetch_schedule_data(api_key_value: str, api_id_value: str) -> list:
    """
    Fetch the current on-call schedule from your on-call management tool
    and return a list of shift details for the configured policy.
    """
    url     = ONCALL_TOOL_CONFIG["schedule_url"].format(team_id=ONCALL_TOOL_CONFIG["team_id"])
    headers = {
        "X-VO-Api-Key": api_key_value,
        "X-VO-Api-Id":  api_id_value,
    }
    params   = {"daysForward": 7, "daysSkip": 0}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    schedules_data      = response.json()
    oncall_details      = []
    target_policy_name  = ONCALL_TOOL_CONFIG["policy_name"]

    for schedule in schedules_data.get("schedules", []):
        if schedule["policy"]["name"] == target_policy_name:
            for rotation in schedule.get("schedule", []):
                oncall_details.append({
                    "shift_name":                rotation["shiftName"],
                    "weekly_oncall_start_datetime": rotation["shiftRoll"],
                    "current_on_call":           rotation["rolls"][0]["onCallUser"]["username"],
                })

    return oncall_details


# ---------------------------------------------------------------------------
# Jira helpers
# ---------------------------------------------------------------------------

def _jira_headers() -> dict:
    """Build Jira auth headers using the service account token from SSM."""
    token = fetch_ssm_value(SSM_PATHS["jira_token"])
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


def get_current_year_epic_key() -> str:
    """
    Search Jira for the current year's on-call tracking Epic and return its key.
    Raises if the Epic is not found.
    """
    current_year  = datetime.now().year
    epic_summary  = JIRA_CONFIG["epic_summary"].format(year=current_year)
    jql_query     = f'summary ~ "{epic_summary}" AND issuetype = Epic'
    search_url    = f"{JIRA_CONFIG['base_url']}/rest/api/2/search?jql={jql_query}"

    logger.info(f"Searching for Jira Epic: {epic_summary}")
    response = requests.get(search_url, headers=_jira_headers())

    if response.status_code != 200:
        raise Exception(f"Jira search failed: {response.text}")

    results = response.json()
    if results["total"] == 0:
        raise Exception(f"No Epic found matching: {epic_summary}")

    epic_key = results["issues"][0]["key"]
    logger.info(f"Resolved Epic key: {epic_key}")
    return epic_key


def create_jira_ticket(labels: list, shift_name: str,
                       oncall_start: str, oncall_username: str):
    """
    Create a weekly on-call Jira ticket using the ticket template stored in SSM.
    The template is expected to be a valid JSON string.
    """
    headers = _jira_headers()

    # Load ticket template from SSM (stored as a JSON string)
    raw_template    = fetch_ssm_value(SSM_PATHS["ticket_template"])
    ticket_template = json.loads(raw_template)

    # Inject dynamic fields
    epic_key = get_current_year_epic_key()
    ticket_template["fields"][JIRA_CONFIG["epic_field"]] = epic_key
    ticket_template["fields"]["summary"]     = f"Weekly Oncall - {shift_name} | {oncall_start}"
    ticket_template["fields"]["description"] = (
        f"Weekly on-call for '{shift_name}' is scheduled from {oncall_start}."
    )
    ticket_template["fields"]["labels"]   = labels
    ticket_template["fields"]["assignee"] = {"name": oncall_username}

    create_url = f"{JIRA_CONFIG['base_url']}/rest/api/2/issue"
    response   = requests.post(create_url, data=json.dumps(ticket_template), headers=headers)

    if response.status_code == 201:
        issue_key = response.json()["key"]
        logger.info(f"Jira ticket created successfully: {issue_key}")
    else:
        logger.error(f"Failed to create Jira ticket: {response.status_code} - {response.text}")
        raise Exception(f"Jira ticket creation failed: {response.text}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    api_key_value = fetch_ssm_value(SSM_PATHS["api_key"])
    api_id_value  = fetch_ssm_value(SSM_PATHS["api_id"])
    schedule_data = fetch_schedule_data(api_key_value, api_id_value)

    for shift in schedule_data:
        shift_name    = shift["shift_name"]
        oncall_start  = shift["weekly_oncall_start_datetime"]
        oncall_user   = shift["current_on_call"]
        labels        = SHIFT_LABELS.get(shift_name, SHIFT_LABELS["default"])

        logger.info(f"Creating ticket for shift='{shift_name}' user='{oncall_user}'")
        create_jira_ticket(labels, shift_name, oncall_start, oncall_user)
