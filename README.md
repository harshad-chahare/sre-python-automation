# sre-python-automation

This repository contains Python automation used to route data pipeline alerts to the correct owners and reduce manual on-call effort.

The code demonstrates production-style alert handling, ownership-based routing, and failure escalation using AWS services.

---

## Project: Pipeline Alert Routing Automation

### Overview

This project processes pipeline notification events and routes alerts based on predefined ownership metadata.

Alerts with clear producer action items are sent directly to data owners, while operational or failed notifications are escalated to the operations team.

---

### Components

#### etleap_notification_translator.py
An AWS Lambda function that:
- Consumes pipeline notification events
- Classifies alerts based on event type
- Builds alert payloads and action messages
- Routes alerts to producers or operations teams
- Avoids duplicate or non-actionable alerts

#### Alert_routing_utility.py
A shared utility module used by the Lambda to:
- Fetch routing metadata from DynamoDB
- Validate alert ownership and routing rules
- Send notifications via email or incident tools
- Trigger fallback escalation when routing fails

---

### Key Capabilities

- Ownership-based alert routing
- On-call noise reduction through automation
- Environment-aware alert handling
- Fallback escalation for failed notifications
- Configuration via environment variables

---

### Technologies Used

- Python
- AWS Lambda
- Amazon DynamoDB
- Amazon SNS
- Amazon SES
- Incident management APIs

---

### Use Case

The automation removes manual alert triage by:
- Sending actionable alerts directly to producers
- Escalating only required issues to operations
- Reducing repetitive on-call tasks and alert fatigue

---

