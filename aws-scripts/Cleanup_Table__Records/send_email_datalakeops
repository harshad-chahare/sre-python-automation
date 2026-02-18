import boto3
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

sender = {"dev": "<email-non-prod>",
          "prod": "<email-prod>"
          }

account = {"dev": "Datalake Non-Prod",
           "prod": "Datalake Prod"}


def send_email_ses(size_cleared, user, env):
    msg = MIMEMultipart('mixed')
    msg["Subject"] = f"S3 Storage Cleanup - Cleared {size_cleared} bytes in {account.get(env)}"
    msg["From"] = sender.get(env)
    charset = "utf-8"
    body_html = """<html>
                           <body>
                            Hello,<br><br>
                            The automated process for cleaning up table record in our S3 storage has been successfully executed.<br><br>
                            User: """ + user + """
                            Total Storage Size Cleared:""" + str(size_cleared) + """ bytes.<br><br>
                            Attach file contains list of table records.<br><br>
                            Feel free to reach out if further details are needed.<br><br>
                            Thanks,<br><br>
                            EDP Operations
                            </body>
                            </html>"""

    with open(r'delete_table_details.csv', 'rb') as attachment:
        part = MIMEApplication(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename="delete_table_details.csv")

    htmlpart = MIMEText(body_html.encode(charset), 'html', charset)
    msg_body = MIMEMultipart('alternative')
    msg_body.attach(htmlpart)
    msg.attach(msg_body)
    msg.attach(part)
    ses_client = boto3.client("ses")
    try:
        ses_client.send_raw_email(Source=msg['From'], Destinations=[<distribution_email>],
                                  RawMessage={'Data': msg.as_string()})
    except Exception as e:
        print(e)
    else:
        print("Email sent! Message ID:"),
