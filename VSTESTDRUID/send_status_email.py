import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time

def send_email(config, metrics):
    """
    Sends a status email with collected metrics.

    Args:
        config (dict): The configuration dictionary containing the following keys:
            - 'smtp_user': Sender's email address (from).
            - 'recipient_email': Recipient's email address (to).
            - 'smtp_server': SMTP server address.
            - 'smtp_port' (optional): SMTP server port (default is 587).
            - 'smtp_password': SMTP server password.
            - 'cluster_name': Name of the Druid cluster for the subject.
        metrics (dict): A dictionary containing metrics data to include in the email body.
    """
    sender_email = config.get('smtp_user')
    recipient_email = config.get('recipient_email')
    smtp_server = config.get('smtp_server')
    smtp_port = config.get('smtp_port', 587)
    smtp_password = config.get('smtp_password')
    cluster_name = metrics.get('Cluster Name')
    subject = f"Druid Status - {cluster_name} - {time.strftime('%Y-%m-%d %H:%M:%S')}"

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = subject

    # Create the email content with metrics as part of the body
    email_body = ""
    for key, value in metrics.items():
        email_body += f"{key}: {value}\n"

    message.attach(MIMEText(email_body, 'plain'))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, smtp_password)
            server.sendmail(sender_email, recipient_email, message.as_string())
        print("Email sent successfully")
    except smtplib.SMTPException as e:
        # Handle email sending errors
        print(f"Error sending email: {str(e)}")
