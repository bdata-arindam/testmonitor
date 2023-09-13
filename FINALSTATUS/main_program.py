import json
import logging
import time
import requests
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from druid_api import fetch_druid_status
from status_report import create_table_from_clusters
from common_config import COMMON_CONFIG

def apply_color_formatting(table, email_thresholds):
    """
    Apply color formatting to the table based on the specified thresholds.

    Args:
        table (list of lists): The table data as a list of rows.
        email_thresholds (list of dict): List of threshold configurations for columns.

    Returns:
        list of lists: The formatted table with color coding.
    """
    colored_table = []

    for row in table:
        colored_row = []
        for i, cell in enumerate(row):
            threshold_config = email_thresholds[i]
            if threshold_config["enabled"] and cell > threshold_config["threshold"]:
                colored_row.append(f'<span style="color: red;">{cell}</span>')
            else:
                colored_row.append(cell)
        colored_table.append(colored_row)

    return colored_table

def send_email(table, email_config, execution_datetime, attach_csv):
    """
    Send an email report with an optional CSV attachment.

    Args:
        table (list of lists): The table data as a list of rows.
        email_config (dict): Email configuration settings.
        execution_datetime (str): Execution date and time.
        attach_csv (bool): Whether to attach a CSV file to the email.
    """
    column_names = email_config["column_names"]
    email_thresholds = email_config.get("email_thresholds", [])
    colored_table = apply_color_formatting(table, email_thresholds)

    html_table = "<table border='1' cellspacing='0'><tr>"
    for column_name in column_names:
        html_table += f"<th>{column_name}</th>"
    html_table += "</tr>"

    for row in colored_table:
        html_table += "<tr>"
        for item in row:
            html_table += f"<td>{item}</td>"
        html_table += "</tr>"

    html_table += "</table>"

    sender_email = email_config["sender_email"]
    receiver_emails = email_config["receiver_emails"]
    smtp_server = email_config["smtp_server"]

    subject = f"DRUID STATUS REPORT FOR CLUSTERS: {execution_datetime}"

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_emails)
    message["Subject"] = subject
    message.attach(MIMEText(html_table, "html"))

    # Attach CSV file if the option is enabled
    if attach_csv:
        csv_data = "\t".join(column_names) + "\n"
        for row in table:
            csv_data += "\t".join(map(str, row)) + "\n"

        csv_filename = f"druid_status_report_{execution_datetime}.csv"
        part = MIMEBase("application", "octet-stream")
        part.set_payload(csv_data.encode("utf-8"))
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={csv_filename}")
        message.attach(part)

    try:
        with smtplib.SMTP(smtp_server) as server:
            server.sendmail(sender_email, receiver_emails, message.as_string())
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")

def main():
    with open("clusters_config.json", "r") as config_file:
        cluster_configs = json.load(config_file)

    with open("email_config.json", "r") as email_config_file:
        email_config = json.load(email_config_file)

    log_file_path = email_config.get("log_file_path", "druid_status.log")
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    while True:
        try:
            execution_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            table = create_table_from_clusters(cluster_configs, email_config["column_names"])
            attach_csv = email_config.get("attach_csv", False)
            send_email(table, email_config, execution_datetime, attach_csv)
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")

        auto_restart = email_config.get("auto_restart", False)
        if not auto_restart:
            break

        email_intervals = email_config.get("email_intervals", 3600)
        time.sleep(email_intervals)

if __name__ == "__main__":
    main()