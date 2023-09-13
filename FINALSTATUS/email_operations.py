import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

def send_email(sender_email, receiver_emails, smtp_server, subject, message, attachments=None):
    """
    Send an email with optional attachments.

    Args:
        sender_email (str): Sender's email address.
        receiver_emails (list): List of recipient email addresses.
        smtp_server (str): SMTP server address.
        subject (str): Email subject.
        message (str): Email message content in HTML format.
        attachments (list of str, optional): List of file paths to attach (default is None).
    """
    try:
        # Create a MIMEMultipart object
        email_message = MIMEMultipart()
        email_message["From"] = sender_email
        email_message["To"] = ", ".join(receiver_emails)
        email_message["Subject"] = subject

        # Attach the message (HTML content)
        email_message.attach(MIMEText(message, "html"))

        # Attach any specified files
        if attachments:
            for attachment in attachments:
                part = MIMEBase("application", "octet-stream")
                with open(attachment, "rb") as file:
                    part.set_payload(file.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={attachment}")
                email_message.attach(part)

        # Connect to the SMTP server
        server = smtplib.SMTP(smtp_server)
        server.starttls()

        # Log in to the sender's email account (if needed)
        # server.login(sender_email, password)

        # Send the email
        server.sendmail(sender_email, receiver_emails, email_message.as_string())

        # Close the SMTP server connection
        server.quit()

        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

if __name__ == "__main__":
    # Example usage of email_operations.py
    sender_email = "your_sender_email@your_domain.com"
    receiver_emails = ["user1@example.com", "user2@example.com"]
    smtp_server = "your_smtp_server"
    subject = "Sample Email Report"
    message = "<h1>This is a sample email report.</h1>"

    # Attach any files (optional)
    attachments = ["attachment1.txt", "attachment2.pdf"]

    send_email(sender_email, receiver_emails, smtp_server, subject, message, attachments)
