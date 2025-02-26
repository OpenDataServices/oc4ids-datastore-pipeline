import datetime
import logging
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def _send_email(errors: list[dict[str, str]]) -> None:
    SMTP_HOST = os.environ["NOTIFICATIONS_SMTP_HOST"]
    SMTP_PORT = int(os.environ["NOTIFICATIONS_SMTP_PORT"])
    SMTP_SSL_ENABLED = int(os.environ["NOTIFICATIONS_SMTP_SSL_ENABLED"])
    SENDER_EMAIL = os.environ["NOTIFICATIONS_SENDER_EMAIL"]
    RECEIVER_EMAIL = os.environ["NOTIFICATIONS_RECEIVER_EMAIL"]
    logger.info(f"Sending email to {RECEIVER_EMAIL}")
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        if SMTP_SSL_ENABLED:
            context = ssl.create_default_context()
            server.starttls(context=context)
        message = MIMEMultipart()
        current_time = datetime.datetime.now(datetime.UTC)
        message["Subject"] = f"Errors in OC4IDS Datastore Pipeline run: {current_time}"
        message["From"] = SENDER_EMAIL
        message["To"] = RECEIVER_EMAIL

        html = f"""\
        <h1>Errors in OC4IDS Datastore Pipeline run</h1>
        <p>The pipeline completed at {current_time}.</p>
        <p>Please see errors for each dataset below:</p>
        {"".join([
            f"""
            <h2>{error["dataset_id"]}</h2>
            <p>Source URL: <code>{error["source_url"]}</code></p>
            <pre><code>{error["message"]}</code></pre>
            """
            for error in errors
        ])}
        """
        message.attach(MIMEText(html, "html"))

        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, message.as_string())


def send_notification(errors: list[dict[str, str]]) -> None:
    NOTIFICATIONS_ENABLED = bool(int(os.environ.get("NOTIFICATIONS_ENABLED", "0")))
    if NOTIFICATIONS_ENABLED:
        _send_email(errors)
    else:
        logger.info("Notifications are disabled, skipping")
