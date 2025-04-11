# main.py
import os
import json
import logging
import base64

from flask import Request
from sqlmodel import select
from google.cloud import pubsub_v1

# Import the shared code from your PyPI package
from pto-common.database import init_db, get_session
from pto-common.models import PTO
from pto-common.utils.dashboard_events import build_dashboard_payload

# Configure logging
logger = logging.getLogger("bulk_pto_lookup")
logger.setLevel(logging.INFO)

# Pub/Sub configuration – set these via environment variables or default values
PROJECT_ID = os.environ.get("PROJECT_ID", "hopkinstimesheetproj")
DASHBOARD_TOPIC = f"projects/{PROJECT_ID}/topics/dashboard-queue"
publisher = pubsub_v1.PublisherClient()

# Initialize the database on cold start
init_db()

def bulk_pto_lookup(request: Request):
    """
    HTTP-triggered Cloud Function to perform a bulk PTO lookup,
    build a dashboard payload and publish it to a Pub/Sub topic.
    """
    try:
        if request.method != "POST":
            return ("Method Not Allowed", 405)

        data = request.get_json(silent=True) or {}
        logger.info("Received bulk PTO lookup request.")
        logger.info(f"Payload: {data}")

        # Obtain a database session
        session = get_session()

        # Query all PTO records from the database
        pto_records = session.exec(select(PTO)).all()
        pto_list = [{"employee_id": p.employee_id, "pto_balance": p.balance} for p in pto_records]
        msg_str = f"Bulk PTO lookup: found {len(pto_list)} records."
        logger.info(msg_str)

        # Build the dashboard payload using the shared utility function
        payload = build_dashboard_payload(
            "all",               # Identifier for a bulk lookup
            "bulk_pto_lookup",   # The event type
            msg_str,
            {"pto_records": pto_list}
        )

        # Publish the payload to the dashboard topic on Pub/Sub
        future = publisher.publish(DASHBOARD_TOPIC, json.dumps(payload).encode("utf-8"))
        future.result()  # Optionally wait until the publishing is complete
        logger.info("Published dashboard update to Pub/Sub.")

        return (json.dumps({"status": "success", "message": msg_str}), 200)

    except Exception as e:
        logger.exception(f"Error during bulk PTO lookup: {str(e)}")
        return (json.dumps({"status": "error", "message": str(e)}), 500)
