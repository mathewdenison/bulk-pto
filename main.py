import base64
import json
import logging

from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging

# Import your Django model and any helper functions.
# Ensure your Cloud Function environment is set up to load your Django settings if needed.
from pto_update.models import PTO
from utils.dashboard_events import build_dashboard_payload

# Initialize Cloud Logging (Cloud Functions automatically integrates with Cloud Logging,
# but this call ensures that logs are formatted consistently)
cloud_logging_client = cloud_logging.Client()
cloud_logging_client.setup_logging()

logger = logging.getLogger("bulk_pto_lookup")
logger.setLevel(logging.INFO)

# GCP configuration – set your project and topic values
project_id = "hopkinstimesheetproj"
dashboard_topic = f"projects/{project_id}/topics/dashboard-queue"

# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

def bulk_pto_lookup(event, context):
    """
    Cloud Function to process a bulk PTO lookup message.
    Triggered from a message published to a Pub/Sub topic.
    
    Args:
        event (dict): The dictionary with data specific to this event. The Pub/Sub message data 
                      is in the 'data' field as a base64 encoded string.
        context (google.cloud.functions.Context): Metadata of triggering event.
    """
    try:
        # Decode the Pub/Sub message data from Base64
        if 'data' in event:
            data_str = base64.b64decode(event['data']).decode('utf-8')
            data = json.loads(data_str)
        else:
            logger.error("No data found in the Pub/Sub message.")
            return

        logger.info("Received bulk PTO lookup message.")
        logger.info(f"Bulk PTO lookup trigger payload: {data}")

        # Retrieve all PTO objects from the database.
        all_pto = PTO.objects.all()
        pto_list = [{"employee_id": p.employee_id, "pto_balance": p.balance} for p in all_pto]
        msg_str = f"Bulk PTO lookup: found {len(pto_list)} records."
        logger.info(msg_str)

        # Build a dashboard payload containing all PTO records.
        payload = build_dashboard_payload(
            "all",               # Special identifier for bulk messages.
            "bulk_pto_lookup",   # Type of the message
            msg_str,
            {"pto_records": pto_list}
        )

        # Publish the dashboard payload to the dashboard topic.
        future = publisher.publish(dashboard_topic, json.dumps(payload).encode("utf-8"))
        # Optionally, wait for the publish to complete.
        future.result()
        logger.info("Published bulk PTO lookup update to dashboard topic.")

        # If no exception is raised, Cloud Functions automatically acknowledges the message.
    except Exception as e:
        logger.exception(f"Error processing bulk PTO lookup message: {str(e)}")
        # Raising the exception will signal the function execution failure so Pub/Sub can re-deliver.
        raise
