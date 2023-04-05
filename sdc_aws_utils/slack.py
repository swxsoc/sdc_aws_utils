import os
import time
from datetime import datetime

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from sdc_aws_utils.logging import log


def get_slack_client(slack_token: str) -> WebClient:
    """
    Initialize a Slack client using the provided token.
    :param slack_token: The Slack API token
    :type slack_token: str
    :return: The initialized Slack WebClient
    :rtype: WebClient
    """
    # If the slack token is not set, try to get it from the environment
    if not slack_token:
        slack_token = os.environ.get("SLACK_TOKEN")

    # If the slack token is still not set, return None
    if not slack_token:
        log.error(
            {
                "status": "ERROR",
                "message": "Slack Token is not set",
            }
        )
        return None

    # Initialize the slack client
    slack_client = WebClient(token=slack_token)

    return slack_client


def send_slack_notification(
    slack_client: WebClient,
    slack_channel: str,
    slack_message: str,
    alert_type: str = "success",
    slack_max_retries: int = 5,
    slack_retry_delay: int = 5,
) -> bool:
    log.debug(f"Sending Slack Notification to {slack_channel}")
    color = {
        "success": "#2ecc71",
        "error": "#ff0000",
    }
    ct = datetime.now()
    ts = ct.strftime("%y-%m-%d %H:%M:%S")

    for i in range(slack_max_retries):
        try:
            slack_client.chat_postMessage(
                channel=slack_channel,
                text=f"{ts} - {slack_message}",
                attachments=[
                    {
                        "color": color[alert_type],
                        "blocks": [
                            {
                                "type": "section",
                                "text": {
                                    "type": "plain_text",
                                    "text": f"{ts} - {slack_message}",
                                },
                            }
                        ],
                    }
                ],
            )

            log.debug(f"Slack Notification Successfully Sent to {slack_channel}")

            return True

        except SlackApiError as e:
            if i < slack_max_retries - 1:  # If it's not the last attempt, wait and try again
                log.warning(
                    f"Error sending Slack Notification (attempt {i + 1}): {e}."
                    f"Retrying in {slack_retry_delay} seconds..."
                )
                time.sleep(slack_retry_delay)
            else:  # If it's the last attempt, log the error and exit the loop
                log.error(
                    {
                        "status": "ERROR",
                        "message": f"Error sending Slack Notification (attempt {i + 1}): {e}",
                    }
                )
                raise e
