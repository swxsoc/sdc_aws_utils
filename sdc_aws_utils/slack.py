import os
import re
import time
from datetime import datetime
from typing import Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from sdc_aws_utils.logging import log
from sdc_aws_utils.config import parser


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


def is_file_manifest(file_name: str) -> bool:
    """
    Check if a file is a manifest file
    :param file_name: The name of the file to check
    :type file_name: str
    :return: True if the file is a manifest file, False otherwise
    :rtype: bool
    """

    # Get the basename of the file
    base_name = os.path.basename(file_name)

    # Check if the file starts with file_manifest prefix
    return base_name.startswith("file_manifest")


def generate_file_pipeline_message(file_path: str, alert_type: Optional[str] = None) -> str or tuple:
    """
    Function to generate file pipeline message
    """

    if "/" in file_path:
        file_path = file_path.split("/")[-1]

    if alert_type != "delete":
        # Get the file name
        alert = {
            "upload": f"File Uploaded to S3 - ( _{file_path}_ )",
            "sorted": f"File Sorted - ( _{file_path}_ )",
            "sorted_error": f"File Not Sorted - ( _{file_path}_ )",
            "processed": f"File Processed - ( _{file_path}_ )",
            "processed_error": f"File Not Processed - ( _{file_path}_ )",
            "download": f"File Downloaded - ( _{file_path}_ )",
            "download_error": f"File Not Downloaded - ( _{file_path}_ )",
            "error": f"File Upload Failed - ( _{file_path}_ )",
        }
        slack_message = f"Science File - ( _{file_path}_ )"

        if is_file_manifest(file_path):
            slack_message = f"Manifest File - ( _{file_path}_ )"
            with open(file_path, "r") as file:
                secondary_message = file.read()

            return (slack_message, secondary_message)

        if alert_type and alert_type in alert:
            slack_message = alert[alert_type]
        return slack_message


def send_slack_notification(
    slack_client: WebClient,
    slack_channel: str,
    slack_message: str,
    alert_type: Optional[str] = None,
    slack_max_retries: int = 5,
    slack_retry_delay: int = 5,
    thread_ts: Optional[str] = None,
) -> bool:
    log.debug(f"Sending Slack Notification to {slack_channel}")
    color = {
        "success": "#2ecc71",
        "error": "#ff0000",
        "delete": "#ff0000",
        "upload": "#3498db",
        "sorted": "#f39c12",
        "sorted_error": "#ff0000",
        "processed": "#2ecc71",
        "processed_error": "#f1c40f",
        "download": "#ffffff",
        "download_error": "#ff0000",
        "info": "#3498db",
        "warning": "#f1c40f",
        "orange": "#f39c12",
        "purple": "#9b59b6",
        "black": "#000000",
        "white": "#ffffff",
    }
    ct = datetime.now()
    ts = ct.strftime("%y-%m-%d %H:%M:%S")
    attachments = []
    # Check if slack_message is a tuple
    if isinstance(slack_message, tuple):
        text = slack_message[0]
        attachments = [
            {
                "color": color["purple"],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"{slack_message[1]}",
                        },
                    }
                ],
                "fallback": f"{slack_message[1]}",
            }
        ]
        pretext = slack_message[0]
    else:
        text = slack_message
        pretext = slack_message
        if alert_type:
            attachments = [
                {
                    "color": color[alert_type],
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"{slack_message}",
                            },
                        }
                    ],
                }
            ]
            text = f"`{ts}` -"

    for i in range(slack_max_retries):
        try:
            slack_client.chat_postMessage(
                channel=slack_channel,
                text=text,
                pretext=pretext,
                attachments=attachments,
                thread_ts=thread_ts,  # Include the thread_ts parameter
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


def have_same_keys_and_values(dicts, keys_to_check):
    """
    Check if a list of dictionaries have the same keys and values for the specified keys.

    :param dicts: List of dictionaries to check.
    :param keys_to_check: List or set of keys that you want to ensure are the same across dictionaries.
    :return: True if all dictionaries have the same keys and values for the specified keys, False otherwise.
    """

    return len({tuple((k, d[k]) for k in keys_to_check if k in d) for d in dicts}) == 1


def get_message_ts(slack_client: WebClient, slack_channel: str, science_filename: str) -> Optional[str]:
    try:
        response = slack_client.conversations_history(channel=slack_channel)
        messages = response["messages"]

        for message in messages:
            if "text" in message:
                slack_science_filename = parse_slack_message(message["text"])

                if not slack_science_filename:
                    continue
                try:
                    if "/" in slack_science_filename:
                        slack_science_filename = slack_science_filename.split("/")[-1]
                    slack_science_file = parser(slack_science_filename)
                except ValueError:
                    continue
                science_file = parser(science_filename)

                if have_same_keys_and_values(
                    [slack_science_file, science_file],
                    ["time", "mode", "test"],
                ):
                    return message["ts"]

        return None
    except SlackApiError as e:
        # Handle the exception according to your needs
        print(f"Error retrieving message_ts: {e}")
        return None


def parse_slack_message(message: str) -> str or None:
    # Search for the pattern with optional underscores surrounding the file path
    match = re.search(r"Science File - \( _?(.+?)_? \)", message)
    if match:
        return match.group(1)
    return None


def send_pipeline_notification(slack_client: WebClient, slack_channel: str, path: str, alert_type: str = None):
    try:
        if not is_file_manifest(path):
            slack_message = generate_file_pipeline_message(path)

            # Get ts of the slack message
            ts = get_message_ts(
                slack_client=slack_client,
                slack_channel=slack_channel,
                science_filename=path,  # Pass the message_ts instead of slack_message
            )

            if ts is None:
                slack_message = generate_file_pipeline_message(path)
                # Send Slack Notification about the event
                send_slack_notification(
                    slack_client=slack_client,
                    slack_channel=slack_channel,
                    slack_message=slack_message,
                )
                ts = get_message_ts(
                    slack_client=slack_client,
                    slack_channel=slack_channel,
                    science_filename=path,  # Pass the message_ts instead of slack_message
                )

            slack_message = generate_file_pipeline_message(path, alert_type=alert_type)

            # Send Slack Notification about the event within thread
            send_slack_notification(
                slack_client=slack_client,
                slack_channel=slack_channel,
                slack_message=slack_message,
                alert_type=alert_type,
                thread_ts=ts,
            )

    except Exception as e:
        log.error({"status": "ERROR", "message": e})
