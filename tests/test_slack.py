import os
from unittest.mock import MagicMock, patch

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from sdc_aws_utils.slack import (
    get_slack_client,
    send_slack_notification,
)


def test_get_slack_client_success():
    # Set the SLACK_TOKEN environment variable
    os.environ["SLACK_TOKEN"] = "test-token"

    # Call the function
    result = get_slack_client(None)

    # Check if the result is a WebClient
    assert isinstance(result, WebClient)

    # Set the SLACK_TOKEN environment variable
    test_token = "test-token"

    # Call the function
    result = get_slack_client(slack_token=test_token)

    # Check if the result is a WebClient
    assert isinstance(result, WebClient)


def test_get_slack_client_failure():
    # Remove the SLACK_TOKEN environment variable
    os.environ.pop("SLACK_TOKEN", None)

    # Call the function
    result = get_slack_client(None)

    # Check if the result is None
    assert result is None


@patch("slack_sdk.WebClient.chat_postMessage")
def test_send_slack_notification_success(mock_chat_postMessage):
    # Create a MagicMock for the WebClient
    slack_client = MagicMock(spec=WebClient)

    # Call the function
    response = send_slack_notification(slack_client, "test-channel", "Test message", "success")

    # Check if chat_postMessage was called
    slack_client.chat_postMessage.assert_called()

    # Check if the response is not None
    assert response is not None

    # Set chat_postMessage to raise a SlackApiError
    slack_client.chat_postMessage.side_effect = SlackApiError("Error", {"Error": {"Code": "404"}})

    # Call the function
    try:
        response = send_slack_notification(slack_client, "test-channel", "Test message", "success", 1, 1)

    except SlackApiError as e:
        assert e is not None


@patch("slack_sdk.WebClient.chat_postMessage")
def test_send_slack_notification_failure(mock_chat_postMessage):
    # Create a MagicMock for the WebClient
    slack_client = MagicMock(spec=WebClient)

    # Set chat_postMessage to raise a SlackApiError
    slack_client.chat_postMessage.side_effect = SlackApiError("Error", {"Error": {"Code": "404"}})

    try:
        # Call the function and expect an error log
        send_slack_notification(slack_client, "test-channel", "Test message", "error", 2, 2)

    except SlackApiError:
        # Check if chat_postMessage was called
        slack_client.chat_postMessage.assert_called()


# Try to running slack notification not in mock mode to see if Token Exception is raised
def test_send_slack_notification_failure_no_token():
    try:
        slack_client = WebClient(token=None)
        # Call the function and expect an error log
        send_slack_notification(slack_client, "test-channel", "Test message", "error", 1, 1)

    except SlackApiError as e:
        assert e is not None
