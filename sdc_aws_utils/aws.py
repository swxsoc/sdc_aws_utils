import time
from datetime import datetime
from pathlib import Path
from typing import Callable

import boto3
import botocore

from sdc_aws_utils.logging import log


# Function to create boto3 s3 client session with credentials with try and except
def create_s3_client_session() -> type:
    """
    Create a boto3 s3 client session.
    :return: The boto3 s3 client session
    :rtype: type
    """
    try:
        s3_client = boto3.client("s3")
        return s3_client
    except Exception as e:
        log.error({"status": "ERROR", "message": e})
        raise e


# Function to create boto3 timestream client session with credentials with try and except
def create_timestream_client_session(region: str = "us-east-1") -> type:
    """
    Create a boto3 timestream client session.
    :return: The boto3 timestream client session
    :rtype: type
    """
    try:
        timestream_client = boto3.client("timestream-write", region_name=region)
        return timestream_client
    except Exception as e:
        log.error({"status": "ERROR", "message": e})
        raise e


def parse_file_key(file_path: str) -> str:
    """
    Parse the file key from the file path.
    :param file_path: The file path
    :type file_path: str
    :return: The file key
    :rtype: str
    """
    try:
        file_key = Path(file_path).name

        return file_key
    except Exception as e:
        log.error({"status": "ERROR", "message": e})
        raise e


def create_s3_file_key(science_file_parser: Callable, old_file_key: str) -> str:
    """
    Generate a full S3 file key in the format:
    {level}/{year}/{month}/{file_key}.
    :param file_key: The name of the file
    :type file_key: str
    :return: The full S3 file key
    :rtype: str
    """
    try:
        science_file = science_file_parser(old_file_key)
        reference_timestamp = datetime.strptime(science_file["time"].value, "%Y-%m-%dT%H:%M:%S.%f")

        # Get Year from science file 'time' key time object
        year = reference_timestamp.year
        month = reference_timestamp.month
        if month < 10:
            month = f"0{month}"

        new_file_key = f"{science_file['level']}/{year}/{month}/{old_file_key}"

        return new_file_key

    except KeyError as e:
        log.error({"status": "ERROR", "message": e})
        raise e


def object_exists(s3_client: type, bucket: str, file_key: str) -> bool:
    """
    Check if a file exists in the specified bucket.
    :param s3_client: The AWS session
    :type s3_client: str
    :param bucket: The name of the bucket
    :type bucket: str
    :param file_key: The name of the file
    :type file_key: str
    :return: True if the file exists, False if it does not
    :rtype: bool
    """

    try:
        s3_client.head_object(Bucket=bucket, Key=file_key)
        return True
    except botocore.exceptions.ClientError:
        return False


def download_file_from_s3(s3_client: type, source_bucket: str, file_key: str, parsed_file_key: str) -> Path:
    """
    Download a file from an S3 bucket.
    :param s3_client: The AWS session
    :type s3_client: str
    :param source_bucket: The name of the source bucket
    :type source_bucket: str
    :param file_key: The name of the file
    :type file_key: str
    :param parsed_file_key: The parsed name of the file
    :type parsed_file_key: str
    :return: The path to the downloaded file
    :rtype: Path
    """
    try:
        # Initialize S3 Client
        log.info(f"Downloading file {parsed_file_key} from {source_bucket}")

        # Download file to tmp directory
        s3_client.download_file(source_bucket, file_key, f"/tmp/{parsed_file_key}")

        log.debug(f"File {file_key} Successfully Downloaded")

        return Path(f"/tmp/{parsed_file_key}")

    except botocore.exceptions.ClientError as e:
        log.error({"status": "ERROR", "message": e})

        raise e


def upload_file_to_s3(s3_client: str, filename: str, destination_bucket: str, file_key: str) -> Path:
    """
    Upload a file to an S3 bucket.
    :param session: The AWS session
    :type session: str
    :param filename: The name of the file
    :type filename: str
    :param destination_bucket: The name of the destination bucket
    :type destination_bucket: str
    :param file_key: The name of the file
    :type file_key: str
    :return: The path to the uploaded file
    :rtype: Path
    """
    try:
        # Initialize S3 Client
        log.info(f"Uploading file {file_key} to {destination_bucket}")

        file_path = f"/tmp/{filename}"

        # Upload file to destination bucket
        s3_client.upload_file(file_path, destination_bucket, file_key)

        log.debug(f"File {file_key} Successfully Uploaded")

        return Path(file_path)

    except boto3.exceptions.S3UploadFailedError as e:
        log.error({"status": "ERROR", "message": e})

        raise e


def log_to_timestream(
    timestream_client: type,
    action_type: str,
    file_key: str,
    new_file_key: str = None,
    source_bucket: str = None,
    destination_bucket: str = None,
) -> None:
    """
    Log information to Timestream.
    :param session: The AWS session
    :type session: str
    :param action_type: The type of action performed
    :type action_type: str
    :param file_key: The name of the file
    :type file_key: str
    :param new_file_key: The new name of the file
    :type new_file_key: str
    :param source_bucket: The name of the source bucket
    :type source_bucket: str
    :param destination_bucket: The name of the destination bucket
    :type destination_bucket: str
    :return: None
    :rtype: None
    """
    log.debug("Logging to Timestream")
    CURRENT_TIME = str(int(time.time() * 1000))
    try:
        if not source_bucket and not destination_bucket:
            raise ValueError("A Source or Destination Buckets is required")

        # Write to Timestream
        timestream_client.write_records(
            DatabaseName="sdc_aws_logs",
            TableName="sdc_aws_s3_bucket_log_table",
            Records=[
                {
                    "Time": CURRENT_TIME,
                    "Dimensions": [
                        {"Name": "action_type", "Value": action_type},
                        {
                            "Name": "source_bucket",
                            "Value": source_bucket or "N/A",
                        },
                        {
                            "Name": "destination_bucket",
                            "Value": destination_bucket or "N/A",
                        },
                        {"Name": "file_key", "Value": file_key},
                        {
                            "Name": "new_file_key",
                            "Value": new_file_key or "N/A",
                        },
                    ],
                    "MeasureName": "timestamp",
                    "MeasureValue": str(datetime.utcnow().timestamp()),
                    "MeasureValueType": "DOUBLE",
                },
            ],
        )

        log.debug(f"File {file_key} Successfully Logged to Timestream")

    except Exception as e:
        log.error({"status": "ERROR", "message": e})

        raise e
