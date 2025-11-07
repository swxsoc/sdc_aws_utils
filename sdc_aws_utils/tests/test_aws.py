import os
from pathlib import Path

import boto3
import botocore
import pytest
from swxsoc.util import parse_science_filename
from swxsoc import _reconfigure
from moto import mock_s3, mock_timestreamwrite

from sdc_aws_utils.aws import (
    create_s3_client_session,
    create_s3_file_key,
    create_timestream_client_session,
    copy_file_in_s3,
    list_files_in_bucket,
    check_file_existence_in_target_buckets,
    download_file_from_s3,
    log_to_timestream,
    object_exists,
    parse_file_key,
    upload_file_to_s3,
    push_science_file,
    get_science_file,
)

# from lambda_function.file_processor.config import parser


SOURCE_BUCKET = "test-bucket"
DEST_BUCKET = "dest-bucket"
FILE_KEY = "test_file.txt"
NEW_FILE_KEY = "new_test_file.txt"
BAD_BUCKET = "bad-bucket"

parser = parse_science_filename


@mock_s3
def test_create_s3_client_session_success():
    # Call the function
    result = create_s3_client_session()

    # Check if the result is a boto3 s3 client
    assert result is not None


@mock_s3
def test_create_s3_client_session_failure(monkeypatch):
    # Mock boto3 client function to raise an exception
    monkeypatch.setattr(boto3, "client", lambda x: 1 / 0)

    # Call the function and expect an exception
    with pytest.raises(Exception):
        create_s3_client_session()


@mock_timestreamwrite
def test_create_timestream_client_session_success():
    # Call the function
    result = create_timestream_client_session()

    # Check if the result is a boto3 timestream client
    assert result is not None


@mock_timestreamwrite
def test_create_timestream_client_session_failure(monkeypatch):
    # Mock boto3 client function to raise an exception
    monkeypatch.setattr(boto3, "client", lambda x, region_name: 1 / 0)

    # Call the function and expect an exception
    with pytest.raises(Exception):
        create_timestream_client_session()


def test_parse_file_key_success():
    file_path = "/test_folder/test-file.txt"
    expected_file_key = "test-file.txt"

    # Call the function
    result = parse_file_key(file_path)

    # Check if the result is the expected file key
    assert result == expected_file_key


def test_parse_file_key_failure():
    file_path = None

    # Call the function and expect an exception
    with pytest.raises(Exception):
        parse_file_key(file_path)


def test_create_s3_file_key():
    """
    Test function that tests if the create_s3_file_key function
    returns the correct file key.
    """

    # Test L0 file
    test_valid_file_key = "swxsoc_EEA_l0_2022335-200137_v01.bin"

    valid_key = create_s3_file_key(parser, old_file_key=test_valid_file_key)

    assert valid_key == "l0/2022/12/01/swxsoc_EEA_l0_2022335-200137_v01.bin"

    # Test CDF file
    test_valid_file_key = "swxsoc_eea_ql_eventlist_20230205T000006_v1.0.01.cdf"

    valid_key = create_s3_file_key(parser, old_file_key=test_valid_file_key)

    assert valid_key == "ql/eventlist/2023/02/05/swxsoc_eea_ql_eventlist_20230205T000006_v1.0.01.cdf"

    # Test CDF file
    test_valid_file_key = "swxsoc_eea_l1_hk_20230205T000006_v1.0.01.cdf"

    valid_key = create_s3_file_key(parser, old_file_key=test_valid_file_key)

    assert valid_key == "l1/housekeeping/2023/02/05/swxsoc_eea_l1_hk_20230205T000006_v1.0.01.cdf"

    def test_parser(filename):
        return {"level": "l0"}

    # Test that the function raises a KeyError if the parser does not return a level
    try:
        create_s3_file_key(test_parser, old_file_key=test_valid_file_key)
    except KeyError as e:
        assert e is not None

    # Test unvalid file key
    test_invalid_file_key = "swxsoc_EEA_l0_2022335-200137_v01"

    try:
        create_s3_file_key(parser, old_file_key=test_invalid_file_key)
    except ValueError as e:
        assert e is not None


@mock_s3
def test_object_exists():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=SOURCE_BUCKET)
    s3_client.put_object(Bucket=SOURCE_BUCKET, Key="test_key", Body="test_data")

    assert object_exists(s3_client, SOURCE_BUCKET, "test_key")
    assert not object_exists(s3_client, SOURCE_BUCKET, "non_existent_key")


@mock_s3
def test_download_file_from_s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=SOURCE_BUCKET)
    s3_client.put_object(Bucket=SOURCE_BUCKET, Key="test_key", Body="test_data")

    local_path = download_file_from_s3(s3_client, SOURCE_BUCKET, "test_key", "downloaded_key")
    assert local_path == Path("/tmp/downloaded_key")
    assert local_path.is_file()

    # Try to download a non-existent file
    try:
        download_file_from_s3(s3_client, SOURCE_BUCKET, "non_existent_key", "downloaded_key")
    except botocore.exceptions.ClientError as e:
        assert e is not None


@mock_s3
def test_upload_file_to_s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=SOURCE_BUCKET)

    with open("/tmp/test_upload.txt", "w") as f:
        f.write("test_data")

    local_path = upload_file_to_s3(s3_client, "test_upload.txt", SOURCE_BUCKET, "uploaded_key")
    assert local_path == Path("/tmp/test_upload.txt")
    assert s3_client.get_object(Bucket=SOURCE_BUCKET, Key="uploaded_key")["Body"].read().decode("utf-8") == "test_data"
    # Try to upload a non-existent file
    try:
        upload_file_to_s3(s3_client, "bad_test_upload.txt", SOURCE_BUCKET, "uploaded_key")
    except FileNotFoundError as e:
        assert e is not None

    # Try to upload a file to a non-existent bucket
    try:
        upload_file_to_s3(s3_client, "test_upload.txt", BAD_BUCKET, "uploaded_key")
    except boto3.exceptions.S3UploadFailedError as e:
        assert e is not None


@mock_s3
def test_copy_file_in_s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=SOURCE_BUCKET)
    s3_client.create_bucket(Bucket=DEST_BUCKET)

    s3_client.put_object(Bucket=SOURCE_BUCKET, Key=FILE_KEY, Body="test data")

    # Test default behavior (move operation - delete_source_file=True)
    copy_file_in_s3(s3_client, SOURCE_BUCKET, DEST_BUCKET, FILE_KEY, NEW_FILE_KEY)

    # Check if the file exists in the destination
    assert s3_client.get_object(Bucket=DEST_BUCKET, Key=NEW_FILE_KEY)["Body"].read().decode() == "test data"

    # Check if the file has been deleted from the source (move operation)
    assert not object_exists(s3_client, SOURCE_BUCKET, FILE_KEY)

    # Test copy operation (delete_source_file=False)
    s3_client.put_object(Bucket=SOURCE_BUCKET, Key=FILE_KEY, Body="test data")
    copy_file_in_s3(s3_client, SOURCE_BUCKET, DEST_BUCKET, FILE_KEY, "another_new_file.txt", delete_source_file=False)

    # Check if the file exists in both locations (copy operation)
    assert s3_client.get_object(Bucket=DEST_BUCKET, Key="another_new_file.txt")["Body"].read().decode() == "test data"
    assert object_exists(s3_client, SOURCE_BUCKET, FILE_KEY)

    # Test error handling
    try:
        copy_file_in_s3(s3_client, SOURCE_BUCKET, DEST_BUCKET, "non_existent_key", NEW_FILE_KEY)
        assert False
    except botocore.exceptions.ClientError as e:
        assert e is not None


@mock_s3
def test_list_files_in_bucket():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=SOURCE_BUCKET)

    s3_client.put_object(Bucket=SOURCE_BUCKET, Key=FILE_KEY, Body="test data")

    files = list_files_in_bucket(s3_client, SOURCE_BUCKET)

    assert FILE_KEY in files


@mock_s3
def test_check_file_existence_in_target_buckets():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=SOURCE_BUCKET)
    s3_client.create_bucket(Bucket=DEST_BUCKET)

    s3_client.put_object(Bucket=DEST_BUCKET, Key=FILE_KEY, Body="test data")

    exists = check_file_existence_in_target_buckets(s3_client, FILE_KEY, SOURCE_BUCKET, [DEST_BUCKET])

    assert exists is True

    # Cleanup
    s3_client.delete_object(Bucket=DEST_BUCKET, Key=FILE_KEY)
    exists_after_delete = check_file_existence_in_target_buckets(s3_client, FILE_KEY, SOURCE_BUCKET, [DEST_BUCKET])

    assert exists_after_delete is False


@mock_timestreamwrite
def test_log_to_timestream():
    timestream_client = boto3.client("timestream-write", region_name="us-east-1")

    db_name = "sdc_aws_logs"
    table_name = "sdc_aws_s3_bucket_log_table"

    # Set up the database and table
    try:
        timestream_client.create_database(DatabaseName=db_name)
    except timestream_client.exceptions.ConflictException:
        pass

    try:
        timestream_client.create_table(DatabaseName=db_name, TableName=table_name)
    except timestream_client.exceptions.ConflictException:
        pass

    # No need to add assertions since we're only testing if the function can be called without exceptions
    log_to_timestream(
        timestream_client,
        "COPY",
        "test_file.txt",
        "L1/2022/09/test_file.txt",
        SOURCE_BUCKET,
        SOURCE_BUCKET,
        "PRODUCTION",
    )

    # Test passing minimum required arguments
    try:
        log_to_timestream(
            timestream_client,
            "COPY",
            "test_file.txt",
            "L1/2022/09/test_file.txt",
        )
        assert False
    except ValueError as e:
        assert e is not None

    # Test without passing any arguments
    try:
        log_to_timestream()
        assert False
    except TypeError as e:
        assert e is not None


def test_file_key_generation():
    # Setup
    filename = "swxsoc_EEA_l0_2023042-000000_v0.bin"

    # Exercise
    file_key = push_science_file(parse_science_filename, "swxsoc-eea", filename, True)

    # Verify
    assert file_key == "l0/2023/02/11/swxsoc_EEA_l0_2023042-000000_v0.bin"


@pytest.mark.parametrize("dry_run", [False, True])
@mock_s3
def test_s3_upload(dry_run):
    # Setup
    bucket = "swxsoc-eea"
    filename = "swxsoc_EEA_l0_2023042-000000_v0.bin"
    expected_key = "l0/2023/02/11/swxsoc_EEA_l0_2023042-000000_v0.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)

    # Create file in tmp directory to fix /tmp/swxsoc_EEA_l0_2023042-000000_v0.bin'
    with open(f"/tmp/{filename}", "w") as f:
        f.write("test")

    # Exercise
    file_key = push_science_file(parse_science_filename, bucket, filename, dry_run)

    # Verify
    assert file_key == expected_key
    if not dry_run:
        assert s3_client.head_object(Bucket=bucket, Key=expected_key)
    else:
        with pytest.raises(botocore.exceptions.ClientError):
            s3_client.head_object(Bucket=bucket, Key=expected_key)

    # Cleanup
    s3_client.delete_object(Bucket=bucket, Key=expected_key)
    s3_client.delete_bucket(Bucket=bucket)
    os.remove(f"/tmp/{filename}")


@pytest.mark.parametrize("dry_run", [False, True])
@mock_s3
def test_s3_upload(dry_run):
    # Setup
    bucket = "swxsoc-eea"
    filename = "swxsoc_EEA_l0_2023042-000000_v0.bin"
    expected_key = "l0/2023/02/11/swxsoc_EEA_l0_2023042-000000_v0.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)

    # Create file in tmp directory to fix /tmp/swxsoc_EEA_l0_2023042-000000_v0.bin'
    with open(f"/tmp/{filename}", "w") as f:
        f.write("test")

    # Exercise
    file_key = push_science_file(parse_science_filename, bucket, filename, dry_run)

    # Verify
    assert file_key == expected_key
    if not dry_run:
        assert s3_client.head_object(Bucket=bucket, Key=expected_key)
        try:
            file_key = push_science_file(parse_science_filename, bucket, filename, dry_run)
        except Exception as e:
            assert e is not None
    else:
        with pytest.raises(botocore.exceptions.ClientError):
            s3_client.head_object(Bucket=bucket, Key=expected_key)


def test_with_sdc_aws_file_path_set():
    # Setup
    parser_mock = lambda filename: filename
    bucket = "swxsoc-eea"
    filename = "swxsoc_EEA_l0_2023042-000000_v0.bin"
    expected_key = "l0/2023/02/11/swxsoc_EEA_l0_2023042-000000_v0.bin"

    os.environ["SDC_AWS_FILE_PATH"] = f"../test_data/{filename}"

    # Exercise
    file_key = push_science_file(parse_science_filename, bucket, filename, False)

    # Cleanup
    del os.environ["SDC_AWS_FILE_PATH"]

    # Verify
    assert file_key == expected_key


@mock_s3
def test_file_download_from_s3():
    # Setup
    bucket = "swxsoc-eea"
    file_key = "l0/2023/02/11/swxsoc_EEA_l0_2023042-000000_v0.bin"
    parsed_file_key = "swxsoc_EEA_l0_2023042-000000_v0.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=file_key, Body="test content")

    # Exercise
    file_path = get_science_file(bucket, file_key, parsed_file_key)

    # Verify
    assert file_path is not None
    assert file_path.name == parsed_file_key

    # Cleanup
    os.remove(file_path)


def test_file_download_with_env_var_set():
    # Setup
    os.environ["SDC_AWS_FILE_PATH"] = "/path/to/test-file.bin"

    # Exercise
    file_path = get_science_file("bucket", "file_key", "parsed_file_key")

    # Verify
    assert file_path == Path("/path/to/test-file.bin")

    # Cleanup
    del os.environ["SDC_AWS_FILE_PATH"]


@pytest.mark.parametrize("dry_run", [True, False])
@mock_s3
def test_dry_run_behavior(dry_run):
    # Setup
    bucket = "swxsoc-eea"
    file_key = "l0/2023/02/11/swxsoc_EEA_l0_2023042-000000_v0.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=file_key, Body="test content")

    # Exercise
    file_path = get_science_file(bucket, file_key, "swxsoc_EEA_l0_2023042-000000_v0.bin", dry_run)

    # Verify
    if dry_run:
        assert file_path is None
    else:
        assert file_path is not None
        assert file_path.name == "swxsoc_EEA_l0_2023042-000000_v0.bin"
        assert file_path.parent == Path("/tmp")


@mock_s3
def test_file_not_found_in_s3_bucket():
    # Setup
    bucket = "test-bucket"
    file_key = "nonexistent-file.bin"
    parsed_file_key = "parsed-nonexistent-file.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)

    # Exercise and Verify
    with pytest.raises(FileNotFoundError):
        get_science_file(bucket, file_key, parsed_file_key)
