import logging
import os

import pytest

from sdc_aws_utils.logging import configure_logger, log


def test_logger_level_development():
    os.environ["LAMBDA_ENVIRONMENT"] = "DEVELOPMENT"
    configure_logger()
    assert log.level == logging.DEBUG


def test_logger_level_production():
    os.environ["LAMBDA_ENVIRONMENT"] = "PRODUCTION"
    configure_logger()
    assert log.level == logging.INFO


def test_botocore_logger_level():
    botocore_logger = logging.getLogger("botocore")
    assert botocore_logger.level == logging.CRITICAL


def test_boto3_logger_level():
    boto3_logger = logging.getLogger("boto3")
    assert boto3_logger.level == logging.CRITICAL


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if "LAMBDA_ENVIRONMENT" in os.environ:
        del os.environ["LAMBDA_ENVIRONMENT"]
