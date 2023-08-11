import os
import logging

# Configure logging
log = logging.getLogger()

# Format for log file entries log_file_format = %(asctime)s, %(origin)s, %(levelname)s, %(message)s
log_file_format = "%(asctime)s, %(origin)s, %(levelname)s, %(message)s"


def configure_logger():
    # Set log level
    environment = os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")
    log.setLevel(logging.DEBUG)
    if environment == "PRODUCTION":
        log.setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
