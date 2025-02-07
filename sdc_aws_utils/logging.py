import os
import logging

# Configure logging
from swxsoc.util.logger import _init_log
from swxsoc.util.config import load_config, print_config

# Load user configuration
config = load_config()

log = _init_log(config=config)

# Format for log file entries log_file_format = %(asctime)s, %(origin)s, %(levelname)s, %(message)s
log_file_format = "%(asctime)s, %(origin)s, %(levelname)s, %(message)s"


def configure_logger():
    # Set log level
    environment = os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")
    log.propagate = True  # Ensures propagation to the root logger
    log.setLevel(logging.DEBUG)
    if environment == "PRODUCTION":
        log.setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
