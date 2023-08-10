import os
import logging

# Configure logging
log = logging.getLogger()

# Format for log file entries log_file_format = %(asctime)s, %(origin)s, %(levelname)s, %(message)s
log_file_format = "%(asctime)s, %(origin)s, %(levelname)s, %(message)s"

# Set log level
environment = os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")
log.setLevel(logging.DEBUG)
if environment == "PRODUCTION":
    log.setLevel(logging.INFO)
log_file = "/tmp/sdc_aws_processing_lambda.log"
fh = logging.FileHandler(log_file)
fh.setLevel(logging.INFO)
formatter = logging.Formatter(log_file_format)

# To remove boto3 noisy debug logging
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("boto3").setLevel(logging.CRITICAL)
