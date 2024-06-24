import os
import yaml

from sdc_aws_utils.logging import log
import swxsoc
from swxsoc.util import util

# Read config file
mission_config = swxsoc.config["mission"]

# Set up global variables
MISSION_NAME = mission_config["mission_name"]
INSTR_NAMES = mission_config["inst_names"]
MISSION_PKG = "swxsoc"

if os.getenv("AWS_REGION") is not None:
    TSD_REGION = os.getenv("AWS_REGION")
else:
    TSD_REGION = "us-east-1"

if os.getenv("SWXSOC_INCOMING_BUCKET") is not None:
    INCOMING_BUCKET = os.getenv("SWXSOC_INCOMING_BUCKET")
else:
    INCOMING_BUCKET = f"{MISSION_NAME}-incoming"

INSTR_PKG = [f"{MISSION_NAME}_{this_instr}" for this_instr in INSTR_NAMES]
INSTR_TO_BUCKET_NAME = {this_instr: f"{MISSION_NAME}-{this_instr}" for this_instr in INSTR_NAMES}
INSTR_TO_PKG = dict(zip(INSTR_NAMES, INSTR_PKG))

# Import parser and writer from util
parser = util.parse_science_filename
writer = util.create_science_filename


# Get Incoming Bucket Name
def get_incoming_bucket(environment: str = "DEVELOPMENT") -> str:
    """
    Get the incoming bucket name.
    :param environment: The environment
    :return: The incoming bucket name
    :rtype: str
    """

    return f"dev-{INCOMING_BUCKET}" if environment == "DEVELOPMENT" else INCOMING_BUCKET


# Get Instrument Bucket Names
def get_instrument_bucket(instrument: str, environment: str = "DEVELOPMENT") -> str:
    """
    Get the instrument bucket name.
    :param instrument: The instrument
    :param environment: The environment
    :return: The instrument bucket name
    :rtype: str
    """

    return (
        f"dev-{INSTR_TO_BUCKET_NAME[instrument]}" if environment == "DEVELOPMENT" else INSTR_TO_BUCKET_NAME[instrument]
    )


# Get all instrument bucket names
def get_all_instrument_buckets(environment: str = "DEVELOPMENT") -> list:
    """
    Get the instrument bucket name.
    :param instrument: The instrument
    :param environment: The environment
    :return: The instrument bucket name
    :rtype: str
    """

    return [
        f"dev-{INSTR_TO_BUCKET_NAME[instrument]}" if environment == "DEVELOPMENT" else INSTR_TO_BUCKET_NAME[instrument]
        for instrument in INSTR_NAMES
    ]
