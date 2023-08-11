import os
import yaml

from sdc_aws_utils.logging import log


def read_config_file():
    # Get the config file path from environment variable
    config_file_path = os.getenv("SDC_AWS_CONFIG_FILE_PATH", "./config.yaml")
    try:
        with open(config_file_path) as f:
            config = yaml.safe_load(f)
            log.debug("config.yaml loaded successfully")
            return config
    except FileNotFoundError:
        log.error("config.yaml not found")
        raise FileNotFoundError("config.yaml not found")


# Read config file
config = read_config_file()

# Set up global variables
MISSION_NAME = config["MISSION_NAME"]
INSTR_NAMES = config["INSTR_NAMES"]
MISSION_PKG = config["MISSION_PKG"]
TSD_REGION = config["TSD_REGION"]
INCOMING_BUCKET = config["INCOMING_BUCKET"]
INSTR_PKG = [f"{MISSION_NAME}_{this_instr}" for this_instr in INSTR_NAMES]
INSTR_TO_BUCKET_NAME = {this_instr: f"{MISSION_NAME}-{this_instr}" for this_instr in INSTR_NAMES}
INSTR_TO_PKG = dict(zip(INSTR_NAMES, INSTR_PKG))


# Import logging and util from mission package
mission_pkg = __import__(MISSION_PKG)
parser = mission_pkg.util.parse_science_filename


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
