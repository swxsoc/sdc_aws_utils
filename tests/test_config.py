import os
import yaml


def test_read_config_file():
    from sdc_aws_utils import config

    # Read the config.yaml file
    with open("./config.yaml") as f:
        expected_config = yaml.safe_load(f)

    # Check if the config is loaded correctly
    assert config.config == expected_config

    # Check if the global variables are properly set
    assert config.MISSION_NAME == expected_config["MISSION_NAME"]
    assert config.INSTR_NAMES == expected_config["INSTR_NAMES"]
    assert config.MISSION_PKG == expected_config["MISSION_PKG"]
    assert config.TSD_REGION == expected_config["TSD_REGION"]
    assert config.INCOMING_BUCKET == expected_config["INCOMING_BUCKET"]

    # Check INSTR_PKG and INSTR_TO_BUCKET_NAME
    expected_instr_pkg = [f"{expected_config['MISSION_NAME']}_{instr}" for instr in expected_config["INSTR_NAMES"]]
    expected_instr_to_bucket_name = {
        instr: f"{expected_config['MISSION_NAME']}-{instr}" for instr in expected_config["INSTR_NAMES"]
    }

    assert config.INSTR_PKG == expected_instr_pkg
    assert config.INSTR_TO_BUCKET_NAME == expected_instr_to_bucket_name

    # Check INSTR_TO_PKG
    expected_instr_to_pkg = dict(zip(expected_config["INSTR_NAMES"], expected_instr_pkg))
    assert config.INSTR_TO_PKG == expected_instr_to_pkg

    # Check if the mission package is imported correctly
    mission_pkg = __import__(expected_config["MISSION_PKG"])
    assert config.mission_pkg == mission_pkg

    # Check if the parser is set correctly
    assert config.parser == mission_pkg.util.parse_science_filename


# Test non-existent file
def test_read_config_file_failure():
    # Set the config file path to a non-existent file
    none_existent = "non-existent-file!.yaml"

    # Set environment variable SDCscoped to this test
    os.environ["SDC_AWS_CONFIG_FILE_PATH"] = none_existent

    from sdc_aws_utils import config

    # Set environment variable scoped to this test

    # Call the function and expect an exception
    try:
        config.read_config_file()
        # If no exception is raised, fail the test
        assert False
    except Exception as e:
        assert isinstance(e, FileNotFoundError)


def test_get_incoming_bucket_development():
    from sdc_aws_utils import config

    bucket = config.get_incoming_bucket("DEVELOPMENT")
    assert bucket == "dev-swsoc-incoming"


def test_get_incoming_bucket_production():
    from sdc_aws_utils import config

    bucket = config.get_incoming_bucket("PRODUCTION")
    assert bucket == "swsoc-incoming"


def test_get_instrument_bucket_development():
    from sdc_aws_utils import config

    bucket = config.get_instrument_bucket("eea", "DEVELOPMENT")
    assert bucket == "dev-hermes-eea"


def test_get_instrument_bucket_production():
    from sdc_aws_utils import config

    bucket = config.get_instrument_bucket("eea", "PRODUCTION")
    assert bucket == "hermes-eea"


def test_get_all_instrument_buckets_development():
    from sdc_aws_utils import config

    buckets = config.get_all_instrument_buckets("DEVELOPMENT")
    expected_buckets = ["dev-hermes-eea", "dev-hermes-spani", "dev-hermes-merit", "dev-hermes-nemisis"]

    assert buckets.sort() == expected_buckets.sort()


def test_get_all_instrument_buckets_production():
    from sdc_aws_utils import config

    buckets = config.get_all_instrument_buckets("PRODUCTION")
    excepted_buckets = ["hermes-eea", "hermes-spani", "hermes-merit", "hermes-nemisis"]
    assert buckets.sort() == excepted_buckets.sort()
