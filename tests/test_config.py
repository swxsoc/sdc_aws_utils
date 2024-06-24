import os
import yaml


def test_get_incoming_bucket_development():
    from sdc_aws_utils import config

    bucket = config.get_incoming_bucket("DEVELOPMENT")
    assert bucket == "dev-swxsoc-incoming"


def test_get_incoming_bucket_production():
    from sdc_aws_utils import config

    bucket = config.get_incoming_bucket("PRODUCTION")
    assert bucket == "swxsoc-incoming"


def test_get_instrument_bucket_development():
    from sdc_aws_utils import config

    bucket = config.get_instrument_bucket("eea", "DEVELOPMENT")
    assert bucket == "dev-swxsoc-eea"


def test_get_instrument_bucket_production():
    from sdc_aws_utils import config

    bucket = config.get_instrument_bucket("eea", "PRODUCTION")
    assert bucket == "swxsoc-eea"


def test_get_all_instrument_buckets_development():
    from sdc_aws_utils import config

    buckets = config.get_all_instrument_buckets("DEVELOPMENT")
    expected_buckets = ["dev-swxsoc-eea", "dev-swxsoc-spani", "dev-swxsoc-merit", "dev-swxsoc-nemisis"]

    assert buckets.sort() == expected_buckets.sort()


def test_get_all_instrument_buckets_production():
    from sdc_aws_utils import config

    buckets = config.get_all_instrument_buckets("PRODUCTION")
    excepted_buckets = ["swxsoc-eea", "swxsoc-spani", "swxsoc-merit", "swxsoc-nemisis"]
    assert buckets.sort() == excepted_buckets.sort()
