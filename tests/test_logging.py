import logging

from sdc_aws_utils.logging import log


def test_log():
    log.info("Test log message")
    log.debug("Test log message")
    log.error("Test log message")
    log.warning("Test log message")
    log.critical("Test log message")

    assert log is not None
    assert isinstance(log, logging.Logger)
