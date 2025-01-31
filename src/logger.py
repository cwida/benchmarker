import logging
import os


def get_logger(name: str) -> logging.Logger:
    # set up with environment variable if available
    # can be set to DEBUG, INFO, WARNING, ERROR, CRITICAL
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    logging.basicConfig(level=log_level)
    return logging.getLogger(name)