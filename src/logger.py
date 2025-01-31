import logging
import os


def get_logger(name: str) -> logging.Logger:
    # set up with environment variable if available
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    logging.basicConfig(level=log_level)
    return logging.getLogger(name)