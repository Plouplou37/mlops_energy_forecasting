import json
import logging
from pathlib import Path

from feature_pipeline import settings


def get_logger(name: str) -> logging.Logger:
    """
    Template for getting a logger

    Args: 
        name: Name of the logger

    Return Logger
    """

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name)

    return logger
