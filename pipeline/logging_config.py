"""
Structured logging configuration for the hospital pipeline.

Sets up both console and file handlers with a consistent format,
making logs easy to read locally and easy to parse in a log aggregation
system (e.g. Loki, CloudWatch) without any additional changes.
"""

import logging
import os
from datetime import datetime


def get_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """
    Return a configured logger that writes to both console and a dated log file.

    Parameters
    ----------
    name : str
        Logger name — typically __name__ of the calling module.
    log_dir : str
        Directory where log files are written.

    Returns
    -------
    logging.Logger
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if the logger is already configured
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Console handler — INFO and above
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    # File handler — DEBUG and above, one file per pipeline run date
    log_file = os.path.join(log_dir, f"pipeline_{datetime.now():%Y-%m-%d}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger
