"""Centralized logging configuration for IoT ingestion pipeline.

This module provides logging setup functions that can be used by
any part of the application for consistent logging behavior.
"""

import logging
import sys


def setup_logging(
    log_file: str = "iot_ingestion.log",
    level: int = logging.INFO,
    name: str = None,
) -> logging.Logger:
    """Configure logging with file and console handlers.
    
    Args:
        log_file: Name of log file to write to
        level: Logging level (logging.DEBUG, logging.INFO, etc.)
        name: Logger name, defaults to module name
        
    Returns:
        Configured logger instance
    """
    if name is None:
        name = __name__

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers = []

    # Create formatters
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # File handler
    try:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except IOError as e:
        print(f"Warning: Could not create log file '{log_file}': {e}", file=sys.stderr)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def setup_root_logging(
    log_file: str = "iot_ingestion.log",
    level: int = logging.INFO,
) -> None:
    """Configure root logger for entire application.
    
    Args:
        log_file: Name of log file to write to
        level: Logging level (logging.DEBUG, logging.INFO, etc.)
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers
    root_logger.handlers = []

    # Create formatters
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # File handler
    try:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except IOError as e:
        print(f"Warning: Could not create log file '{log_file}': {e}", file=sys.stderr)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


def setup_test_logging(
    log_file: str = "test.log",
    level: int = logging.DEBUG,
) -> logging.Logger:
    """Configure logging specifically for testing/standalone mode.
    
    Args:
        log_file: Name of test log file
        level: Logging level (defaults to DEBUG for tests)
        
    Returns:
        Configured logger instance
    """
    return setup_logging(log_file=log_file, level=level, name=None)
