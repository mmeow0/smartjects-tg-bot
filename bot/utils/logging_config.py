import logging
import sys
from typing import Optional
from pathlib import Path

def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    log_file: Optional[str] = None,
    console_output: bool = True
) -> logging.Logger:
    """
    Setup centralized logging configuration for the entire application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom format string for log messages
        log_file: Optional file path to write logs to
        console_output: Whether to output logs to console

    Returns:
        Configured logger instance
    """

    # Default format if none provided
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(format_string)

    # Add console handler if requested
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # Add file handler if log file specified
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    return root_logger

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    Inherits configuration from root logger.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)

def setup_bot_logging():
    """
    Setup logging specifically for the Telegram bot using config values.
    This should be called early in the application startup.
    """
    try:
        from bot.config import LOG_LEVEL, LOG_FORMAT, LOG_TO_FILE, LOG_FILE_PATH

        # Setup logging with config values
        setup_logging(
            level=LOG_LEVEL,
            format_string=LOG_FORMAT,
            log_file=LOG_FILE_PATH if LOG_TO_FILE else None,
            console_output=True
        )

        # Get logger for this module
        logger = get_logger(__name__)
        logger.info(f"Logging configured with level: {LOG_LEVEL}")
        if LOG_TO_FILE:
            logger.info(f"Logging to file: {LOG_FILE_PATH}")

    except ImportError as e:
        # Fallback if config import fails
        setup_logging(level="INFO")
        logger = get_logger(__name__)
        logger.warning(f"Could not import config, using default logging: {e}")

def setup_script_logging(level: str = "INFO"):
    """
    Setup logging for standalone scripts (outside the bot module).

    Args:
        level: Logging level to use
    """
    setup_logging(
        level=level,
        console_output=True
    )

class LoggingMixin:
    """
    Mixin class that provides logging functionality to any class.
    """

    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class."""
        if not hasattr(self, '_logger'):
            self._logger = get_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        return self._logger

def log_function_call(func):
    """
    Decorator to log function calls with arguments and return values.
    Useful for debugging.
    """
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)

        # Log function entry
        logger.debug(f"Calling {func.__name__} with args={args}, kwargs={kwargs}")

        try:
            result = func(*args, **kwargs)
            logger.debug(f"{func.__name__} returned: {result}")
            return result
        except Exception as e:
            logger.error(f"{func.__name__} raised {type(e).__name__}: {e}")
            raise

    return wrapper

def suppress_external_loggers(level: str = "WARNING"):
    """
    Suppress noisy external library loggers.

    Args:
        level: Minimum level for external loggers
    """
    external_loggers = [
        'aiogram',
        'aiohttp',
        'supabase',
        'httpx',
        'urllib3',
        'requests'
    ]

    numeric_level = getattr(logging, level.upper(), logging.WARNING)

    for logger_name in external_loggers:
        logging.getLogger(logger_name).setLevel(numeric_level)
