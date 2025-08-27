"""
Logging setup and configuration utilities.

This module provides centralized logging configuration using loguru
with support for file rotation, structured logging, and multiple outputs.
"""

from ..config.models import LoggingConfig
from ...core.interfaces.lifecycle import IComponent
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Union

loguru_logger: Any = None
try:
    from loguru import logger as loguru_logger
    LOGURU_AVAILABLE = True
except ImportError:
    LOGURU_AVAILABLE = False


def setup_logging(config: LoggingConfig) -> None:
    """
    Setup application logging with the given configuration.

    Args:
        config: Logging configuration
    """
    if LOGURU_AVAILABLE:
        _setup_loguru_logging(config)
    else:
        _setup_standard_logging(config)


def _setup_loguru_logging(config: LoggingConfig) -> None:
    """Setup logging using loguru."""
    # Remove default handler
    loguru_logger.remove()

    # Create log directory
    log_dir = Path(config.log_directory)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Console logging
    if config.console_enabled:
        loguru_logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                   "<level>{message}</level>",
            level=config.level,
            colorize=True,
            backtrace=True,
            diagnose=True
        )

    # File logging
    if config.file_enabled:
        # Main application log
        loguru_logger.add(
            log_dir / "app.log",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
                   "{name}:{function}:{line} - {message}",
            level=config.level,
            rotation=config.max_file_size,
            retention=config.backup_count,
            compression="zip",
            backtrace=True,
            diagnose=True
        )


def _setup_standard_logging(config: LoggingConfig) -> None:
    """Setup logging using standard library."""
    # Create log directory
    log_dir = Path(config.log_directory)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, config.level.upper()),
        format=config.format,
        handlers=[]
    )

    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Console handler
    if config.console_enabled:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(getattr(logging, config.level.upper()))
        console_formatter = logging.Formatter(config.format)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    # File handler
    if config.file_enabled:
        file_handler = logging.FileHandler(log_dir / "app.log")
        file_handler.setLevel(getattr(logging, config.level.upper()))
        file_formatter = logging.Formatter(config.format)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)


class LoggingManager(IComponent):
    """
    Logging manager for runtime logging configuration.

    This class manages logging configuration and provides utilities
    for structured logging throughout the application.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = LoggingConfig(**config)
        self._started = False
        self._logger = logging.getLogger(__name__)

    @property
    def name(self) -> str:
        """Get component name."""
        return "LoggingManager"

    @property
    def version(self) -> str:
        """Get component version."""
        return "1.0.0"

    async def start(self) -> None:
        """Start the logging manager."""
        if self._started:
            return

        setup_logging(self._config)
        self._started = True

        self._logger.info("Logging manager started")
        self._logger.info(f"Log level: {self._config.level}")
        self._logger.info(f"Log directory: {self._config.log_directory}")

    async def stop(self) -> None:
        """Stop the logging manager."""
        if not self._started:
            return

        self._logger.info("Logging manager stopped")
        self._started = False

    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the logging manager."""
        self._config = LoggingConfig(**config)
        if self._started:
            # Reconfigure logging
            setup_logging(self._config)
            self._logger.info("Logging configuration updated")

    async def check_health(self) -> Dict[str, Any]:
        """Check logging manager health."""
        log_dir = Path(self._config.log_directory)

        return {
            'healthy': True,
            'status': 'running' if self._started else 'stopped',
            'details': {
                'log_level': self._config.level,
                'log_directory': str(log_dir),
                'log_directory_exists': log_dir.exists(),
                'console_enabled': self._config.console_enabled,
                'file_enabled': self._config.file_enabled,
                'max_file_size': self._config.max_file_size,
                'backup_count': self._config.backup_count
            }
        }

    def get_logger(self, name: str) -> Any:
        """
        Get a logger instance for the given name.

        Args:
            name: Logger name

        Returns:
            Logger instance
        """
        if LOGURU_AVAILABLE:
            return loguru_logger.bind(name=name)
        else:
            return logging.getLogger(name)

    def log_access(self, message: str, **kwargs: Any) -> None:
        """
        Log an access message.

        Args:
            message: Access log message
            **kwargs: Additional context
        """
        if LOGURU_AVAILABLE:
            loguru_logger.bind(access_log=True, **kwargs).info(message)
        else:
            self._logger.info(f"ACCESS: {message}")

    def log_performance(self, message: str, **kwargs: Any) -> None:
        """
        Log a performance message.

        Args:
            message: Performance log message
            **kwargs: Additional context
        """
        if LOGURU_AVAILABLE:
            loguru_logger.bind(performance_log=True, **kwargs).info(message)
        else:
            self._logger.info(f"PERFORMANCE: {message}")

    def log_error(self, message: str, error: Optional[Exception] = None, **kwargs: Any) -> None:
        """
        Log an error message with optional exception.

        Args:
            message: Error message
            error: Exception instance
            **kwargs: Additional context
        """
        if error:
            if LOGURU_AVAILABLE:
                loguru_logger.bind(**kwargs).exception(f"{message}: {error}")
            else:
                self._logger.exception(f"{message}: {error}")
        else:
            if LOGURU_AVAILABLE:
                loguru_logger.bind(**kwargs).error(message)
            else:
                self._logger.error(message)

    def log_structured(self, level: str, message: str, **kwargs: Any) -> None:
        """
        Log a structured message with additional context.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Log message
            **kwargs: Additional structured data
        """
        if LOGURU_AVAILABLE:
            log_func = getattr(loguru_logger, level.lower(),
                               loguru_logger.info)
            log_func(message, **kwargs)
        else:
            log_func = getattr(self._logger, level.lower(), self._logger.info)
            log_func(f"{message} - {kwargs}")
