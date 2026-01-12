"""
Logging Module
"""

import logging
import sys


class SimpleLogger:
    """
    A simple logging class that streams logs to the console and writes logs to a file.
    """

    def __init__(self, log_file=None, level=logging.INFO, **kwargs):
        """
        Initializes the SimpleLogger with optional file logging.
        """
        # Create a logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)

        # Create a formatter
        formatter = logging.Formatter(
            "%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s"
        )

        # Create console handler and set level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # Create file handler if log_file is provided
        if log_file:
            self.log_file = log_file

            file_handler = logging.FileHandler(self.log_file)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        # Set the custom exception hook
        sys.excepthook = self.custom_exception_hook

    def get_logger(self):
        """
        Returns the logger instance for logging messages.
        """
        return self.logger

    def custom_exception_hook(self, exctype, value, tb):
        """
        Custom exception hook that logs uncaught exceptions.
        """
        # Log the exception along with the traceback
        self.logger.error("Uncaught exception", exc_info=(exctype, value, tb))

        # Call the default hook
        sys.__excepthook__(exctype, value, tb)


# Example usage:
# logger = SimpleLogger().get_logger()
# logger.info("This is an info message")
# logger.error("This is an error message")
