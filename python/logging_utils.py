# Imports
import logging
import json
from datetime import datetime

class JsonFormatter(logging.Formatter):
    """Custom formatter to output logs in JSON format"""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
            "duration": getattr(record, "duration", ""),
            "pid": record.process,
        }
        return json.dumps(log_entry)


def setup_logger(name, log_file, level=logging.INFO):
    """Function to setup a logger with JSON format"""
    formatter = JsonFormatter()

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    # Optionally add a StreamHandler to log to console
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(stream_handler)

    return logger


def log_message(logger, level, message, duration=""):
    """Function to log a message with the given logger"""
    extra = {"duration": duration}
    if level == "DEBUG":
        logger.debug(message, extra=extra)
    elif level == "INFO":
        logger.info(message, extra=extra)
    elif level == "WARN":
        logger.warning(message, extra=extra)
    elif level == "ERROR":
        logger.error(message, extra=extra)


# Example usage:
# logger = setup_logger('mapping_csv_to_parquet_incremental', '/var/log/myapp/mapping_csv_to_parquet_incremental.log')
# log_message(logger, 'INFO', 'This is an info message', '100ms')
