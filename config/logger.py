from sys import stdout
from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO, WARNING, ERROR, CRITICAL

def formatter(log: dict) -> str:
    """
    Formats the log message.
    """
    return f"{log['level']} - {log['message']}"


def create_logger() -> getLogger:
    """
    Creates a logger.
    """
    logger = getLogger()
    logger.setLevel(DEBUG)
    handler = StreamHandler(stdout)
    handler.setLevel(INFO)
    handler.setFormatter(Formatter(formatter))
    logger.addHandler(handler)
    return logger

LOGGER = create_logger()