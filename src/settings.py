import logging
from pathlib import Path

from rich.logging import RichHandler

LOGGING_FORMAT = "%(message)s"


ROOT_FOLDER = Path(__file__).parents[1]
DEFAULT_DATA_FOLDER = ROOT_FOLDER / "data"
DEFAULT_INPUT_FOLDER = DEFAULT_DATA_FOLDER / "raw"
DEFAULT_OUTPUT_FOLDER = DEFAULT_DATA_FOLDER / "processed"


def create_logger(logger_name: str) -> logging.Logger:
    logging.basicConfig(
        level="NOTSET", format=LOGGING_FORMAT, datefmt="[%X]", handlers=[RichHandler()]
    )
    return logging.getLogger(logger_name)
