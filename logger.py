import logging


def setup_logger(name="global_logger"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create a console manager with a proper logging level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Format the logging messages.
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    # Add the console manager to the logger.
    if not logger.hasHandlers():
        logger.addHandler(console_handler)

    return logger


# create the global logger
logger = setup_logger()
