import logging

def setup_logger(name, log_file, level=logging.INFO):
    """Function to setup as many loggers as you want"""
    handler = logging.FileHandler(log_file, mode='a') 
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False

    return logger

def log_ready_flag(logger):
    """
    Logs a decorative flag indicating the system is ready to upload data to OMERO.
    """
    line_pattern = "/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/"
    logger.info("\n" + line_pattern + "\n         READY TO UPLOAD DATA TO OMERO\n" + line_pattern)