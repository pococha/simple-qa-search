import logging
import logging.handlers
import os



current_path = os.path.abspath(__file__)
father_path = os.path.abspath(os.path.dirname(current_path) + os.path.sep + ".")

def getLoggers(loggerName , loggerLevel , loggerLocation ):
    logger = logging.getLogger(loggerName)
    logger.setLevel(loggerLevel)
    format="%(asctime)s - %(levelname)s : %(message)s"
    formater = logging.Formatter(format)
    handler = logging.handlers.TimedRotatingFileHandler(loggerLocation, "D", 1, 0)
    handler.suffix = "%Y%m%d.%H:%M:%S"
    handler.setFormatter(formater)
    logger.addHandler(handler)
    return logger
