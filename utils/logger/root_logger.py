# root_logger.py
# provides the logger for the entire repository

########################################################

import logging

########################################################


def logger():

    log = logging.getLogger(__name__)

    log.setLevel(logging.INFO)

    return log
