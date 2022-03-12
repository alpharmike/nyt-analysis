import logging
import logging.config
from src.utils.utils import ROOT_DIR


def init_logger():
    logging.config.fileConfig(f"{ROOT_DIR}/config/logger.ini", defaults={'log_filename': f'{ROOT_DIR}/log/application'
                                                                                         f'.log'})
