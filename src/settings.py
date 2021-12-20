import logging
import logging.handlers
from datetime import datetime
import random
import os


class Settings():

    def __init__(self, id):
        # Files and Path global variable
        self.logger = None
        self.initial_timestamp = None
        self.initial_timestamp = datetime.now().isoformat().replace(".", "").replace(":", "_") + "_" + str(id)

        level = logging.INFO
        self.logger_name = f'log_{self.initial_timestamp}'
        self.log_file = f'log_{self.initial_timestamp}.log'
        self.logger = logging.getLogger(self.logger_name)
        formatter = logging.Formatter('%(asctime)s : %(message)s')
        file_handler = logging.FileHandler(self.log_file, mode='w')
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        self.logger.setLevel(level)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def reset_timestamp(self):

        self.initial_timestamp = datetime.now().isoformat().replace(".", "").replace(":", "_")

        return self.initial_timestamp
