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
        self.initial_timestamp = datetime.now().isoformat().replace(".","").replace(":","_") + "_" + str(id)
        # logging.basicConfig(filename=f'log_{self.initial_timestamp}.log',
        #                     filemode='a',
        #                     format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
        #                     datefmt='%H:%M:%S',
        #                     level=logging.DEBUG)
        #
        # logging.info("Running dip-extractor-cli")
        #
        # self.logger = logging.getLogger(f'dip-extractor-cli-{self.initial_timestamp}')

        level = logging.INFO
        self.logger_name = f'log_{self.initial_timestamp}'
        self.log_file = f'log_{self.initial_timestamp}.log'
        self.logger = logging.getLogger(self.logger_name)
        formatter = logging.Formatter('%(asctime)s : %(message)s')
        fileHandler = logging.FileHandler(self.log_file, mode='w')
        fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        self.logger.setLevel(level)
        self.logger.addHandler(fileHandler)
        self.logger.addHandler(streamHandler)

    def reset_timestamp(self):
        # global initial_timestamp
        self.initial_timestamp = datetime.now().isoformat().replace(".","").replace(":","_")

        return self.initial_timestamp