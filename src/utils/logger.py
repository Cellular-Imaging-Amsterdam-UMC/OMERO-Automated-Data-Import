# Copyright 2023 Rodrigo Rosas-Bertolini
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# logger.py

import logging
import sys
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

def setup_logger(name, log_file, level=logging.DEBUG):
    """Function to setup as many loggers as you want"""
    LOGFORMAT = '%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s'
    formatter = logging.Formatter(LOGFORMAT)

    # Create handlers
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)

    # Create a queue and a QueueHandler
    log_queue = Queue()
    queue_handler = QueueHandler(log_queue)

    # Setup the root logger
    logging.basicConfig(level=level, handlers=[queue_handler])

    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(queue_handler)
    logger.propagate = False

    # Create and start a QueueListener
    listener = QueueListener(log_queue, file_handler, stream_handler)
    listener.start()

    return logger, listener

def log_flag(logger, flag_type):
    line_pattern = "    /\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/"
    if flag_type == 'start':
        logger.info("\n" + line_pattern + "\n           READY TO UPLOAD DATA TO OMERO\n" + line_pattern)
    elif flag_type == 'end':
        logger.info("\n" + line_pattern + "\n           STOPPING AUTOMATIC UPLOAD SERVICE\n" + line_pattern)
