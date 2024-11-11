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

"""
OMERO Automated Data Import Logging System
----------------------------------------

A thread-safe, queue-based logging system designed for OMERO data import operations.
This module provides centralized logging management through a singleton pattern.

Logging Flow:
------------
graph TD
    A[main.py::main] -->|1. setup_logger| B[LoggerManager]
    B -->|2. Creates| C[Main Logger]
    B -->|3. Initializes| D[Queue System]
    
    E[initialize.py::initialize_system] -->|4. get_module_logger| F[Module Logger]
    G[main.py::DirectoryPoller] -->|5. get_module_logger| F
    H[upload_order_manager.py::UploadOrderManager] -->|6. get_module_logger| F
    
    D -->|Handles Messages| I[QueueHandler]
    I -->|Routes| J[QueueListener]
    J -->|Writes| K[RotatingFileHandler]
    J -->|Outputs| L[StreamHandler]
    
    M[main.py::run_application] -->|Shutdown| N[LoggerManager.cleanup]
    N -->|Stop Listener| O[Close Resources]

    subgraph "1. Initialization"
        A
        B[LoggerManager::setup_logger]
        C
        D
    end

    subgraph "2. Module Registration"
        E
        G
        H
    end

    subgraph "3. Message Processing"
        I
        J
        K[log_file]
        L[console]
    end

    subgraph "4. Cleanup"
        M
        N
        O
    end

Key Components:
--------------
1. LoggerManager (logger.py)
   - setup_logger(): Initial configuration
   - get_module_logger(): Module-specific loggers
   - cleanup(): Resource management

2. Main Process (main.py)
   - main(): Logger initialization
   - run_application(): Runtime logging
   - DirectoryPoller: File monitoring logs

3. System Setup (initialize.py)
   - initialize_system(): System checks logging
   - check_directory_access(): Access verification logs

4. Order Management (upload_order_manager.py)
   - UploadOrderManager: Order processing logs

Configuration:
-------------
- Log Format: '%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s'
- File Rotation: 10MB per file, 5 backup files
- Queue Size: 10,000 messages
- Cleanup Timeout: 5.0 seconds default

Usage Example:
-------------
1. Initialize Logger:
   ```python
   from utils.logger import LoggerManager
   
   # In main.py::main()
   LoggerManager.setup_logger(__name__, "path/to/log.file")
   logger = LoggerManager.get_module_logger(__name__)
   ```

2. Module Usage:
   ```python
   # In any module
   from utils.logger import LoggerManager
   
   class MyClass:
       def __init__(self):
           self.logger = LoggerManager.get_module_logger(__name__)
   ```

3. Cleanup:
   ```python
   # In main.py::run_application()
   try:
       # ... application code ...
   finally:
       LoggerManager.cleanup(timeout=5.0)
   ```

Limitations:
-----------
1. Process Management:
   - Limited to single main process with ≤4 workers
   - No per-process log files
   - Shared queue between processes

2. Resource Usage:
   - Fixed queue size (10,000 messages)
   - Memory usage scales with queue size
   - File handles remain open until cleanup

3. Error Handling:
   - Basic fallback logging on failure
   - No automatic recovery mechanisms
   - Queue overflow protection only

Best Practices:
--------------
1. Always initialize logger before other operations
2. Use module-specific loggers via get_module_logger()
3. Include proper error context in log messages
4. Ensure cleanup is called during shutdown
5. Monitor log rotation and queue size in production

See Also:
---------
- main.py: Main application and logger initialization
- initialize.py: System setup and verification logging
- upload_order_manager.py: Order processing logging
"""

import logging
import sys
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from queue import Queue
from typing import Optional, Tuple
import time
import os

class LoggerManager:
    """Singleton manager for logging configuration and resources."""
    _instance = None
    _loggers = {}  # Store all module loggers
    _main_logger = None
    _listener = None
    _log_queue = None
    _initialized = False  # New flag
    _mp_initialized = False  # Track multiprocessing initialization

    @classmethod
    def is_initialized(cls) -> bool:
        return cls._initialized

    @classmethod
    def setup_logger(cls, name: str, log_file: str, level=logging.DEBUG) -> logging.Logger:
        """Setup the main logger and queue listener."""
        if cls._initialized and not cls._mp_initialized:
            # If we're in a subprocess, reinitialize the queue
            cls._setup_mp_logging()
            return cls._main_logger

        if cls._initialized:
            return cls._main_logger

        # Create log directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        os.makedirs(log_dir, exist_ok=True)

        LOGFORMAT = '%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(LOGFORMAT)

        # Setup root logger first
        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        # Create handlers
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            mode='a'
        )
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)

        # Setup queue with increased size
        cls._log_queue = Queue(maxsize=100000)  # Increased queue size
        queue_handler = QueueHandler(cls._log_queue)

        # Add queue handler to root logger
        root_logger.addHandler(queue_handler)

        # Create main logger
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False  # Prevent double logging

        # Create and start listener
        cls._listener = QueueListener(
            cls._log_queue, 
            file_handler, 
            stream_handler,
            respect_handler_level=True
        )
        cls._listener.start()

        cls._main_logger = logger
        cls._loggers[name] = logger
        cls._initialized = True
        
        # Log initialization success
        logger.info(f"Logging system initialized. Log file: {log_file}")
        return logger

    @classmethod
    def _setup_mp_logging(cls):
        """Setup multiprocessing-safe logging."""
        from multiprocessing import current_process
        
        # Only modify logging for child processes
        if current_process().name != 'MainProcess':
            # Create a new queue for this process
            cls._log_queue = Queue(maxsize=100000)
            
            # Add queue handler to existing loggers
            queue_handler = QueueHandler(cls._log_queue)
            for logger in cls._loggers.values():
                for handler in logger.handlers[:]:
                    logger.removeHandler(handler)
                logger.addHandler(queue_handler)
            
            cls._mp_initialized = True

    @classmethod
    def get_module_logger(cls, module_name: str) -> logging.Logger:
        """Get or create a logger for a specific module."""
        try:
            if not cls._initialized:
                raise RuntimeError("LoggerManager not initialized. Call setup_logger first.")
            if module_name not in cls._loggers:
                logger = logging.getLogger(module_name)
                logger.setLevel(cls._main_logger.level)
                logger.addHandler(QueueHandler(cls._log_queue))
                logger.propagate = False
                cls._loggers[module_name] = logger
            return cls._loggers[module_name]
        except Exception as e:
            # Fallback to basic logging if something goes wrong
            print(f"Error getting logger for {module_name}: {e}")
            return logging.getLogger(module_name)

    @classmethod
    def get_logger(cls) -> Optional[logging.Logger]:
        """Get the main logger instance."""
        return cls._main_logger

    @classmethod
    def cleanup(cls, timeout: float = 5.0) -> None:
        """Clean up all logging resources safely."""
        start_time = time.time()
        if cls._listener:
            cls._listener.stop()
            while not cls._log_queue.empty():
                if time.time() - start_time > timeout:
                    cls._main_logger.warning("Cleanup timed out with messages still in queue")
                    break
                time.sleep(0.1)
        cls._listener = None
        cls._main_logger = None
        cls._loggers.clear()
        cls._log_queue = None
        cls._initialized = False

# Keep existing functions
def setup_logger(name, log_file, level=logging.DEBUG):
    """Function to setup as many loggers as you want"""
    return LoggerManager.setup_logger(name, log_file, level)

def log_flag(logger, flag_type):
    line_pattern = "    /\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/"
    if flag_type == 'start':
        logger.info("\n" + line_pattern + "\n           READY TO UPLOAD DATA TO OMERO\n" + line_pattern)
    elif flag_type == 'end':
        logger.info("\n" + line_pattern + "\n           STOPPING AUTOMATIC UPLOAD SERVICE\n" + line_pattern)
