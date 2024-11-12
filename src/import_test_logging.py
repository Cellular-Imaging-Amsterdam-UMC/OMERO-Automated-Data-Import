import logging
from concurrent.futures import ProcessPoolExecutor
# Configure the logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class WProcess1:
    def __init__(self, logger):
        self.logger = logger
    def worker_process(self, i):
        self.logger.info(f"1: {i}")
        self.logger.warning(f"1: {i}")


class WProcess2:
    def __init__(self, logger):
        self.logger = logger
    def do(self, i):
        self.logger.info(f"2: {i}")
        self.logger.warning(f"2: {i}")

  
class WProcess3:
    def __init__(self):
        self.logger = logging.getLogger("wp3")
    def worker_process(self, i):
        self.logger.info(f"3: {i}") 
        self.logger.warning(f"3: {i}")    



with ProcessPoolExecutor(max_workers=4) as executor:
    for i in range(5):
        wp = WProcess1(logger)
        executor.submit(wp.worker_process, i)
        
        wp2 = WProcess2(logger)
        executor.submit(wp2.do, i)
        
        wp = WProcess3()
        executor.submit(wp.worker_process, i)
        