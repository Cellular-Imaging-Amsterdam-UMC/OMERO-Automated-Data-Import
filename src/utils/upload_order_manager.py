import logging
import yaml
from pathlib import Path

class UploadOrderManager:
    def __init__(self, order_file_path, settings_file_path):
        self.settings = self._load_settings(settings_file_path)
        self.order_info = self._parse_order_file(order_file_path)
        self._init_logging()

    def _load_settings(self, settings_file_path):
        with open(settings_file_path, 'r') as file:
            return yaml.safe_load(file)

    def _parse_order_file(self, order_file_path):
        order_info = {}
        with open(order_file_path, 'r') as file:
            for line in file:
                key, value = line.strip().split(': ')
                order_info[key] = value
        return order_info

    def _init_logging(self):
        log_file_path = self.settings['log_file_path']
        logging.basicConfig(level=logging.INFO, filename=log_file_path, filemode='a',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def get_dataset_full_path(self):
        base_dir = self.settings['base_dir']
        group = self.order_info['Group']
        project = self.order_info['Project']
        username = self.order_info['Username']
        data_dir_name = self.settings['data_dir_name']
        dataset = self.order_info['Dataset']
        full_path = Path(base_dir) / group / project / username / data_dir_name / dataset
        return str(full_path)

    def log_upload_order_info(self):
        info_lines = [f"{key}: {value}" for key, value in self.order_info.items()]
        log_message = "Upload Order Information:\n" + "\n".join(info_lines)
        self.logger.info(log_message)

    def get_order_info(self):
        return (
            self.order_info['Group'],
            self.order_info['Username'],
            self.order_info['Project'],
            self.order_info['Dataset']
        )

# Example usage
order_file_path = 'config/upload_order.template.txt'
settings_file_path = 'config/settings.yml'
manager = UploadOrderManager(order_file_path, settings_file_path)
print(manager.get_dataset_full_path())
manager.log_upload_order_info()