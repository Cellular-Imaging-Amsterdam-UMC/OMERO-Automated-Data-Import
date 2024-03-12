from .logger import setup_logger

class UploadOrderManager:
    def __init__(self, order_file_path, settings):
        self.settings = settings
        self.logger = setup_logger(__name__, self.settings.get('log_file_path', 'upload_order_manager.log'))
        self.order_info = self._parse_order_file(order_file_path)
        self.validate_order_info()

    def _parse_order_file(self, order_file_path):
        order_info = {}
        with open(order_file_path, 'r') as file:
            for line in file:
                key, value = line.strip().split(': ', 1)
                if key == 'Files':
                    order_info[key] = [file_name.strip() for file_name in value.split(',')]
                else:
                    order_info[key] = value.strip()
        return order_info

    def validate_order_info(self):
        required_keys = ['UUID', 'Group', 'Username', 'Dataset', 'Path', 'Files']
        missing_keys = [key for key in required_keys if key not in self.order_info]
        empty_keys = [key for key, value in self.order_info.items() if not value]

        if missing_keys:
            self.logger.error(f"Missing required keys in order info: {', '.join(missing_keys)}")
        if empty_keys:
            self.logger.error(f"Empty values found for keys in order info: {', '.join(empty_keys)}")

        if not missing_keys and not empty_keys:
            self.logger.info("Order info validation passed: All required keys are present and non-empty.")

    def get_dataset_full_path(self):
        path = self.order_info.get('Path', '')
        if not path:
            raise ValueError("Path is missing from the order information.")
        return path

    def log_upload_order_info(self):
        info_lines = [f"{key}: {value}" for key, value in self.order_info.items()]
        log_message = "Upload Order Information:\n" + "\n".join(info_lines)
        self.logger.info(log_message)

    def get_order_info(self):
        required_keys = ['UUID', 'Group', 'Username', 'Dataset', 'Path', 'Files']
        missing_keys = [key for key in required_keys if key not in self.order_info]
        if missing_keys:
            raise KeyError(f"Missing required keys in order info: {', '.join(missing_keys)}")
        return (
            self.order_info['UUID'],
            self.order_info['Group'],
            self.order_info['Username'],
            self.order_info['Dataset'],
            self.order_info['Path'],
            self.order_info['Files']
        )
