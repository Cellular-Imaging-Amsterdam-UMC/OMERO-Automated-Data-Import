# config.py

import yaml
import json

def load_settings(settings_path='config/settings.yml'):
    with open(settings_path, 'r') as file:
        return yaml.safe_load(file)

def load_json(json_path):
    with open(json_path, 'r') as file:
        return json.load(file)

settings = load_settings()