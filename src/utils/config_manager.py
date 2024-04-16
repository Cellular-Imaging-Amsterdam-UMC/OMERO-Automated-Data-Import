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

# config_manager.py

import yaml
import json

def load_settings(settings_path='config/settings.yml'):
    with open(settings_path, 'r') as file:
        return yaml.safe_load(file)

def load_json(json_path):
    with open(json_path, 'r') as file:
        return json.load(file)

settings = load_settings()