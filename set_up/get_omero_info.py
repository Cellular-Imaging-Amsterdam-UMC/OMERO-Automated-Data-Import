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

#get_omero_info.py

import subprocess
import os
import csv
import json
from dotenv import load_dotenv
from omero.gateway import BlitzGateway

# Load environment variables
load_dotenv()

def get_omero_groups(host, username, password, port=4064):
    # Construct the login command
    login_command = f"omero login {username}@{host}:{port} -w {password}"
    # Construct the command to list groups and save to CSV
    list_groups_command = "omero group list --style csv --long > groups_info.csv"

    # Combine commands to execute them in sequence
    combined_command = f"{login_command} && {list_groups_command}"

    try:
        # Execute the combined command
        subprocess.run(combined_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, executable='/bin/bash')
        print("Groups information saved to groups_info.csv")
    except subprocess.CalledProcessError as e:
        print(f"Failed to list groups. Error: {e.stderr}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def create_groups_json(): #Enable this after development
    groups_list = []
    try:
        with open('groups_info.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                groups_list.append({
                    "core_grp_name": "",  # This will be filled in by the user later
                    "omero_grp_name": row["name"],
                    "omero_grp_id": int(row["id"])
                })
        with open('groups_info.json', 'w', encoding='utf-8') as jsonfile:
            json.dump(groups_list, jsonfile, ensure_ascii=False, indent=4)
        print("Groups JSON file created successfully.")
    except Exception as e:
        print(f"Failed to create JSON file. Error: {e}")

def get_group_members(host, username, password, port=4064):
    conn = None
    members_of = []
    try:
        conn = BlitzGateway(username, password, host=host, port=port)
        conn.connect()
        groups = conn.getObjects("ExperimenterGroup")
        for group in groups:
            members = []
            for experimenter in group.copyGroupExperimenterMap():
                user = experimenter.child
                members.append(user.getOmeName().getValue())
            members_of.append({
                "omero_grp_name": group.getName(),
                "omero_grp_id": group.getId(),
                "members": members
            })
    except Exception as e:
        print(f"Failed to get group members. Error: {e}")
    finally:
        if conn:
            conn.close()
    return members_of

def save_members_of_json(members_of):
    try:
        with open('members_of.json', 'w', encoding='utf-8') as jsonfile:
            json.dump(members_of, jsonfile, ensure_ascii=False, indent=4)
        print("Members JSON file created successfully.")
    except Exception as e:
        print(f"Failed to create Members JSON file. Error: {e}")

if __name__ == "__main__":
    host = os.getenv("OMERO_HOST")
    username = os.getenv("OMERO_USERNAME")
    password = os.getenv("OMERO_PASSWORD")
    port = os.getenv("OMERO_PORT")

    if not all([host, username, password, port]):
        print("Please ensure all environment variables (OMERO_HOST, OMERO_USERNAME, OMERO_PASSWORD, OMERO_PORT) are set.")
    else:
        get_omero_groups(host, username, password, port)
        #create_groups_json()
        members_of = get_group_members(host, username, password, port)
        save_members_of_json(members_of)