import sys
import json
import os
import asyncio
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuration
#CONFIG_PATH = 
DIRECTORY_STRUCTURE = "config/directory_structure.json"
LOG_FILE_PATH = "logs/app.logs"

# Set up logging
logging.basicConfig(level=logging.INFO, filename=LOG_FILE_PATH, filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

# Placeholder for the ingest function
async def ingest(group, user, dataset):
    logger.info(f"Starting ingestion for group: {group}, user: {user}, dataset: {dataset}")
    # Simulate a long-running process
    await asyncio.sleep(2)
    logger.info(f"Completed ingestion for group: {group}, user: {user}, dataset: {dataset}")

# Handler class
class DatasetHandler(FileSystemEventHandler):
    def __init__(self, base_directory, group_folders, loop):
        self.base_directory = base_directory
        self.group_folders = group_folders
        self.loop = loop

    def on_created(self, event):
        if not event.is_directory:
            return

        # Normalize paths for comparison
        created_dir = os.path.normpath(event.src_path)
        for group, users in self.group_folders.items():
            for user in users:
                user_folder = os.path.normpath(os.path.join(self.base_directory, group, user))
                # Check if the created directory is directly under the user's folder
                if os.path.dirname(created_dir) == user_folder:
                    logger.info(f"Dataset detected: {os.path.basename(created_dir)} for user: {user} in group: {group}")
                    asyncio.run_coroutine_threadsafe(
                        ingest(group, user, os.path.basename(created_dir)), 
                        self.loop
                    )

    @staticmethod
    def is_valid_dataset(folder_path):
        if not os.path.isdir(folder_path):
            return False
        for _, dirs, _ in os.walk(folder_path):
            if dirs:
                return True
        return False

def main(directory):
    # Load JSON directory_structure
    with open(DIRECTORY_STRUCTURE, 'r') as f:
        directory_structure = json.load(f)

    group_folders = {group: users['membersOf'] for group, users in directory_structure['Groups'].items()}

    # Asyncio event loop
    loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(loop)

    # Set up the observer
    observer = Observer()
    for group in group_folders.keys():
        event_handler = DatasetHandler(directory, group_folders, loop)
        group_path = os.path.normpath(os.path.join(directory, group))
        observer.schedule(event_handler, path=group_path, recursive=True)
    observer.start()

    logger.info("Starting the folder monitoring service.")

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Stopping the folder monitoring service.")
    observer.join()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <directory>")
        sys.exit(1)

    directory = sys.argv[1]
    main(directory)