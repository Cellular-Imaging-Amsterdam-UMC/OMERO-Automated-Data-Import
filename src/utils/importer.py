# importer.py

import os
import glob
import logging
import ezomero
import functools
from omero.gateway import BlitzGateway
from omero.sys import Parameters
from utils.ingest_tracker import log_ingestion_step, STAGE_IMPORTED, STAGE_PREPROCESSING
import Ice
import time
from omero.cli import CLI
from omero.rtypes import rstring, rlong
from omero.plugins.sessions import SessionsControl
from subprocess import Popen, PIPE, STDOUT, CalledProcessError
import re
from importlib import import_module
ImportControl = import_module("omero.plugins.import").ImportControl

MAX_RETRIES = 5  # Maximum number of retries
RETRY_DELAY = 5  # Delay between retries (in seconds)
TMP_OUTPUT_FOLDER = f"./converted-data"

def connection(func):
    @functools.wraps(func)
    def wrapper_connection(self, *args, **kwargs):
        try:
            with BlitzGateway(self.user, self.password, host=self.host, port=self.port, secure=True) as root_conn:
                self.logger.debug("Connected (again) as root")
                if root_conn.connect():
                    uuid = self.data_package.get('UUID')
                    intended_username = self.data_package.get('Username')
                    group_id = self.data_package.get('GroupID')
                    group_name = self.data_package.get('Group')

                    self.logger.debug(f"TTL: {self.ttl_for_user_conn}")
                    with root_conn.suConn(intended_username, ttl=self.ttl_for_user_conn) as user_conn:
                        user_conn.keepAlive()
                        self.logger.debug(f"Connected (again) as user {intended_username}")
                        user_conn.setGroupForSession(group_id)
                        self.logger.debug(f"Session group set to {group_name}")
                        result = func(self, user_conn, *args, **kwargs)
                        return result
                else:
                    raise ConnectionError("Could not connect to the OMERO server")
        except Exception as e:
            self.logger.error(f"Exception in connection wrapper: {e}", exc_info=True)
            raise
    return wrapper_connection

class DataProcessor:
    def __init__(self, data_package, logger=None):
        """Initialize DataProcessor with proper logging."""
        self.data_package = data_package
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug(f"Initializing DataProcessor for package: {data_package.get('UUID', 'Unknown')}")

    def has_preprocessing(self):
        """Check if any 'preprocessing_' keys are present in the data package."""
        return any(key.startswith("preprocessing_") for key in self.data_package)

    def get_preprocessing_args(self, file_path):
        """Generate podman command arguments from 'preprocessing_' keys in the data package."""
        self.logger.debug(f"Getting preprocessing args for file: {file_path}")
        if not self.has_preprocessing():
            self.logger.info("No preprocessing options found.")
            return None, None, None

        # Retrieve preprocessing container from dict
        container = self.data_package.get("preprocessing_container")
        if not container:
            self.logger.warning("No 'preprocessing_container' defined in data package.")
            return None, None, None
        # Add 'docker.io/' prefix if not already present
        if not container.startswith("docker.io/"):
            container = "docker.io/" + container

        # Build kwargs from remaining 'preprocessing_' keys (exclude 'preprocessing_container')
        kwargs = []
        mount_path = None
        for key, value in self.data_package.items():
            if key.startswith("preprocessing_") and key != "preprocessing_container":
                # Check if the value contains a placeholder like {Files}
                if isinstance(value, str) and "{Files}" in value:
                    if file_path:
                        # Replace {Files} with the file paths in the secondary mount
                        data_file_path = os.path.join("/data", os.path.basename(file_path))
                        value = value.replace("{Files}", data_file_path)

                        # Gather the mount path (replace parent dir with /data)
                        mount_path = os.path.dirname(file_path)

                # Convert key to '--key', 'value' format
                arg_key = key.replace("preprocessing_", "")
                kwargs.append(f"--{arg_key}")
                kwargs.append(value)

        return container, kwargs, mount_path

    def replace_placeholders(self, value):
        """Replace placeholders like {key} with corresponding data package values."""
        # Find all placeholders in the format {key}
        placeholders = re.findall(r'\{(\w+)\}', value)

        # Replace each placeholder with its corresponding value from data_package
        for placeholder in placeholders:
            replacement_value = self.data_package.get(placeholder)
            if replacement_value is not None:
                value = value.replace(f"{{{placeholder}}}", str(replacement_value))
            else:
                self.logger.warning(f"Placeholder '{placeholder}' not found in data package.")

        return value

    def build_podman_command(self, file_path):
        """Constructs the full Podman command."""
        container, kwargs, mount_path = self.get_preprocessing_args(file_path)
        if not container:
            return None

        # Predefined Podman settings
        podman_settings = ["podman", "run", "--rm", "--userns=keep-id"]
        # Add the volume mount if mount_path is available
        if mount_path:
            podman_settings = podman_settings + ["-v", f"{mount_path}:/data"]
        
        # TODO don't hardcode   
        kwargs.append(f"--outputfolder")
        folder = os.path.dirname(file_path)
        relative_output_path = os.path.join("/data", TMP_OUTPUT_FOLDER.lstrip('./'))
        # Create the directory if it doesn't exist
        os.makedirs(relative_output_path, exist_ok=True)
        self.logger.info(f"Created intermediate output folder {relative_output_path}")
        kwargs.append(relative_output_path)

        podman_command = podman_settings + [container] + kwargs

        self.logger.info(f"Podman command: {' '.join(podman_command)}")
        return podman_command

    def log_subprocess_output(self, pipe):
        for line in iter(pipe.readline, b''):  # b'\n'-separated lines
            self.logger.debug('sub: %r', line)

    def run(self, dry_run=False):
        """Run the constructed podman command and check its exit status."""
        if not self.has_preprocessing():
            self.logger.info("No preprocessing options found.")
            return True  # Indicate success since there's nothing to do

        file_paths = self.data_package.get("Files", [])
        for file_path in file_paths:
            self.logger.info(f"Processing file path: {file_path}")

            podman_command = self.build_podman_command(file_path)
            if not podman_command:
                self.logger.error("Failed to build podman command.")
                return False

            if dry_run:
                self.logger.info(f"Dry run enabled. Podman command would have been: {' '.join(podman_command)}")
                continue  # Continue to next file_path

            # Run the command and wait for it to complete
            process = Popen(podman_command, stdout=PIPE, stderr=STDOUT)
            with process.stdout:  # log output during subprocess run
                self.log_subprocess_output(process.stdout)
            returncode = process.wait()  # 0 means success
            if returncode == 0:
                self.logger.info("Podman command executed successfully.")
            else:
                self.logger.info(f"Podman command failed with exit code {returncode}.")
                return False
        return True

class DataPackageImporter:
    """
    Handles the import of data packages into OMERO.
    """
    def __init__(self, config, data_package, ttl_for_user_conn=6000000):
        """
        Initialize the DataPackageImporter with configuration settings.
        """
        self.config = config
        self.data_package = data_package
        self.ttl_for_user_conn = ttl_for_user_conn
        self.logger = logging.getLogger(__name__)
        self.logger.debug(f"Initializing DataPackageImporter for UUID: {data_package.get('UUID', 'Unknown')}")

        # Log OMERO connection details (excluding password)
        self.host = os.getenv('OMERO_HOST')
        self.password = os.getenv('OMERO_PASSWORD')
        self.user = os.getenv('OMERO_USER')
        self.port = os.getenv('OMERO_PORT')
        self.logger.debug(f"OMERO connection details - Host: {self.host}, User: {self.user}, Port: {self.port}")

    @connection
    def import_to_omero(self, conn, file_path, target_id, target_type, uuid, transfer_type="ln_s", depth=None):
        """
        Import a file to OMERO using CLI, either to a dataset or screen.
        """
        self.logger.debug(f"Starting import to OMERO - File: {file_path}, Target: {target_id} ({target_type})")
        cli = CLI()
        cli.register('import', ImportControl, '_')
        cli.register('sessions', SessionsControl, '_')

        # Common CLI arguments for importing
        arguments = [
            'import',
            '-k', conn.getSession().getUuid().val,
            '-s', conn.host,
            '-p', str(conn.port),
            f'--transfer={transfer_type}',
            '--no-upgrade',
            '--file', f"logs/cli.{uuid}.logs",
            '--errs', f"logs/cli.{uuid}.errs",
        ]
        # Add optional arguments based on config values
        if 'parallel_upload_per_worker' in self.config:
            arguments.append('--parallel-upload')
            arguments.append(str(self.config['parallel_upload_per_worker']))

        if 'parallel_filesets_per_worker' in self.config:
            arguments.append('--parallel-fileset')
            arguments.append(str(self.config['parallel_filesets_per_worker']))

        # Add skip options based on config values
        if self.config.get('skip_all', False):
            arguments.append('--skip')
            arguments.append('all')
        else:
            if self.config.get('skip_checksum', False):
                arguments.append('--skip')
                arguments.append('checksum')
            if self.config.get('skip_minmax', False):
                arguments.append('--skip')
                arguments.append('minmax')
            if self.config.get('skip_thumbnails', False):
                arguments.append('--skip')
                arguments.append('thumbnails')
            if self.config.get('skip_upgrade', False):
                arguments.append('--skip')
                arguments.append('upgrade')

        # Add depth argument if provided
        if depth:
            arguments.append('--depth')
            arguments.append(str(depth))

        # Add target-specific argument
        if target_type == 'screen':
            arguments.extend(['-r', str(target_id)])
        elif target_type == 'dataset':
            arguments.extend(['-d', str(target_id)])
        else:
            raise ValueError("Invalid target_type. Must be 'dataset' or 'screen'.")

        # Add the file path to the arguments
        arguments.append(str(file_path))

        # Invoke the CLI with the constructed arguments
        cli.invoke(arguments)

        if cli.rv == 0:
            self.imported = True
            self.logger.info(f'Imported {str(file_path)}')
            return True
        else:
            self.imported = False
            self.logger.error(f'Import of {str(file_path)} has failed!')
            return False

    @connection
    def get_plate_ids(self, conn, file_path, screen_id):
        """Get the Ids of imported plates."""
        if self.imported is not True:
            self.logger.error(f'File {file_path} has not been imported')
            return None
        else:
            self.logger.debug("Retrieving Plate IDs")
            q = conn.getQueryService()
            params = Parameters()
            path_query = f"{str(file_path).strip('/')}%"
            self.logger.debug(f"path query: {path_query}. Screen_id: {screen_id}")
            params.map = {
                "cpath": rstring(path_query),
                "screen_id": rlong(screen_id),
            }
            results = q.projection(
                "SELECT DISTINCT p.id, p.details.creationEvent.time FROM Plate p "
                "JOIN p.wells w "
                "JOIN w.wellSamples ws "
                "JOIN ws.image i "
                "JOIN i.fileset fs "
                "JOIN fs.usedFiles u "
                "JOIN p.screenLinks spl "
                "WHERE u.clientPath LIKE :cpath AND spl.parent.id = :screen_id "
                "ORDER BY p.details.creationEvent.time DESC",
                params,
                conn.SERVICE_OPTS
            )
            self.logger.debug(f"Query results: {results}")
            plate_ids = [r[0].val for r in results]
            return plate_ids

    @connection
    def import_dataset(self, conn, target, dataset, transfer="ln_s", depth=None):
        # Initialize a dict for named kwargs
        kwargs = {"transfer": transfer}

        # Add parallel options to kwargs if configured
        if 'parallel_upload_per_worker' in self.config:
            kwargs['parallel-upload'] = str(self.config['parallel_upload_per_worker'])
        if 'parallel_filesets_per_worker' in self.config:
            kwargs['parallel-fileset'] = str(self.config['parallel_filesets_per_worker'])
        if self.config.get('skip_all', False):
            kwargs['skip'] = 'all'

        # Add depth argument to kwargs if provided
        if depth:
            kwargs['depth'] = str(depth)
            
        uuid = self.data_package.get('UUID')
        kwargs['file'] = f"logs/cli.{uuid}.logs"
        kwargs['errs'] = f"logs/cli.{uuid}.errs"

        return ezomero.ezimport(conn=conn, target=target, dataset=dataset, **kwargs)

    def upload_files(self, conn, file_paths, dataset_id=None, screen_id=None):
        """
        Upload files to a specified dataset or screen in OMERO.
        """
        uuid = self.data_package.get('UUID')

        if dataset_id and screen_id:
            raise ValueError("Cannot specify both dataset_id and screen_id. Please provide only one.")
        if not dataset_id and not screen_id:
            raise ValueError("Either dataset_id or screen_id must be specified.")

        successful_uploads = []
        failed_uploads = []
        
        self.logger.debug(f"{file_paths}")

        for file_path in file_paths:
            self.logger.debug(f"Uploading file: {file_path}")
            try:
                if screen_id:
                    self.logger.debug(f"Uploading to screen: {screen_id}")
                    imported = self.import_to_omero(
                        file_path=str(file_path),
                        target_id=screen_id,
                        target_type='screen',
                        uuid=uuid,
                        depth=10
                    )
                    self.logger.debug("Upload done. Retrieving plate id.")
                    image_ids = self.get_plate_ids(str(file_path), screen_id)
                else:
                    self.logger.debug(f"Uploading to dataset: {dataset_id}")
                    image_ids = self.import_dataset(
                        target=str(file_path),
                        dataset=dataset_id,
                        transfer="ln_s"
                    )

                if image_ids:
                    # Ensure we're working with a single integer ID
                    image_or_plate_id = image_ids[0] if isinstance(image_ids, list) else image_ids

                    try:
                        self.add_image_annotations(
                            image_or_plate_id,
                            uuid,
                            file_path,
                            is_screen=bool(screen_id)
                        )

                        self.logger.info(f"Uploaded file: {file_path} to dataset/screen ID: {dataset_id or screen_id} with ID: {image_or_plate_id}")
                    except Exception as annotation_error:
                        self.logger.error(f"File uploaded but annotation failed for {file_path}: {annotation_error}")
                    # Still consider it a successful upload even if annotation fails
                    successful_uploads.append((file_path, dataset_id or screen_id, os.path.basename(file_path), image_or_plate_id))
                else:
                    self.logger.error(f"Upload rejected by OMERO for file {file_path} to dataset/screen ID: {dataset_id or screen_id}. No ID returned.")
                    failed_uploads.append((file_path, dataset_id or screen_id, os.path.basename(file_path), None))
            except Exception as e:
                self.logger.error(f"Error uploading file {file_path} to dataset/screen ID: {dataset_id or screen_id}: {e}")
                failed_uploads.append((file_path, dataset_id or screen_id, os.path.basename(file_path), None))
        return successful_uploads, failed_uploads

    def upload_screen_as_parallel_dataset(self, user_conn, file_paths, temp_dataset_id, screen_id):
        for file_path in file_paths:
            folder = os.path.dirname(file_path)
            relative_output_path = os.path.join(folder, TMP_OUTPUT_FOLDER.lstrip('./'))
            self.logger.info(f"Uploading dataset from {relative_output_path}")
            base_name = os.path.splitext(os.path.basename(file_path))[0]

            # Step 1: Find the companion file
            companion_pattern = os.path.join(folder, "*.companion.ome")
            companion_files = glob.glob(companion_pattern)

            if not companion_files:
                raise FileNotFoundError(f"No companion OME file found in {folder}")
            
            companion_file = companion_files[0]
            self.logger.info(f"Found companion file: {companion_file}")

            # Step 2: Find any OME-TIFF file
            ome_tiff_pattern = os.path.join(folder, "*.ome.tiff")
            ome_tiff_files = glob.glob(ome_tiff_pattern)

            if not ome_tiff_files:
                raise FileNotFoundError(f"No OME-TIFF files found in {folder}")
            
            ome_tiff_file = ome_tiff_files[0]
            self.logger.info(f"Selected OME-TIFF file: {ome_tiff_file}")

            # Step 3: Rename the companion file temporarily
            companion_temp = companion_file + ".temp"
            os.rename(companion_file, companion_temp)
            self.logger.info(f"Renamed companion file to: {companion_temp}")

            try:
                # Step 4: Perform your custom action (replace this with your logic)
                self.logger.info("Uploading screen as dataset...")
                # Call upload_files with the appropriate ID
                successful_uploads, failed_uploads = self.upload_files(
                    user_conn,
                    [relative_output_path],
                    dataset_id=temp_dataset_id,
                    screen_id=None
                )

                # Step 5: Rename the companion file back to its original name
                os.rename(companion_temp, companion_file)
                self.logger.info(f"Restored companion file to its original name: {companion_file}")
                
                # TODO: run conversion script on temp_dataset_id -> screen_id
                self.logger.info(f"TODO: Now we should run conversion to Plate script with {companion_file} on dataset {temp_dataset_id} to screen {screen_id}...")
                

            except Exception as e:
                # If any error occurs, attempt to restore the original file name
                os.rename(companion_temp, companion_file)
                self.logger.info(f"Error occurred: {e}. Restored companion file to: {companion_file}")
                raise

        return ome_tiff_file  # Return the selected OME-TIFF file if needed


    def import_data_package(self):
        """
        Import a data package into OMERO as the intended user.
        """
        self.logger.info(f"Starting import for data package: {self.data_package.get('UUID', 'Unknown')}")
        self.logger.debug("Establishing connection")
        intended_username = self.data_package.get('Username')
        group_id = self.data_package.get('GroupID')
        group_name = self.data_package.get('Group')

        if not intended_username or not group_id or not group_name:
            self.logger.error("Required user or group information not provided in data package.")
            return [], [], True

        # Retry mechanism for the connection
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                # Connect as root
                with BlitzGateway(self.user, self.password, host=self.host, port=self.port, secure=True) as root_conn:
                    if not root_conn.connect():
                        self.logger.error("Failed to connect to OMERO as root.")
                        return [], [], True
                    else:
                        self.logger.info("Connected to OMERO as root.")

                    root_conn.keepAlive()

                    # Create a new connection as the intended user
                    with root_conn.suConn(intended_username, ttl=self.ttl_for_user_conn) as user_conn:
                        if not user_conn:
                            self.logger.error(f"Failed to create connection as user {intended_username}")
                            return [], [], True

                        user_conn.keepAlive()
                        # Set the correct group for the session
                        user_conn.setGroupForSession(group_id)

                        self.logger.info(f"Connected as user {intended_username} in group {group_name}")

                        all_successful_uploads = []
                        all_failed_uploads = []

                        dataset_id = self.data_package.get('DatasetID')
                        screen_id = self.data_package.get('ScreenID')

                        # Validation for dataset_id and screen_id
                        if dataset_id and screen_id:
                            raise ValueError("Cannot specify both DatasetID and ScreenID in the data package. Please provide only one.")
                        if not dataset_id and not screen_id:
                            raise ValueError("Either DatasetID or ScreenID must be provided in the data package.")

                        file_paths = self.data_package.get('Files', [])
                        self.logger.debug(f"File paths to be uploaded: {file_paths}")

                        # Run preprocessing if needed
                        processor = DataProcessor(self.data_package, self.logger)
                        if processor.has_preprocessing():
                            log_ingestion_step(self.data_package, STAGE_PREPROCESSING)
                            success = processor.run(dry_run=False)
                            if success:
                                self.logger.info("Preprocessing ran successfully. Continuing import.")
                                if screen_id:  # import as dataset first
                                    temp_dataset_id = 2001  # TODO: make a temp OMERO dataset first
                                    # TODO: move companion.ome and upload the first tiff.
                                    self.upload_screen_as_parallel_dataset(user_conn, file_paths, temp_dataset_id, screen_id)
                                    # TODO: Also handle metadata CSV
                                else:
                                    # Call upload_files with the appropriate ID
                                    successful_uploads, failed_uploads = self.upload_files(
                                        user_conn,
                                        file_paths,
                                        dataset_id=dataset_id,
                                        screen_id=screen_id
                                    )
                            else:
                                self.logger.error("There was an issue with running the preprocessing container.")
                                all_failed_uploads.extend([(file_path, dataset_id or screen_id, os.path.basename(file_path), None) for file_path in file_paths])
                                return all_successful_uploads, all_failed_uploads, False
                        else:
                            self.logger.info("No preprocessing required. Continuing import.")

                            # Call upload_files with the appropriate ID
                            successful_uploads, failed_uploads = self.upload_files(
                                user_conn,
                                file_paths,
                                dataset_id=dataset_id,
                                screen_id=screen_id
                            )
                        self.logger.debug(f"Successful uploads: {successful_uploads}")
                        self.logger.debug(f"Failed uploads: {failed_uploads}")

                        all_successful_uploads.extend(successful_uploads)
                        all_failed_uploads.extend(failed_uploads)

                        if successful_uploads:
                            log_ingestion_step(self.data_package, STAGE_IMPORTED)

                    return all_successful_uploads, all_failed_uploads, False

            except Exception as e:
                if isinstance(e, Ice.ConnectionRefusedException) or "connect" in f"{e}".lower():
                    retry_count += 1
                    self.logger.error(f"Connection refused (attempt {retry_count}/{MAX_RETRIES}): {e}")
                    if retry_count < MAX_RETRIES:
                        self.logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                    else:
                        self.logger.error("Max retries reached. Aborting import.")
                        return [], [], True  # Fail after max retries
                else:
                    self.logger.error(f"Exception during import: {e}", exc_info=True)
                    return [], [], True

        return [], [], True

    @connection
    def add_image_annotations(self, conn, object_id, uuid, file_path, is_screen=False):
        """Add UUID and filepath as annotations to the image or plate."""
        try:
            annotation_dict = {'UUID': str(uuid), 'Filepath': str(file_path)}
            ns = "omeroadi.import"

            if is_screen:
                self.logger.debug(f"Attempting to add annotations to Plate ID: {object_id}")
                object_type = "Plate"  # Set to Plate when it's a screen
            else:
                self.logger.debug(f"Attempting to add annotations to Image ID: {object_id}")
                object_type = "Image"  # Set to Image when it's a dataset

            self.logger.debug(f"Annotation dict: {annotation_dict}")

            map_ann_id = ezomero.post_map_annotation(
                conn=conn,
                object_type=object_type,
                object_id=object_id,
                kv_dict=annotation_dict,
                ns=ns,
                across_groups=False  # Set to False if you don't want cross-group behavior
            )

            if map_ann_id:
                self.logger.info(f"Successfully added annotations to {object_type} ID: {object_id}. MapAnnotation ID: {map_ann_id}")
            else:
                self.logger.warning(f"MapAnnotation created for {object_type} ID: {object_id}, but no ID was returned.")

        except Exception as e:
            self.logger.error(f"Failed to add annotations to {object_type} ID: {object_id}. Error: {str(e)}")
