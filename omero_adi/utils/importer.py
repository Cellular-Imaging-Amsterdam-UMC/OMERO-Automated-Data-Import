import os
import csv
import shutil
import logging
import functools
import time
from subprocess import Popen, PIPE, STDOUT
from importlib import import_module

import ezomero
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, rlong
from omero.cli import CLI
from omero.plugins.sessions import SessionsControl
from omero.model import DatasetI
from omero.sys import Parameters

from .ingest_tracker import log_ingestion_step, STAGE_PREPROCESSING

# Import the ImportControl from the OMERO plugins (if needed)
ImportControl = import_module("omero.plugins.import").ImportControl

MAX_RETRIES = 5  # Maximum number of retries
RETRY_DELAY = 5  # Delay between retries (in seconds)
TMP_OUTPUT_FOLDER = "OMERO_inplace"
PROCESSED_DATA_FOLDER = ".processed"


def get_tmp_output_path(data_package):
    """
    Helper function to generate the temporary output folder path.
    """
    return os.path.join("/OMERO", TMP_OUTPUT_FOLDER, data_package.get('UUID'))


def connection(func):
    """
    A decorator that wraps a function so that it receives an OMERO user connection.
    If a connection is already provided (as the first positional argument after self), it is reused.
    """
    @functools.wraps(func)
    def wrapper_connection(self, *args, **kwargs):
        # If a connection is already provided, simply call the function.
        if args and hasattr(args[0], "keepAlive"):
            return func(self, *args, **kwargs)
        try:
            with BlitzGateway(self.user, self.password, host=self.host, port=self.port, secure=True) as root_conn:
                self.logger.debug("Connected as root to OMERO.")
                if root_conn.connect():
                    # Retrieve order-specific connection details from the data package.
                    intended_username = self.data_package.get('Username')
                    group_name = self.data_package.get('Group')
                    group_id = ezomero.get_group_id(
                        root_conn, group_name)  # grab w/ ezomero
                    self.logger.debug(f"Using TTL: {self.ttl_for_user_conn}")
                    with root_conn.suConn(intended_username, ttl=self.ttl_for_user_conn) as user_conn:
                        user_conn.keepAlive()
                        self.logger.debug(
                            f"Connected as user {intended_username}")
                        user_conn.setGroupForSession(group_id)
                        self.logger.debug(f"Session group set to {group_name}")
                        return func(self, user_conn, *args, **kwargs)
                else:
                    raise ConnectionError(
                        "Could not connect to the OMERO server as root.")
        except Exception as e:
            self.logger.error(
                f"Exception in connection wrapper: {e}", exc_info=True)
            raise
    return wrapper_connection


def retry_on_connection_issue(func):
    """
    A decorator to retry a function when connection issues occur.
    If an exception with 'connect' in its message is raised, the function is retried.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Assumes the first argument is 'self' with a logger
        logger = args[0].logger
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "connect" in str(e).lower():
                    logger.error(
                        f"Connection issue (attempt {attempt}/{MAX_RETRIES}): {e}")
                    if attempt < MAX_RETRIES:
                        logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                        continue
                raise
        raise ValueError(
            f"Max retries ({MAX_RETRIES}) reached in {func.__name__}")
    return wrapper


class DataProcessor:
    # TODO: Change so it is not PODMAN specific. Otherwise testing will be... tricky. Or we skip for now :)
    def __init__(self, data_package, logger=None):
        """Initialize DataProcessor with proper logging."""
        self.data_package = data_package
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug(
            f"Initializing DataProcessor for package: {data_package.get('UUID', 'Unknown')}")

    def has_preprocessing(self):
        """Check if any preprocessing options are specified in the data package."""
        return any(key.startswith("preprocessing_") for key in self.data_package)

    def get_preprocessing_args(self, file_path):
        """Generate podman command arguments based on preprocessing keys."""
        self.logger.debug(f"Getting preprocessing args for file: {file_path}")
        if not self.has_preprocessing():
            self.logger.info("No preprocessing options found.")
            return None, None, None

        container = self.data_package.get("preprocessing_container")
        if not container:
            self.logger.warning(
                "No 'preprocessing_container' defined in data package.")
            raise ValueError(
                "Missing required 'preprocessing_container' in data package.")

        if not container.startswith("docker.io/"):
            container = "docker.io/" + container

        output_folder = self.data_package.get("preprocessing_outputfolder")
        input_file = self.data_package.get("preprocessing_inputfile")
        if not output_folder or not input_file:
            self.logger.warning(
                f"Missing output or input file: output_folder={output_folder}, input_file={input_file}")
            raise ValueError(
                "Missing required 'preprocessing_outputfolder' or 'preprocessing_inputfile' in data package.")

        kwargs = []
        mount_paths = []
        mount_path = None
        for key, value in self.data_package.items():
            if key.startswith("preprocessing_") and key not in ("preprocessing_container", "preprocessing_outputfolder", "preprocessing_altoutputfolder"):
                self.logger.debug(f"Found {key}:{value}")
                if isinstance(value, str) and "{Files}" in value: # handle inputfile = {Files}
                    self.logger.debug(f"Found Files in {key}:{value}")
                    if file_path:
                        data_file_path = os.path.join(
                            output_folder, os.path.basename(file_path))
                        self.logger.debug(
                            f"Replacing {file_path} ({data_file_path}) for Files in {key}:{value}")
                        value = value.replace("{Files}", data_file_path)
                        mount_path = os.path.dirname(file_path)
                        self.logger.debug(f"Set mount_path to {mount_path}")
                arg_key = key.replace("preprocessing_", "")
                kwargs.append(f"--{arg_key}")
                kwargs.append(value)
        self.logger.debug(f"Found extra preproc kwargs: {kwargs}")


        mount_paths.append((mount_path, output_folder))
        self.logger.debug(
            f"Output folder mount: {mount_path} --> {output_folder}")
        
        # Setup a processed subfolder for the processed outputs
        proc_output_folder = os.path.join(output_folder, PROCESSED_DATA_FOLDER)
        real_proc_output_folder = os.path.join(mount_path, PROCESSED_DATA_FOLDER)
        os.makedirs(real_proc_output_folder, exist_ok=True)
        self.logger.debug(f"Created processed subfolder at {real_proc_output_folder} and giving container --outputfolder {proc_output_folder}")
        kwargs += ["--outputfolder", proc_output_folder]
        
        alt_output_folder = self.data_package.get(
            "preprocessing_altoutputfolder")
        if alt_output_folder:
            kwargs += ["--altoutputfolder", alt_output_folder]
            relative_output_path = get_tmp_output_path(self.data_package)
            mount_paths.append((relative_output_path, alt_output_folder))
            self.logger.debug(
                f"Alt output folder mount: {relative_output_path} --> {alt_output_folder}")
        else:
            self.logger.error("Missing altoutputfolder. Not handled yet.")
            return None, None, None

        return container, kwargs, mount_paths

    def log_subprocess_output(self, pipe):
        for line in iter(pipe.readline, b''):
            self.logger.debug('sub: %r', line)

    def build_podman_command(self, file_path):
        """Construct the full Podman command based on preprocessing parameters."""
        container, kwargs, mount_paths = self.get_preprocessing_args(file_path)
        if not container:
            self.logger.warning("No container specified for podman command.")
            return None

        podman_settings = ["podman", "run", "--rm", "--userns=keep-id"]
        for src, dst in mount_paths:
            podman_settings += ["-v", f"{src}:{dst}"]
        podman_command = podman_settings + [container] + kwargs
        self.logger.info(f"Podman command: {' '.join(podman_command)}")
        return podman_command

    def run(self, dry_run=False):
        """Execute the podman command for preprocessing."""
        if not self.has_preprocessing():
            self.logger.info("No preprocessing required.")
            return True

        file_paths = self.data_package.get("Files", [])
        for file_path in file_paths:
            self.logger.info(f"Preprocessing file: {file_path}")
            podman_command = self.build_podman_command(file_path)
            if not podman_command:
                self.logger.error("Failed to build podman command.")
                return False

            if dry_run:
                self.logger.info(f"Dry run: {' '.join(podman_command)}")
                continue

            process = Popen(podman_command, stdout=PIPE, stderr=STDOUT)
            with process.stdout:
                self.log_subprocess_output(process.stdout)
            if process.wait() == 0:
                self.logger.info("Podman command executed successfully.")
            else:
                self.logger.error("Podman command failed.")
                return False
        return True


class DataPackageImporter:
    # TODO: Check if I can clean up this class. Its a bit messy.
    """
    Handles the import of data packages into OMERO using database-driven order details.
    """

    def __init__(self, config, data_package, ttl_for_user_conn=6000000):
        self.config = config
        self.data_package = data_package
        self.ttl_for_user_conn = ttl_for_user_conn
        self.logger = logging.getLogger(__name__)
        self.logger.debug(
            f"Initializing DataPackageImporter for UUID: {data_package.get('UUID', 'Unknown')}")
        self.host = os.getenv('OMERO_HOST')
        self.password = os.getenv('OMERO_PASSWORD')
        self.user = os.getenv('OMERO_USER')
        self.port = os.getenv('OMERO_PORT')

        # Validate environment variables
        if not all([self.host, self.password, self.user, self.port]):
            self.logger.error(
                "OMERO connection details missing from environment variables.")
            raise ValueError("Missing OMERO connection environment variables.")

        self.imported = False

    @connection
    def import_to_omero(self, conn, file_path, target_id, target_type, uuid, transfer_type="ln_s", depth=None):
        self.logger.debug(
            f"Starting import to OMERO for file: {file_path}, Target: {target_id} ({target_type})")
        cli = CLI()
        cli.register('import', ImportControl, '_')
        cli.register('sessions', SessionsControl, '_')
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
        if 'parallel_upload_per_worker' in self.config:
            arguments += ['--parallel-upload',
                          str(self.config['parallel_upload_per_worker'])]
        if 'parallel_filesets_per_worker' in self.config:
            arguments += ['--parallel-fileset',
                          str(self.config['parallel_filesets_per_worker'])]
        if self.config.get('skip_all', False):
            arguments += ['--skip', 'all']
        else:
            if self.config.get('skip_checksum', False):
                arguments += ['--skip', 'checksum']
            if self.config.get('skip_minmax', False):
                arguments += ['--skip', 'minmax']
            if self.config.get('skip_thumbnails', False):
                arguments += ['--skip', 'thumbnails']
            if self.config.get('skip_upgrade', False):
                arguments += ['--skip', 'upgrade']
        if depth:
            arguments += ['--depth', str(depth)]
        if target_type == 'Screen':
            arguments += ['-r', str(target_id)]
        elif target_type == 'Dataset':
            arguments += ['-d', str(target_id)]
        else:
            raise ValueError(
                "Invalid target_type. Must be 'Screen' or 'Dataset'.")
        arguments.append(str(file_path))
        cli.invoke(arguments)
        if cli.rv == 0:
            self.imported = True
            self.logger.info(f'Imported successfully for {str(file_path)}')
            return True
        else:
            self.imported = False
            self.logger.error(f'Import failed for {str(file_path)}')
            return False

    @connection
    def get_plate_ids(self, conn, file_path, screen_id):
        if not self.imported:
            self.logger.error(f'File {file_path} was not imported')
            return None
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
            "SELECT DISTINCT p.id, p.details.creationEvent.time, fs.templatePrefix FROM Plate p "
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
        # Extract Template Prefixes
        template_prefixes = [r[2].val for r in results]
        self.logger.debug(f"Plate id determined to be {plate_ids} by SQL query")
        return plate_ids, template_prefixes

    @connection
    def import_dataset(self, conn, target, dataset, transfer="ln_s", depth=None):
        kwargs = {"transfer": transfer}
        if 'parallel_upload_per_worker' in self.config:
            kwargs['parallel-upload'] = str(
                self.config['parallel_upload_per_worker'])
        if 'parallel_filesets_per_worker' in self.config:
            kwargs['parallel-fileset'] = str(
                self.config['parallel_filesets_per_worker'])
        if self.config.get('skip_all', False):
            kwargs['skip'] = 'all'
        if depth:
            kwargs['depth'] = str(depth)
        uuid = self.data_package.get('UUID')
        kwargs['file'] = f"logs/cli.{uuid}.logs"
        kwargs['errs'] = f"logs/cli.{uuid}.errs"
        self.logger.debug(f"EZImport: {conn} {target} {int(dataset)} {kwargs}")
        return ezomero.ezimport(conn=conn, target=target, dataset=int(dataset), **kwargs)

    def upload_files(self, conn, file_paths, dataset_id=None, screen_id=None, local_paths=None):
        uuid = self.data_package.get('UUID')
        if dataset_id and screen_id:
            raise ValueError("Cannot specify both dataset_id and screen_id.")
        if not dataset_id and not screen_id:
            raise ValueError(
                "Either dataset_id or screen_id must be specified.")

        successful_uploads = []
        failed_uploads = []
        self.logger.debug(f"Uploading files: {file_paths}")
        upload_target = dataset_id or screen_id

        for i, file_path in enumerate(file_paths):
            self.logger.debug(f"Uploading file: {file_path}")
            try:
                if screen_id:
                    if not local_paths:
                        imported = self.import_to_omero(
                            file_path=str(file_path),
                            target_id=screen_id,
                            target_type='Screen',
                            uuid=uuid,
                            depth=10
                        )
                        image_ids, _ = self.get_plate_ids(
                            str(file_path), screen_id)
                    else:
                        # If local_paths, we have done preprocessing 
                        # data is now in PROCESSED_DATA_FOLDER subfolder on remote storage
                        # and in local_paths folder on the omero server storage
                        # we will import now in-place from the omero server storage 
                        # and then we'll switch the in-place symlinks to the remote storage (subfolder)
                        fp = str(local_paths[i])
                        self.logger.debug(f"Importing {fp}")
                        imported = self.import_to_omero(
                            file_path=fp,
                            target_id=screen_id,
                            target_type='Screen',
                            uuid=uuid,
                            depth=10
                        )
                        self.logger.debug("Upload done. Retrieving plate id.")
                        image_ids, local_file_dir = self.get_plate_ids(
                            str(local_paths[i]), screen_id)

                        # rsync file to remote and point symlink there
                        # Ensure remote_path is the directory itself if file_path is a directory
                        remote_path = file_path if os.path.isdir(
                            file_path) else os.path.dirname(file_path)
                        # select the PROCESSED_DATA_FOLDER subfolder with processed data
                        remote_path = os.path.join(remote_path, PROCESSED_DATA_FOLDER)
                        
                        local_file_dir = local_file_dir[0].rstrip("/") + "/"
                        local_file_dir = "/OMERO/ManagedRepository/" + local_file_dir
                        # self.logger.debug(f"Move {local_file_dir} to {remote_path}")
                        # 1. Rsync the actual files to the remote location
                        # rsync_command = [
                        #     "rsync", "-av", "--copy-links",  # Copy actual files instead of symlinks
                        #     local_file_dir,  # Already guaranteed to have a trailing slash
                        #     remote_path
                        # ]
                        # self.logger.info(f"Rsync command: {rsync_command}")
                        # subprocess.run(rsync_command, check=True)
                        # 2. Update the symlinks to point to the remote location
                        self.logger.info(
                            f"Now update symlinks in {local_file_dir} to {remote_path}")
                        for root, _, files in os.walk(local_file_dir):
                            for file in files:
                                symlink_path = os.path.join(root, file)
                                # Only process symlinks
                                if os.path.islink(symlink_path):
                                    # Update symlink to point to remote location
                                    # Remove the old symlink
                                    os.unlink(symlink_path)
                                    new_target = os.path.join(
                                        remote_path, file)
                                    # Create the new symlink
                                    os.symlink(new_target, symlink_path)
                                    self.logger.debug(
                                        f"new symlinks {symlink_path} -> {new_target}")

                        # delete local copy in tmp out folder
                        relative_output_path = os.path.join(
                            "/OMERO", TMP_OUTPUT_FOLDER, self.data_package.get('UUID'))
                        if os.path.exists(relative_output_path):
                            self.logger.debug(
                                f"Removing temporary local {relative_output_path} folder")
                            shutil.rmtree(relative_output_path)
                        else:
                            self.logger.debug(
                                f"The folder {relative_output_path} does not exist.")
                    upload_target = screen_id
                else:
                    if os.path.isfile(file_path):
                        image_ids = self.import_dataset(
                            target=str(file_path),
                            dataset=dataset_id,
                            transfer="ln_s"
                        )
                        self.logger.debug(f"EZimport returned ids {image_ids} for {str(file_path)} ({dataset_id})")
                    elif os.path.isdir(file_path):
                        imported = self.import_to_omero(
                            file_path=str(file_path),
                            target_id=dataset_id,
                            target_type='Dataset',
                            uuid=uuid,
                            depth=10
                        )
                        image_ids = dataset_id
                        self.logger.debug(f"Set ids {image_ids} to the dataset {dataset_id}")
                    else:
                        raise ValueError(
                            f"{file_path} is not recognized as file or directory.")
                    upload_target = dataset_id

                if image_ids:
                    image_or_plate_id = max(image_ids) if isinstance(
                        image_ids, list) else image_ids
                    # Selecting 1 id, because this is a for-loop over files.
                    # This means we should only be getting back 1 ID per single upload.
                    self.logger.debug(f"Postprocessing ids {image_ids}: max ID = {image_or_plate_id}")
                    try:
                        self.add_image_annotations(
                            conn, image_or_plate_id, uuid, file_path, is_screen=bool(screen_id))
                        self.logger.info(
                            f"Uploaded file: {file_path} to target: {upload_target} with ID: {image_or_plate_id}")
                    except Exception as annotation_error:
                        self.logger.error(
                            f"Annotation failed for {file_path}: {annotation_error}")
                    successful_uploads.append(
                        (file_path, upload_target, os.path.basename(file_path), image_or_plate_id))
                else:
                    self.logger.error(
                        f"Upload rejected by OMERO for file {file_path}.")
                    failed_uploads.append(
                        (file_path, upload_target, os.path.basename(file_path), None))
            except Exception as e:
                self.logger.error(f"Error uploading file {file_path}: {e}")
                failed_uploads.append(
                    (file_path, upload_target, os.path.basename(file_path), None))
        return successful_uploads, failed_uploads

    @connection
    def add_image_annotations(self, conn, object_id, uuid, file_path, is_screen=False):
        try:
            annotation_dict = {'UUID': str(uuid), 'Filepath': str(file_path)}
            ns = "omeroadi.import"
            object_type = "Plate" if is_screen else "Image"
            metadata_file = self.data_package.get(
                'metadata_file', 'metadata.csv')
            metadata_path = os.path.join(
                os.path.dirname(file_path), metadata_file)
            if os.path.exists(metadata_path):
                self.logger.info(f"Reading metadata from {metadata_path}")
                with open(metadata_path, 'r', newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        if len(row) == 2:
                            key, value = row
                            if key:
                                annotation_dict[key] = value or ''
                        else:
                            self.logger.warning(f"Invalid metadata row: {row}")
            else:
                self.logger.info(f"No metadata found at {metadata_path}")
            self.logger.debug(f"Annotation dict: {annotation_dict}")
            map_ann_id = ezomero.post_map_annotation(
                conn=conn,
                object_type=object_type,
                object_id=object_id,
                kv_dict=annotation_dict,
                ns=ns,
                across_groups=False
            )
            if map_ann_id:
                self.logger.info(
                    f"Annotations added to {object_type} ID: {object_id}. MapAnnotation ID: {map_ann_id}")
            else:
                self.logger.warning(
                    f"Annotations created for {object_type} ID: {object_id}, but no ID returned.")
        except Exception as e:
            self.logger.error(
                f"Failed to add annotations to {object_type} ID: {object_id}: {e}")

    @connection
    def create_new_dataset(self, conn, name="New Dataset", description=""):
        dataset = DatasetI()
        dataset.setName(rstring(name))
        dataset.setDescription(rstring(description))
        dataset = conn.getUpdateService().saveAndReturnObject(dataset)
        dataset_id = dataset.getId().getValue()
        self.logger.info(f"Created new dataset with ID: {dataset_id}")
        return dataset_id

    def import_data_package(self):
        """Import the data package and log the outcome."""
        try:
            # Get DestinationID from DataPackage
            target_id = self.data_package.get('DestinationID')
            if not target_id:
                self.logger.error("No DestinationID provided")
                return [], [], True

            target_type = self.data_package.get('DestinationType')
            if not target_type:
                self.logger.error("No DestinationType provided")
                return [], [], True
            # Determine if the target ID is a Dataset or a Screen
            is_screen = target_type == "Screen"
            self.logger.info(
                f"Target ID {target_id} ({type(target_id)}) identified as {target_type}.")

            intended_username = self.data_package.get('Username')
            group_name = self.data_package.get('Group')

            if not (intended_username and group_name):
                self.logger.error(
                    "Missing required user or group information in data package.")
                return [], [], True
            # TODO: can we use this decorator instead? no?
            retry_count = 0
            while retry_count < MAX_RETRIES:
                try:
                    with BlitzGateway(self.user, self.password, host=self.host, port=self.port, secure=True) as root_conn:
                        if not root_conn.connect():
                            self.logger.error(
                                "Failed to connect to OMERO as root.")
                            return [], [], True
                        self.logger.info("Connected to OMERO as root.")
                        root_conn.keepAlive()
                        group_id = ezomero.get_group_id(
                            root_conn, group_name)  # grab w/ ezomero
                        with root_conn.suConn(intended_username, ttl=self.ttl_for_user_conn) as user_conn:
                            if not user_conn:
                                self.logger.error(
                                    f"Failed to connect as user {intended_username}")
                                return [], [], True
                            user_conn.keepAlive()
                            user_conn.setGroupForSession(group_id)
                            self.logger.info(
                                f"Connected as user {intended_username} in group {group_name}")

                            all_successful_uploads = []
                            all_failed_uploads = []
                            file_paths = self.data_package.get('Files', [])
                            self.logger.debug(f"Files to upload: {file_paths}")

                            processor = DataProcessor(
                                self.data_package, self.logger)
                            if processor.has_preprocessing():
                                # Setup a local tmp folder on the OMERO server itself
                                local_tmp_folder = get_tmp_output_path(
                                    self.data_package)
                                os.makedirs(local_tmp_folder, exist_ok=True)
                                log_ingestion_step(
                                    self.data_package, STAGE_PREPROCESSING)
                                if not processor.run(dry_run=False):
                                    self.logger.error("Preprocessing failed.")
                                    return [], [], True
                                self.logger.info(
                                    "Preprocessing succeeded; proceeding with upload.")

                                # Pass the target id based on its type; include local paths if preprocessed
                                if is_screen:
                                    successful_uploads, failed_uploads = self.upload_files(
                                        user_conn,
                                        file_paths,
                                        dataset_id=None,
                                        screen_id=target_id,
                                        local_paths=[local_tmp_folder]
                                    )
                                else:
                                    successful_uploads, failed_uploads = self.upload_files(
                                        user_conn,
                                        file_paths,
                                        dataset_id=target_id,
                                        screen_id=None,
                                        local_paths=[local_tmp_folder]
                                    )
                            else:
                                self.logger.info(
                                    "No preprocessing required; continuing upload.")
                                if is_screen:
                                    successful_uploads, failed_uploads = self.upload_files(
                                        user_conn,
                                        file_paths,
                                        dataset_id=None,
                                        screen_id=target_id
                                    )
                                else:
                                    successful_uploads, failed_uploads = self.upload_files(
                                        user_conn,
                                        file_paths,
                                        dataset_id=target_id,
                                        screen_id=None
                                    )
                            all_successful_uploads.extend(successful_uploads)
                            all_failed_uploads.extend(failed_uploads)
                            return all_successful_uploads, all_failed_uploads, False

                except Exception as e:
                    # TODO: can we use this decorator instead? no?
                    if "connect" in str(e).lower():
                        retry_count += 1
                        self.logger.error(
                            f"Connection issue (attempt {retry_count}/{MAX_RETRIES}): {e}")
                        if retry_count < MAX_RETRIES:
                            self.logger.info(
                                f"Retrying in {RETRY_DELAY} seconds...")
                            time.sleep(RETRY_DELAY)
                            continue
                    self.logger.error(
                        f"Error during import: {e}", exc_info=True)
                    return [], [], True

            self.logger.error(
                f"Max retries ({MAX_RETRIES}) reached during import")
            return [], [], True

        except Exception as e:
            self.logger.error(
                f"Error during import_data_package: {e}", exc_info=True)
            return [], [], True
