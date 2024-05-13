# OMERO Automated Data Import

This Automated Data Imports (ADI) system enables automatic uploads from the local network to an OMERO server. ADI uses two main components: the lif-pre-processer and the auto-importer. Toguether, they allow users to select LIF datasets to upload to OMERO.

### LAN and configurations

This ADI system is built to work with a local network (LAN) which is accessible both by the microscope work stations and the OMERO server. Furthermore the deployment of the auto-importer is containerized and configured for deployment in concert with BIOMERO. The system is setup for in-place imports and leaves the original data in the LAN hidden (yet still accessible) the user.

### lif-pre-processing

This light weight matlab application provides the user a GUI to select LIF (.lif) datasets. The user-selected dataset is separated into single images. Separating these datasets into LIFs containing a single image has two advantages:

- It prevents OMERO making a fileset of the datasets, which would causes data management and analysis restrictions.
- Maintains the display settings and correct metadata, obviating the need of post upload processing and metadata parsing.

The LIF-pre-processing will also create an upload order that will dictate to the auto-importer what files to upload and to what group/user.

```
# Example upload order

UUID: 539b3030-5bd4-483c-8ed7-0c542c5187b2
Group: Demo
Username: rrosas
Dataset: 2024_05_13_15-32-58
Files: [/divg/coreReits/Screens/TestM/Plate1_A1_image1.tif,/divg/coreReits/.omerodata/2024/05/13/15-32-58/Test__image1__image1.lif]

```

### auto-importer

This app built in python surveys the upload_orders_dir for new orders produced by lif-pre-provessor. New orders are verified and then used to coordinate in-place imports through ezomero. Each order will be uploaded within one OEMRO Dataset. The dataset name , destination group, and owner are all specified in the upload order. The uuid of the order is used to keep track of the 'ingestion' process where the progress of each upload is recorded in a database.

## Configure and Run

Prepare the configuration of the ADI system using the tools in the set_up directory. The groups in omero must be represented by folders in the storage from where the inplace uploads will be made. Note that the group names in omero and the names in the folder system do not need to match. View the sample group_list.json for a clear example of the required configuration.

Filling in the field in the .env and the "core_grp_name" field in the group_lis.json will be requiered. Additionally, the process to create folders for each group and and for each group member will have to be done in accordance to your institutional workflows.
