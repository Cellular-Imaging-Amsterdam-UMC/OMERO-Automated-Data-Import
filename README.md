# OMERO Automated Data Import (ADI) System

Automated Data Imports (ADI) system enables uploading image data from microscope workstations to an OMERO server. ADI is modular and configurable system that uses one of two available intake systems (which determine how the users chooses files to import), and a configurable ingestion system: 

**Intake system 1: Matlab-clone of OMERO Insight** 

Developed specifically to import LIF files correctly. Designed to be installed in the workstations of a laboratory proximal to the imaging equipment. Has a GUI which prompts users to login with their OMERO credential and allows them to select files to be uploaded to an OMERO group of their choice. The program separates LIFs dataset files into single image, whcih has two advantages:

- It prevents OMERO making a fileset of the datasets, which would causes data management and analysis restrictions.
- Maintains the display settings and correct metadata, eliminating the need for post-upload processing.


**Intake system 2: Drag and drop functionality**
    
For all other files. Development not completed.

**Ingestion system: Imports files selected via the intake system and keeps a database of import history**

The modules of the ingestion system allow communication with the intake system being used. Both systems cerate 'upload orders', which describe to the system the user uploading files, the paths of the files to upload, and the group where the user wants to upload them.

---
This ADI system is built to work with a local network (LAN) which is accessible both by the microscope work stations and the OMERO server. Furthermore the deployment of the auto-importer is containerized and configured for deployment in concert with BIOMERO. The system is confiured for in-place imports and leaves the original data in the LAN hidden (yet still accessible) the user. 

This app built in python surveys the upload_orders_dir for new orders produced by the intake system. New orders are verified and then used to coordinate in-place imports through ezomero. Each order will be uploaded within one OEMRO Dataset. The dataset name , destination group, and owner are all specified in the upload order. The uuid of the order is used to keep track of the 'ingestion' process where the progress of each upload is recorded in a database.

#### Here explanation of the folder system chosen and the settings
```
# Example upload order

UUID: 539b3030-5bd4-483c-8ed7-0c542c5187b2
Group: Demo
Username: rrosas
Dataset: 2024_05_13_15-32-58
Files: [/divg/coreReits/Screens/TestM/Plate1_A1_image1.tif,/divg/coreReits/.omerodata/2024/05/13/15-32-58/Test__image1__image1.lif]

```

## Configure and Run

#TODO: Add proper link to BIOMERO documentation and (once completed) Ansible deployment script.
For system requirements, refer to the main BIOMERO documentation for detailed system requirements and dependencies. 



Prepare the configuration of the ADI system using the tools in the set_up directory. The groups in omero must be represented by folders in the storage from where the inplace uploads will be made. Note that the group names in omero and the names in the folder system do not need to match. View the sample group_list.json for a clear example of the required configuration.

Filling in the field in the .env and the "core_grp_name" field in the group_lis.json will be requiered. Additionally, the process to create folders for each group and and for each group member will have to be done in accordance to your institutional workflows.


## Checking the Setup

To check the setup, you can execute the following commands:

1. Enter the running `omeroadi` container:

```bash
docker exec -it omeroadi /bin/bash
```

2. Run the test script:
```bash
python tests/t_main.py
```

This will help verify that the setup is correct and the system is functioning as expected by uploading a sample TIF image to the omero server.
Upload logs will be visible in the console and if successful, the image should be visible via omero web in the 

## Unit Tests

To run the pytest unit tests:

1. Create a virtual environment:
   ```bash
   python -m venv venv
   ```
2. Install the right IcePy wheel directly for your [os & py version](https://www.glencoesoftware.com/blog/2023/12/08/ice-binaries-for-omero.html)

   ```bash
   #Linux - python 3.11.5 (does not work:  not a supported wheel on this platform)
   venv/bin/python -m pip install https://github.com/glencoesoftware/zeroc-ice-py-linux-aarch64/releases/download/20240620/zeroc_ice-3.6.5-cp311-cp311-manylinux_2_28_aarch64.whl

   # Windows - python 3.10.0
   venv/Scripts/python -m pip install https://github.com/glencoesoftware/zeroc-ice-py-win-x86_64/releases/download/20240325/zeroc_ice-3.6.5-cp310-cp310-win_amd64.whl
   ```
3. Install this package (editable):
   ```bash
   venv/Scripts/python -m pip install --editable .
   ```
4. Run the unit tests with pytest:
   ```bash
   venv/Scripts/pytest .\tests\unittests\
   ```


