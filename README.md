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


