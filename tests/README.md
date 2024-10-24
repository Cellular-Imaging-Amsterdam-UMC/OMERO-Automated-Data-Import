# OMERO Automated Data Import - Test Environment Setup

This document explains how to set up a test environment for the OMERO Automated Data Import project using a Conda environment and a local dockerized OMERO instance.

TODO: Add here source for our dockerized omero
TODO: Add more info on the sys req like WSL

## Setting up the Conda Environment

We've created a script `setup_conda_test_env.sh` to automate the process of setting up a Conda environment for testing. This script does the following:

1. Checks if Conda is installed and provides installation instructions if it's not.
2. Creates a new Conda environment named `auto-import-env` with Python 3.10.
3. Installs necessary packages including `omero-py`, `bftools`, and `psycopg2`.
4. Clones the OMERO-Automated-Data-Import repository if it doesn't exist.
5. Installs additional requirements from the `requirements.txt` file.
6. Sets up logging directories and makes the main script executable.

To use this script:

1. Ensure you have Conda installed on your system.
2. Make the script executable: `chmod +x setup_conda_test_env.sh`
3. Run the script: `./setup_conda_test_env.sh`

You can also use the `--clean` flag to remove an existing environment before creating a new one: `./setup_conda_test_env.sh --clean`

## Local Dockerized OMERO

This test environment is designed to work with a local dockerized OMERO instance. Ensure you have Docker installed and a local OMERO instance running before proceeding with testing.

## Testing the Connection to OMERO

After setting up the Conda environment and ensuring your local OMERO instance is running, you can test the connection to OMERO. Here are some steps to do so:

1. Activate the Conda environment:
   ```
   conda activate auto-import-env
   ```

2. Log in to OMERO:
   ```
   omero login
   ```
   You'll be prompted for the server address, username, and password.

3. Try some sample commands:
   ```
   omero group list  # List groups
   omero user list   # List users
   omero proj list   # List projects
   ```

If these commands work successfully, your test environment is properly set up and connected to your local OMERO instance.

## Running Tests with Coverage

To run the tests with coverage, use the following command from the root directory of the project:

```
pytest tests/unittests/ --cov=src/
```

This command will run all the tests in the `tests/unittests/` directory and provide a coverage report for the `src/` directory.

Remember to deactivate the Conda environment when you're done testing:
```
conda deactivate
```

This setup allows you to test the OMERO Automated Data Import project in an environment that closely mirrors the production setup, ensuring that your tests are as accurate and relevant as possible.
