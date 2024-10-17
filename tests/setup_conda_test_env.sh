#!/bin/bash
# Make sure this shell script is executable by running:
# chmod +x setup_conda_test_env.sh

# Function to check if conda is installed and provide instructions if not
check_conda() {
    if ! command -v conda &> /dev/null
    then
        echo "Conda could not be found. Please install Conda and initialize it before running this script."
        echo "Here are the steps to install and initialize Conda:"
        echo "1. Download the Miniconda installer:"
        echo "   wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
        echo "2. Make the installer executable:"
        echo "   chmod +x Miniconda3-latest-Linux-x86_64.sh"
        echo "3. Run the installer:"
        echo "   ./Miniconda3-latest-Linux-x86_64.sh"
        echo "4. Follow the prompts in the installer"
        echo "5. Initialize Conda for your shell:"
        echo "   source ~/miniconda3/bin/activate"
        echo "6. Add Conda to your PATH by adding this line to your ~/.bashrc file:"
        echo "   export PATH=\"\$HOME/miniconda3/bin:\$PATH\""
        echo "7. Reload your shell configuration:"
        echo "   source ~/.bashrc"
        echo "8. Verify the installation:"
        echo "   conda --version"
        echo "After completing these steps, run this script again."
        exit 1
    fi
}

# Check if conda is installed
check_conda

# After running the script, activate the Conda environment before running any test scripts:
# conda activate auto-import-env
# Note: The --clean flag can be used to remove the existing environment before creating a new one.
# Usage: ./setup_conda_test_env.sh --clean
# This is useful for ensuring a fresh setup or resolving environment conflicts.

# Set the name for the Conda environment
ENV_NAME="auto-import-env"

# Check if the --clean flag is passed
if [[ "$1" == "--clean" ]]; then
    echo "Cleaning up existing environment..."
    conda env remove --name $ENV_NAME -y
    echo "Existing environment removed."
fi

# Create a new Conda environment with Python 3.10
conda create -n $ENV_NAME python=3.10 -y

# Initialize conda for shell interaction
eval "$(conda shell.bash hook)"

# Activate the new environment
conda activate $ENV_NAME

# Install packages from conda-forge and bioconda
conda install -c conda-forge omero-py -y
conda install -c bioconda bftools -y
conda install -c conda-forge psycopg2 libffi==3.3 -y

# Install git (if not already installed)
command -v git >/dev/null 2>&1 || { conda install git -y; }

# Install the package in editable mode
pip install -e .[test]

# Make the main script executable TODO: Check if this is necessary and weather other files need to be made executable
chmod +x src/main.py

echo "Conda environment '$ENV_NAME' has been set up and activated."
echo "You can now test your container setup in this environment."
echo "To activate the environment, run: conda activate $ENV_NAME"
echo "To deactivate the environment, use 'conda deactivate'."
