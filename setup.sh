#!/bin/bash

# Check Python version
python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
if [[ $(echo "$python_version < 3.12" | bc) -eq 1 ]]; then
    echo "Error: Python 3.12 or higher is required"
    exit 1
fi

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install required packages from requirements.txt
pip install -r requirements.txt

# Create necessary directories
mkdir -p models
mkdir -p data

echo "Setup completed successfully!"
echo "Python version: $(python --version)"
echo "Please activate the virtual environment using: source venv/bin/activate" 