#!/bin/bash

deactivate
# Create a python virtual environment
python3 -m venv slj
# Activate the virtual environment
source slj/bin/activate
# Upgrade pip
pip install --upgrade pip
# Install the required packages
pip install -r requirements.txt

mkdir -p .tmp
mv DuplicateDetective.py .tmp/
mv requirements.txt .tmp/