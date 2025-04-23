#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

echo "Creating virtual environment '.hf' with Python 3.11..."
uv venv .tracks --python 3.13

echo "Activating environment for script execution..."
source .tracks/bin/activate

echo "Installing packages from requirements.txt..."
uv pip install -r requirements.txt

echo "Setup complete. To activate the environment in your terminal, run: source .tracks/bin/activate"