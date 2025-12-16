# Use Bash for robust command execution
SHELL := /bin/bash

# Define the path to your Conda environment for clarity
CONDA_ENV_PREFIX := /home/jack_li/python/LOB_research/.conda
PYTHON_MODULE := hyperliquid.main

.PHONY: run start stop

# --- Foreground Execution ---
# Runs the module directly in the foreground.
# We use a single bash command ('/bin/bash -c') to ensure 'source', 'activate', 
# and 'python' run in the same shell session.
run:
	@echo "Activating Conda environment and running LOB research module (Foreground)..."
	/bin/bash -c 'source $$(conda info --base)/etc/profile.d/conda.sh && conda activate $(CONDA_ENV_PREFIX) && python -m $(PYTHON_MODULE)'

# --- Background Execution ---
# Runs the module in the background using 'nohup'. The '>>' ensures appending.
# We use the same single-shell command approach to handle 'conda activate'.
start:
	@echo "Starting LOB research module in background (nohup). Output is APPENDED to output.log."
	# Command execution within a single shell instance, finding the Conda initialization script dynamically
	nohup /bin/bash -c 'source $$(conda info --base)/etc/profile.d/conda.sh && conda activate $(CONDA_ENV_PREFIX) && python -m $(PYTHON_MODULE)' >> output.log 2>&1 &

# --- Utility to Stop the Background Process ---
# Finds the process running the main module and terminates it gracefully (SIGTERM).
stop:
	@echo "Attempting to stop LOB research process..."
	# Search for the Python process running the specific module
	pkill -f "python -m $(PYTHON_MODULE)" || echo "Process not found or already stopped."