#!/bin/bash

# Source the environment variables
source ./.env

# Get the list of directories containing .parquet files
IFS=$'\n' # Split on new line
for dir in $(ssh -i $PEM_FILE $REMOTE_HOST "find $REMOTE_DIR -type f -path  '$PATH_PATTERN' -name '*.parquet'")
do
    # Extract the file path and base directory
    file_path=$(dirname "$dir")       # Gets the directory path
    file_name=$(basename "$dir")      # data.parquet
    partition_dir=$(basename "$file_path") # e.g., 2025-04-20T19.17071
    table_dir=$(basename $(dirname "$file_path"))

    # Form the new local filename
    new_file_name="${table_dir}.${partition_dir}.${file_name}"  # e.g., 2025-04-20T19.17071.data.parquet

    echo "Copying to $new_file_name"
    # Copy and rename the file from remote to local directory
    scp -i $PEM_FILE "${REMOTE_HOST}:${dir}" "${LOCAL_DIR}/${new_file_name}"
done
