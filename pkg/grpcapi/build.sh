#!/bin/bash

# Debug: Indicate the script has started
echo "Starting build.sh script"

# Get the directory where the script resides
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Debug: Show the resolved script directory
echo "Script directory resolved to: $SCRIPT_DIR"

# Define the full path to the api.proto file
PROTO_FILE="$SCRIPT_DIR/api.proto"

# Debug: Show the full path to the api.proto file
echo "Proto file path: $PROTO_FILE"

# Check if the api.proto file exists
if [ ! -f "$PROTO_FILE" ]; then
  echo "Error: $PROTO_FILE does not exist."
  exit 1
fi

# Debug: Indicate protoc command is about to run
echo "Running protoc command..."

# Run the protoc command with the proto_path and output directories
protoc --proto_path="$SCRIPT_DIR" \
  --go_out="$SCRIPT_DIR" --go_opt=paths=source_relative \
  --go-grpc_out="$SCRIPT_DIR" --go-grpc_opt=paths=source_relative \
  "$(basename "$PROTO_FILE")"

# Debug: Check if protoc command succeeded
if [ $? -eq 0 ]; then
  echo "protoc command executed successfully!"
else
  echo "Error: protoc command failed."
  exit 1
fi

# Debug: Script completed
echo "build.sh script completed."
