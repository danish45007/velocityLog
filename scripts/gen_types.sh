#!/bin/bash

# Get the directory where the script resides
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Define the full path to the serialization.proto file in the root directory
PROTO_FILE="$SCRIPT_DIR/../proto/serialization.proto"

# Debugging path
echo "Looking for proto file at: $PROTO_FILE"

# Check if serialization.proto exists
if [ ! -f "$PROTO_FILE" ]; then
    echo "serialization.proto not found in $PROTO_FILE"
    exit 1
fi

# Run the protoc command with the proto_path and output directories
protoc --proto_path="$(dirname "$PROTO_FILE")" \
  --go_out="$SCRIPT_DIR/../" --go_opt=paths=source_relative \
  --go-grpc_out="$SCRIPT_DIR/../" --go-grpc_opt=paths=source_relative \
  "$(basename "$PROTO_FILE")"
