#!/bin/bash
set -e

# Install bdns_core from mounted volume if available
if [ -d "/app/bdns_core_package" ]; then
    echo "Installing bdns_core from mounted volume..."
    pip install -e /app/bdns_core_package
    echo "bdns_core installed successfully"
else
    echo "Warning: bdns_core_package not found at /app/bdns_core_package"
fi

# Execute the main command
exec "$@"
