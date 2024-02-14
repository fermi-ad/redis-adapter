#!/bin/sh

# Start the process
create-cluster start

# Wait for a bit to ensure the service is ready
echo "Waiting for the service to be ready..."
sleep 5 # Waits for 5 seconds; adjust this value as needed

# Check if the start command succeeded
if [ $? -eq 0 ]; then
    echo "Start command executed successfully. Proceeding to create command."
    # Create the cluster
    create-cluster create

    # Check if the create command succeeded
    if [ $? -eq 0 ]; then
        echo "Create command executed successfully. Tailing /dev/null."
        # Keep the container running
        tail -f /dev/null
    else
        echo "Create command failed. Exiting."
        exit 1
    fi
else
    echo "Start command failed. Exiting."
    exit 1
fi

