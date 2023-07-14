#!/bin/sh

echo "downloding localstack dependency"
pip install localstack-client

echo  "starting python script"
python /tmp/scripts/create_write_queue.py

echo "winding down"
