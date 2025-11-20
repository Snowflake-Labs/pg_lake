#!/bin/bash

set -euo pipefail

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

# Create test bucket if it doesn't exist
if ! awslocal s3 ls s3://testbucket 2>/dev/null; then
    echo "Creating S3 bucket: testbucket"
    awslocal s3 mb s3://testbucket
    echo "Bucket created successfully"
else
    echo "Bucket testbucket already exists"
fi

echo "S3 initialization complete!"