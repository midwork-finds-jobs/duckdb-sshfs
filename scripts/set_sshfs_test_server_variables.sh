#!/usr/bin/env bash
# This script sets environment variables for SSHFS test server
# Usage: source ./scripts/set_sshfs_test_server_variables.sh

# Check if test server container is running
if ! docker ps --format '{{.Names}}' | grep -q "duckdb-sshfs"; then
  echo "Warning: SSHFS test server doesn't appear to be running."
  echo "Run: ./scripts/run_sshfs_test_server.sh"
  return 1 2>/dev/null || exit 1
fi

# Set test server environment variables
export SSHFS_TEST_SERVER_AVAILABLE=1
export SSHFS_TEST_USERNAME=duckdb_sshfs_user
export SSHFS_TEST_PORT=2222
export SSHFS_TEST_BASE_URL="sshfs://localhost:2222"
export SSHFS_TEST_SFTP_PORT=2223