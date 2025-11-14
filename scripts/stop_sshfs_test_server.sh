#!/usr/bin/env bash

echo "Stopping SSHFS test server..."
docker compose -f scripts/sshfs.yml -p duckdb-sshfs down

echo "SSHFS test server stopped."
