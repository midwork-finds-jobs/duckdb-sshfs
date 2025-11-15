#!/usr/bin/env bash
# Note: DON'T run as root

docker compose -f scripts/sshfs.yml -p duckdb-sshfs up -d

# Get setup container name to monitor logs
container_name=$(docker ps -a --format '{{.Names}}' | grep -m 1 "duckdb-sshfs")
echo $container_name

# Wait for setup completion (up to 360 seconds like in webdav)
for i in $(seq 1 360);
do
  docker_finish_logs=$(docker logs $container_name 2>/dev/null | grep -m 1 'FINISHED SETTING UP SFTP-ONLY SERVER' || echo '')
  if [ ! -z "${docker_finish_logs}" ]; then
    echo "Setup completed successfully!"
    break
  fi
  sleep 1
done

export SSHFS_TEST_SERVER_AVAILABLE=1
export SSHFS_TEST_BASE_URL="sshfs://localhost:2222"
export SSHFS_TEST_SFTP_PORT=2223  # SFTP-only server port
