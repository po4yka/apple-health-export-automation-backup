#!/bin/bash
set -e

# Configuration
BACKUP_ROOT="/backups"
RETENTION_DAYS=${BACKUP_RETENTION_DAYS:-7}
INFLUX_HOST=${INFLUXDB_URL:-"http://influxdb:8086"}
DATE=$(date +%Y-%m-%d_%H-%M-%S)
BACKUP_DIR="${BACKUP_ROOT}/${DATE}"

echo "[${DATE}] Starting backup routine..."

mkdir -p "${BACKUP_DIR}"

# 1. InfluxDB Backup
echo "Starting InfluxDB backup..."
if influx backup "${BACKUP_DIR}/influxdb" --host "${INFLUX_HOST}" --token "${INFLUXDB_TOKEN}"; then
    echo "InfluxDB backup success."
else
    echo "InfluxDB backup failed!"
    exit 1
fi

# 2. Raw Archive Backup
# We assume the raw archives are mounted at /data/archive
if [ -d "/data/archive" ]; then
    echo "Starting Raw Archive backup..."
    tar -czf "${BACKUP_DIR}/archive.tar.gz" -C /data archive
    echo "Raw Archive backup success."
else
    echo "Warning: /data/archive not found, skipping file backup."
fi

# 3. Deduplication DB Backup
if [ -f "/data/dedup/cache.db" ]; then
    echo "Backing up Dedup cache..."
    cp "/data/dedup/cache.db" "${BACKUP_DIR}/dedup_cache.db"
fi

# 4. Cleanup Old Backups
echo "Cleaning up backups older than ${RETENTION_DAYS} days..."
find "${BACKUP_ROOT}" -mindepth 1 -maxdepth 1 -type d -mtime +${RETENTION_DAYS} -exec rm -rf {} +
echo "Cleanup complete."

echo "[$(date)] Backup routine finished successfully."
