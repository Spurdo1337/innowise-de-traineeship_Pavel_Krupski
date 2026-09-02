#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for Airflow metadata database..."
until airflow db migrate; do
  echo "Airflow DB is not ready yet. Retrying in 5 seconds..."
  sleep 5
done

echo "Creating Airflow admin user..."
airflow users create \
  --username airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email airflow@example.com \
  --password airflow || true

echo "Configuring filesystem connection for FileSensor..."
airflow connections delete fs_default || true
airflow connections add fs_default \
  --conn-type fs \
  --conn-extra '{"path": "/opt/airflow/data/raw"}'

echo "Configuring MongoDB connection..."
airflow connections delete mongo_default || true
airflow connections add mongo_default \
  --conn-uri "mongodb://airflow:airflow@mongo:27017/reviews_db?authSource=admin"

mkdir -p /opt/airflow/data/raw
mkdir -p /opt/airflow/data/work
mkdir -p /opt/airflow/data/processed
mkdir -p /opt/airflow/logs

# Defensive: make sure mounted scripts are executable even if the git
# execute bit didn't survive the checkout (e.g. cloned on Windows, or with
# core.fileMode=false).
chmod +x /opt/airflow/scripts/*.sh || true

# Ensure runtime folders are writable by the Airflow user.
chown -R airflow:root /opt/airflow/data/raw /opt/airflow/data/work /opt/airflow/data/processed /opt/airflow/logs || true

echo "Reserializing DAGs so the UI can pick them up immediately..."
airflow dags reserialize

echo "Unpausing DAGs so they are active on first start..."
airflow dags unpause process_reviews || true
airflow dags unpause load_reviews_to_mongo || true

echo "Bootstrap finished successfully."
