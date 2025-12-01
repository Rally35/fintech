#!/bin/bash
set -e

# Default settings if environment variables are not provided
HOST="${DB_HOST:-db}"
PORT="${DB_PORT:-5432}"
USER="${DB_USER:-postgres}"

echo "Waiting for the Postgres database at $HOST:$PORT..."

# Loop waiting for the database to be ready
# Requires the postgresql-client package (included in the Dockerfile)
until pg_isready -h "$HOST" -p "$PORT" -U "$USER"; do
  >&2 echo "Postgres is unavailable - waiting..."
  sleep 2
done

>&2 echo "Postgres is ready! Running the data update script..."

# Run the consolidated Python script to update all data
python scripts/update_all_data.py

echo "Script execution finished. Starting the application..."

# Execute the main command passed in CMD (Streamlit)
exec "$@"