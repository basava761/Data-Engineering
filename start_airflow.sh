#!/bin/bash

echo "Starting Airflow..."

# Go to airflow project
cd ~/airflow_project || exit

# Activate virtual environment
source airflow_venv/bin/activate

# Set airflow home
export AIRFLOW_HOME=~/airflow

# Start webserver
echo "Starting Airflow Webserver..."
airflow webserver --port 8080 > ~/airflow_webserver.log 2>&1 &

# Start scheduler
echo "Starting Airflow Scheduler..."
airflow scheduler > ~/airflow_scheduler.log 2>&1 &

echo "Airflow started successfully!"
echo "Open browser: http://localhost:8080"
