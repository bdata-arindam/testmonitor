#!/bin/bash

# Stop the Kafka to Prometheus exporter
exporter_pid=$(ps aux | grep 'kafka_to_prometheus_exporter.py' | grep -v grep | awk '{print $2}')

if [ -n "$exporter_pid" ]; then
  echo "Stopping Kafka to Prometheus exporter (PID: $exporter_pid)..."
  kill -SIGTERM $exporter_pid
  wait $exporter_pid
  echo "Kafka to Prometheus exporter stopped."
else
  echo "Kafka to Prometheus exporter is not running."
fi

# Optionally, you can add more commands or cleanup logic here

# Exit the script
exit 0
