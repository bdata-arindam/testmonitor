#!/bin/bash

# Exporter script name (used as a primary label in Prometheus metrics)
exporter_name="kafka_to_prometheus_exporter"

# Start the Kafka to Prometheus exporter and capture its PID
nohup python3 kafka_to_prometheus_exporter.py > exporter.log 2>&1 &
exporter_pid=$!

echo "Kafka to Prometheus exporter started with PID: $exporter_pid"
echo "You can view the exporter's logs in exporter.log"

# Function to collect and export script execution process metrics to Prometheus
function export_process_metrics {
  while true; do
    # Get CPU and memory utilization of the exporter process
    cpu_percent=$(ps -p $exporter_pid -o %cpu | awk 'NR==2')
    memory_percent=$(ps -p $exporter_pid -o %mem | awk 'NR==2')

    # Send metrics to Prometheus
    echo "$exporter_name_cpu_usage{script=\"$exporter_name\"} $cpu_percent" | curl --data-binary @- http://localhost:9090/metrics/job/$exporter_name/instance/localhost
    echo "$exporter_name_memory_usage{script=\"$exporter_name\"} $memory_percent" | curl --data-binary @- http://localhost:9090/metrics/job/$exporter_name/instance/localhost

    sleep "$aggregation_interval_minutes"m
  done
}

# Get the aggregation interval from query.json
aggregation_interval_minutes=$(jq -r '.aggregation_interval_minutes' query.json)

# Start the process metrics export function in the background
export_process_metrics &

# Optionally, you can add more commands or monitoring logic here
# For example, you can set up alerts or monitoring of the exporter process

# Exit the script
exit 0
