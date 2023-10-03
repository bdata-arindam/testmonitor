import json
import subprocess
import threading
import time
import logging

# Load setup configuration from setup.json
with open('setup.json', 'r') as setup_file:
    setup = json.load(setup_file)

log_file = setup.get("log_file", "main_program.log")  # Log file location and name

# Configure logging based on setup.json
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to run a subprocess (e.g., a Python script)
def run_subprocess(script_name):
    try:
        subprocess.check_call(["python", script_name])
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script_name}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error running {script_name}: {e}")

# Function to start and manage Prometheus read script
def start_prometheus_read():
    while True:
        logging.info("Starting prometheus_read.py")
        run_subprocess("prometheus_read.py")
        logging.info("prometheus_read.py exited, restarting in 5 seconds")
        time.sleep(5)

# Function to start and manage Kafka producer script
def start_kafka_producer():
    while True:
        logging.info("Starting prom_con_pro.py")
        run_subprocess("prom_con_pro.py")
        logging.info("prom_con_pro.py exited, restarting in 5 seconds")
        time.sleep(5)

# Create threads for Prometheus read and Kafka producer scripts
prometheus_read_thread = threading.Thread(target=start_prometheus_read)
kafka_producer_thread = threading.Thread(target=start_kafka_producer)

# Start the threads
prometheus_read_thread.start()
kafka_producer_thread.start()

# Wait for the threads to complete (which won't happen)
prometheus_read_thread.join()
kafka_producer_thread.join()
