import json
import time
import logging
from prometheus_api_client import PrometheusConnect, PrometheusConnectError
import pandas as pd

# Load the Prometheus setup details from setup.json
with open('setup.json', 'r') as setup_file:
    setup = json.load(setup_file)

prometheus_url = setup.get("prometheus_url", "http://localhost")
port = setup.get("port", 9090)
https = setup.get("https", False)
username = setup.get("username", "")
password = setup.get("password", "")
interval_seconds = setup.get("interval_seconds", 60)

# Configure logging based on setup.json
log_file = setup.get("log_file", "prometheus_read.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Construct the Prometheus URL with port number
prometheus_url_with_port = f"{prometheus_url}:{port}"

try:
    # Create a Prometheus connection with optional HTTPS and authentication
    prom = PrometheusConnect(url=prometheus_url_with_port, use_https=https, user=username, password=password)
    logging.info("Connected to Prometheus")
except PrometheusConnectError as e:
    logging.error(f"Error connecting to Prometheus: {e}")
    raise SystemExit(1)

# Load the configuration from config.json
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Create a dictionary to hold in-memory tables
tables = {}

# Continuously read data from Prometheus with error handling
while True:
    try:
        for query_config in config.get("queries", []):
            query_name = query_config.get("name", "")
            prom_query = query_config.get("query", "")

            if not query_name or not prom_query:
                logging.warning("Skipping invalid query configuration")
                continue

            # Execute the Prometheus query
            data = prom.custom_query(query=prom_query)

            # Convert the data to a Pandas DataFrame
            df = pd.DataFrame(data)

            # Store the DataFrame in the tables dictionary
            tables[query_name] = df

        # Sleep for the specified interval before fetching data again
        time.sleep(interval_seconds)
    except Exception as e:
        logging.error(f"Error fetching data from Prometheus: {e}")
