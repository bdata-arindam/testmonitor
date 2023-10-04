import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, KafkaError
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway, generate_basic_auth
import datetime
import logging.handlers  # Import the logging.handlers module
import time  # Import the time module

# Load Kafka configuration from JSON file (kafka.json)
with open('kafka.json', 'r') as kafka_config_file:
    kafka_config = json.load(kafka_config_file)

# Extract log file location and name from Kafka configuration
log_file_location = kafka_config.get('log_file_location', '/var/log')
log_file_name = kafka_config.get('log_file_name', 'exporter.log')

# Configure the logging module with daily log file rotation and monthly compression
log_file_path = os.path.join(log_file_location, log_file_name)

log_handler = logging.handlers.TimedRotatingFileHandler(
    filename=log_file_path,
    when="midnight",
    interval=1,
    backupCount=30  # Keep log files for 30 days
)

log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

# Load query configuration from JSON file (query.json)
with open('query.json', 'r') as query_config_file:
    query_config = json.load(query_config_file)

# Extract query parameters
default_sql_query = query_config['sql_query']
aggregation_interval_minutes = query_config['aggregation_interval_minutes']
start_time_iso = query_config['start_time']
include_message_key = query_config.get('include_message_key', True)  # Default to True if not specified

# Allow users to specify an optional custom query
custom_sql_query = input("Enter a custom SQL query (or press Enter to use the default): ")
sql_query = custom_sql_query if custom_sql_query else default_sql_query

# Extract delay configuration from Kafka configuration
delay_seconds = kafka_config.get('delay_seconds', 0)  # Default to 0 if not specified

# Create a Kafka consumer
consumer_config = {
    'bootstrap.servers': kafka_config['server'],
    'group.id': kafka_config['group_id'],
    'security.protocol': kafka_config['security_protocol'],
    'sasl.mechanism': kafka_config['sasl_mechanism'],
    'sasl.kerberos.service.name': kafka_config['sasl_kerberos_service_name'],
    'sasl.kerberos.keytab': kafka_config['sasl_kerberos_keytab'],
    'ssl.checkmode': 'none' if not kafka_config['ssl_check_hostname'] else 'require',
    'ssl.keystore.location': kafka_config['ssl_keystore_location'],
    'ssl.keystore.password': kafka_config['ssl_keystore_password'],
    'ssl.truststore.location': kafka_config['ssl_truststore_location'],
    'ssl.truststore.password': kafka_config['ssl_truststore_password'],
    'enable.auto.commit': False
}

# Function to process Kafka messages
def process_kafka_messages():
    consumer = Consumer(consumer_config)
    gauge = Gauge('kafka_data', 'Kafka Data', ['aggregation_key', 'timestamp', 'key'])

    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error('Error while polling messages: %s', msg.error())
                    continue

            message_data = json.loads(msg.value())
            timestamp_str = message_data.get("timestamp", None)

            if timestamp_str:
                timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
                start_time = datetime.datetime.strptime(start_time_iso, '%Y-%m-%dT%H:%M:%SZ')

                time_difference = (timestamp - start_time).total_seconds()
                aggregation_key = message_data.get("key")

                for key, value in message_data.items():
                    if include_message_key:
                        gauge.labels(aggregation_key=aggregation_key, timestamp=timestamp_str, key=key).set(value)
                    else:
                        if key != "timestamp" and key != "key":
                            gauge.labels(aggregation_key=aggregation_key, timestamp=timestamp_str, key=key).set(value)

        except KeyboardInterrupt:
            break

    consumer.close()

# Continuous execution loop without introducing a sleep
while True:
    current_time = datetime.datetime.utcnow()

    # Calculate the start time for the query with the specified delay
    start_time = current_time - datetime.timedelta(seconds=delay_seconds)
    start_time_iso = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Update the query configuration with the new start time
    query_config['start_time'] = start_time_iso

    process_kafka_messages()