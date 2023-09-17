import json
import logging
from confluent_kafka import Consumer, KafkaError
import time
from multiprocessing import Process, Queue
import psutil
from collections import deque

# Configure logging
logging.basicConfig(filename='kafka_stream.log', level=logging.ERROR)

# Load configuration from config.json
with open("config.json") as config_file:
    config_data = json.load(config_file)

# Extract configuration values
data_frame_name = config_data["data_frame_name"]
memory_threshold_percent = config_data["memory_threshold_percent"]
topic = config_data["topic"]
output_topic = config_data["output_topic"]  # Added output topic

# Initialize Kafka consumer with the remaining configuration
kafka_config = config_data["kafka_config"]
consumer = Consumer(kafka_config)

# Subscribe to the Kafka topic
consumer.subscribe([topic])

# Initialize an in-memory cache to store aggregated data
output_cache = {}

# Initialize a multiprocessing queue for sharing data
output_queue = Queue()

# Extract column names dynamically from the select statement
select_statement = config_data["select_statement"].upper()
select_columns = [col.strip() for col in select_statement.split("SELECT")[1].split("FROM")[0].split(",")]

# Define the table name based on the Kafka topic
table_name = topic

# Function to update the cache based on incoming data
def update_cache(key, value):
    global output_cache
    data = {col: value.get(col, None) for col in select_columns}
    if table_name not in output_cache:
        output_cache[table_name] = []
    output_cache[table_name].append(data)
    output_queue.put(data)  # Send data to the output queue

# Function to check and adjust memory usage
def check_memory_usage():
    memory_usage = psutil.virtual_memory().percent
    if memory_usage > memory_threshold_percent:
        logging.error(f"High memory usage detected: {memory_usage}%")
        cleanup_cache()

# Function to clean up the cache based on a sliding window strategy
def cleanup_cache():
    global output_cache
    if table_name in output_cache:
        data_list = output_cache[table_name]
        new_data_list = []
        new_timestamp_queue = deque(maxlen=None)
        cache_retention_seconds = config_data.get("cache_retention_seconds", 3600)
        timestamp_threshold = time.time() - cache_retention_seconds
        for data, timestamp in zip(data_list, timestamp_queue):
            if timestamp >= timestamp_threshold:
                new_data_list.append(data)
                new_timestamp_queue.append(timestamp)
        output_cache[table_name] = new_data_list
        timestamp_queue = new_timestamp_queue

# Function to stream data from the cache
def stream_data_from_cache(table_name):
    if table_name in output_cache:
        data = output_cache[table_name]
        output_queue.put(data)  # Send data to the output queue

# Define a function for a separate process that streams data from the queue
def stream_data_process():
    while True:
        data = output_queue.get()
        if data is not None:
            # Implement your logic to process the data or feed it to multiple sources
            print(data)  # For demonstration, print the data to the terminal

# Create a separate process for streaming data
streaming_process = Process(target=stream_data_process)
streaming_process.start()

# Continuously process Kafka messages
while True:
    check_memory_usage()
    msg_batch = consumer.consume(num_messages=100)
    for msg in msg_batch:
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logging.error(f"Error while consuming message: {msg.error()}")
                break
        try:
            key = msg.key()
            value = json.loads(msg.value().decode('utf-8'))
            update_cache(key, value)
            consumer.commit(msg)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            consumer.commit(msg)

    cache_cleanup_interval_seconds = config_data.get("cache_cleanup_interval_seconds", 300)
    if time.time() % cache_cleanup_interval_seconds == 0:
        cleanup_cache()

==========================Configjson===========================


{
    "data_frame_name": "kafka_input",
    "memory_threshold_percent": 80,
    "topic": "your_kafka_topic",
    "output_topic": "your_output_topic",
    "timestamp_column": "timestamp",
    "timestamp_format": "%Y-%m-%d %H:%M:%S",
    "window_interval_seconds": 60,
    "select_statement": "SELECT column1, column2 FROM your_table WHERE ...",
    "auto_recovery": true,
    "cache_cleanup_interval_seconds": 300,
    "cache_retention_seconds": 3600,  // Cache retention period in seconds
    "kafka_config": {
        "bootstrap.servers": "your_bootstrap_servers",
        "group.id": "your_group_id",
        "auto.offset.reset": "earliest",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "GSSAPI",
        "sasl.kerberos.keytab": "/path/to/your/keytab.keytab",
        "sasl.kerberos.principal": "your_service_principal@REALM",
        "ssl.truststore.location": "/path/to/truststore.jks",
        "ssl.truststore.password": "truststore_password",
        "ssl.keystore.location": "/path/to/keystore.jks",
        "ssl.keystore.password": "keystore_password",
        "ssl.key.password": "key_password"
    }
}
