# Import necessary libraries and modules
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, KafkaError
import pandas as pd
import time
import psutil
import queue
import streamlit as st  # Added Streamlit import

# Global dictionary to store consumer threads
consumer_threads = {}

# Global dictionary to maintain a rolling buffer of records
timestamp_buffer = {}

# Function to process messages and apply dynamic filtering
def process_message(msg, select_query, in_memory_table):
    try:
        message_data = json.loads(msg.value().decode('utf-8'))

        # Parse the conditions from the SELECT query
        conditions = select_query.split("WHERE")[1].strip()

        # Construct a Python expression by converting SQL-like conditions
        condition_expression = conditions.replace("AND", "and").replace("OR", "or").replace("=", "==")

        # Evaluate the expression for each message
        if eval(condition_expression, globals(), message_data):
            in_memory_table.append(message_data)

        timestamp = message_data.get('timestamp')

        # Check if the timestamp is already in the buffer
        if timestamp not in timestamp_buffer:
            timestamp_buffer[timestamp] = []

        # Add the message data to the buffer
        timestamp_buffer[timestamp].append(message_data)

        # Ensure the buffer does not exceed the maximum number of records per timestamp
        max_records_per_timestamp = 10
        if len(timestamp_buffer[timestamp]) > max_records_per_timestamp:
            timestamp_buffer[timestamp].pop(0)  # Remove the oldest record
    
    except json.JSONDecodeError as e:
        print("Error decoding JSON message: {}".format(e))

# Function to configure logging
def configure_logging(log_file):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    log_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    return logger

# Function to add or remove consumer threads
def add_consumer_threads(num_threads_to_add):
    global consumer_threads

    # Lock to ensure thread-safe access to consumer_threads dictionary
    lock = threading.Lock()

    if num_threads_to_add > 0:
        with lock:
            for _ in range(num_threads_to_add):
                # Create and start a new consumer thread
                thread = threading.Thread(target=consume_messages, args=(cluster_name, consumer, select_query, in_memory_table))
                thread.daemon = True
                thread.start()
                # Store the thread in the dictionary
                consumer_threads[thread.ident] = thread
    elif num_threads_to_add < 0:
        with lock:
            # Terminate and remove the specified number of consumer threads
            num_threads_to_remove = abs(num_threads_to_add)
            for _ in range(num_threads_to_remove):
                if consumer_threads:
                    _, thread = consumer_threads.popitem()  # Remove and get the last thread
                    thread.join()  # Wait for the thread to finish

                    # Clean up resources (close Kafka consumer, etc.)
                    cleanup_consumer_resources(thread)

# Function to clean up consumer resources
def cleanup_consumer_resources(thread):
    # Implement your logic to clean up resources associated with the consumer
    # For example, you can close the Kafka consumer gracefully
    consumer = consumers.get(thread.ident)
    if consumer:
        consumer.close()

# Function to monitor memory usage
def monitor_memory_usage():
    while True:
        current_memory_usage = psutil.virtual_memory().percent
        if current_memory_usage < min_memory_allocation:
            # Increase the number of consumer threads (e.g., by starting more threads)
            num_threads_to_add = (min_memory_allocation - current_memory_usage) / memory_per_thread
            num_threads_to_add = int(max(num_threads_to_add, 0))  # Ensure non-negative integer
            add_consumer_threads(num_threads_to_add)
        time.sleep(memory_check_interval)

# Function to clean up the DataFrame
def cleanup_dataframe(df, max_records_per_timestamp=10):
    cleaned_df = pd.concat([df[df['timestamp'] == timestamp].tail(max_records_per_timestamp) for timestamp in df['timestamp'].unique()])
    return cleaned_df

# Function to print query result as a table with column names
def print_result(result, select_query):
    if select_query.startswith("SELECT "):
        columns = [col.strip() for col in select_query.split("SELECT ")[1].split("FROM")[0].split(",")]
        df = pd.DataFrame(result, columns=columns)
        print(df)
    else:
        print("Invalid SELECT statement format: ", select_query)

# Function to consume messages from Kafka
def consume_messages(cluster_name, consumer, select_query, in_memory_table):
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print("Error while polling message: {}".format(msg.error()))
        else:
            process_message(msg, select_query, in_memory_table)

# Function to initialize the Streamlit app
def initialize_streamlit(data_queue):
    # Streamlit UI
    st.set_page_config(
        page_title="Kafka Consumer Live Dataflow",
        page_icon=":rocket:",
        layout="wide",
    )

    display_live_stream(data_queue)

# Function to display live streaming dataflow
def display_live_stream(data_queue):
    st.title("Live Streaming Dataflow")

    # Create an initial empty DataFrame
    df = pd.DataFrame(columns=["timestamp", "data"])  # Replace with your column names

    # Streamlit UI loop
    while True:
        try:
            # Attempt to get data from the queue (blocks until data is available)
            message_data = data_queue.get()

            # Process the message_data and update the DataFrame
            # Here, we assume message_data is a dictionary with a "timestamp" and "data" field
            df = df.append(message_data, ignore_index=True)

            # Display the updated DataFrame
            st.write(df)

        except queue.Empty:
            pass

# Load configuration from cluster.json
cluster_file_path = "cluster.json"

with open(cluster_file_path, 'r') as cluster_file:
    cluster_config = json.load(cluster_file)

# Create Kafka consumer instances and Streamlit output modules for each cluster
consumers = {}
data_queues = {}

for cluster_name, cluster_params in cluster_config.items():
    # Create Kafka consumer instance for the cluster
    consumer = Consumer({
        **cluster_params,
        **config,
    })
    consumers[cluster_name] = consumer

    # Subscribe to the topic for this cluster
    topic = cluster_params.get("topic", "default_topic")
    consumer.subscribe([topic])

    # Check if the cluster has an output module specified
    if "output_module" in cluster_params and cluster_params["output_module"] == "streamlit":
        # Create a Queue to share data with the Streamlit app
        data_queues[cluster_name] = queue.Queue()

        # Start the Streamlit output module in a separate thread
        streamlit_thread = threading.Thread(target=initialize_streamlit, args=(data_queues[cluster_name],))
        streamlit_thread.daemon = True
        streamlit_thread.start()

# Create a ThreadPoolExecutor with multiple threads
num_threads = config.get("num_threads", 4)  # Adjust as needed
executor = ThreadPoolExecutor(max_workers=num_threads)

# Start the memory usage monitoring thread
memory_monitor_thread = threading.Thread(target=monitor_memory_usage)
memory_monitor_thread.daemon = True
memory_monitor_thread.start()

# Start consuming messages for each cluster
for cluster_name, consumer in consumers.items():
    thread = threading.Thread(target=consume_messages, args=(cluster_name, consumer, select_query, in_memory_table))
    thread.daemon = True
    thread.start()
    consumer_threads[thread.ident] = thread

try:
    while True:
        # Continuously process messages
        pass

except KeyboardInterrupt:
    print('Interrupted, closing consumers...')
finally:
    for consumer in consumers.values():
        consumer.close()
