import json
import threading
import os
import tempfile
import logging
from confluent_kafka import Consumer, KafkaError
import csv
from tabulate import tabulate
import psutil
import re
import sys

# Global variables for in-memory cache and aggregation
message_cache = []
cache_lock = threading.Lock()
aggregation_result = 0

# Initialize logging
log_location = 'logs/'  # Default log location
logging.basicConfig(filename=os.path.join(log_location, 'consumer.log'), level=logging.ERROR)

def process_message(message, select_query, data_format):
    # Process the message based on the data format
    if data_format == 'json':
        try:
            message_dict = json.loads(message)
            # Process the JSON message
            # Replace this with your actual JSON processing logic
            # Example: Extract a specific key from the JSON
            key = message_dict.get('key', '')
            result = f"Processed JSON message: {message_dict}, Select Query: {select_query}, Key: {key}"
        except json.JSONDecodeError:
            result = "Error: Invalid JSON message"
    elif data_format == 'avro':
        # Process the Avro message
        # Replace this with your actual Avro processing logic
        result = f"Processed Avro message: {message}, Select Query: {select_query}"
    else:
        # Assuming 'text' format for all other cases
        # Process the text message
        # Replace this with your actual text processing logic
        result = f"Processed text message: {message}, Select Query: {select_query}"

    return result

def run_select_query(select_query, data_format):
    global aggregation_result
    # Execute the select query, including aggregation if specified
    # Replace this with your actual query execution logic
    if data_format == 'json':
        # Example: Extract a specific key from JSON and perform aggregation
        key = re.search(r"'key' ?: ?'([^']+)'", select_query)
        if key:
            value = key.group(1)
            aggregation_result += int(value)  # Accumulate the result
            return f"Aggregated JSON messages: {aggregation_result}"
        else:
            return "Error: JSON key not found in query"
    elif data_format == 'avro':
        # Example: Perform Avro-specific processing and aggregation
        aggregation_result += 1  # Increment count
        return f"Aggregated Avro messages: {aggregation_result}"
    else:
        # Example: Perform text-specific processing and aggregation
        aggregation_result += 1  # Increment count
        return f"Aggregated text messages: {aggregation_result}"

def consume_messages(bootstrap_servers, topic, kafka_select_file, max_memory_percentage, max_threads, output_format, output_to_module, module_name, module_location):
    # Kafka consumer configuration
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.warning('Reached end of partition')
                else:
                    logging.error('Error: {}'.format(msg.error()))
            else:
                message = msg.value().decode('utf-8')
                logging.info(f"Received message: {message}")

                # Determine the select query based on kafka_select.json
                query_identifier = get_query_identifier(message)  # Implement logic to extract the query identifier
                select_query = get_select_query(kafka_select_file, query_identifier)

                # Process the message and store the result in-memory
                result = process_message(message, select_query, output_format)

                # Cache the result and update aggregation
                cache_result(result, max_memory_percentage)

                # Pass the output to the specified module if enabled
                if output_to_module:
                    pass_output_to_module(result, module_name, module_location)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def get_query_identifier(message):
    # Implement logic to extract the query identifier from the message content
    # For example, if the message contains a specific keyword, use it as the identifier
    if "error" in message:
        return "query1"
    elif "info" in message:
        return "query2"
    else:
        return "default"

def get_select_query(kafka_select_file, query_identifier):
    try:
        with open(kafka_select_file, 'r') as json_file:
            data = json.load(json_file)
            select_queries = data.get('select_queries', {})
            return select_queries.get(query_identifier, select_queries.get('default', ''))
    except FileNotFoundError:
        return ''

def cache_result(result, max_memory_percentage):
    global message_cache

    # Calculate the maximum cache size based on memory percentage
    max_cache_size = max_memory_percentage * 0.01 * psutil.virtual_memory().total

    with cache_lock:
        message_cache.append(result)

        # Check if cache size exceeds the maximum
        while len(message_cache) > max_cache_size:
            message_cache.pop(0)  # Remove the oldest entry

def output_results(output_format, data):
    if output_format == 'csv':
        with open('output.csv', 'w', newline='') as csvfile:
            fieldnames = ["Message", "Select Query"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in data:
                writer.writerow({"Message": row, "Select Query": "your_select_query"})

    elif output_format == 'table':
        print(tabulate(data, headers=["Message", "Select Query"], tablefmt="grid"))

def pass_output_to_module(output, module_name, module_location):
    # Import and call the specified module with the output
    try:
        module = __import__(module_name)
        module.handle_output(output)
    except Exception as e:
        logging.error(f"Error executing the module: {e}")

def main():
    config_path = 'config.json'  # Path to your configuration JSON file

    # Load configuration parameters from config
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
        kafka_config = config.get('kafka_config', {})
        max_memory_percentage = config.get('max_memory_percentage', 20)  # Default to 20% if not provided
        max_threads = config.get('max_threads', 10)  # Default to 10 if not provided
        output_format = config.get('output_format', 'table')

        # New options for output to a separate module and error log location
        output_to_module = config.get('output_to_module', False)
        module_name = config.get('module_name', '')
        module_location = config.get('module_location', '')

        # Specify the log location
        log_location = config.get('log_location', 'logs/')

        # Path to the kafka_select.json file
        kafka_select_file = config.get('kafka_select_file', 'kafka_select.json')

        # Kafka topic name from config.json
        kafka_topic = config.get('kafka_topic', 'your_kafka_topic')

    # Create the log directory if it doesn't exist
    os.makedirs(log_location, exist_ok=True)

    # Initialize logging with the specified log location
    logging.basicConfig(filename=os.path.join(log_location, 'consumer.log'), level=logging.ERROR)

    # Start multiple consumer threads
    consumer_threads = []
    for _ in range(max_threads):
        consumer_thread = threading.Thread(target=consume_messages, args=(
            kafka_config.get('bootstrap.servers', ''), kafka_topic, kafka_select_file, max_memory_percentage, max_threads, output_format, output_to_module, module_name, module_location))
        consumer_thread.start()
        consumer_threads.append(consumer_thread)

    # Create a temporary file for caching
    with tempfile.TemporaryDirectory() as tmpdirname:
        cache_file_path = os.path.join(tmpdirname, 'cache.txt')

        # Continuously write cached results to a temporary file
        while True:
            with cache_lock:
                if message_cache:
                    with open(cache_file_path, 'a') as cache_file:
                        cache_file.write(message_cache.pop(0) + '\n')

            # Output results in CSV or tabular format
            if output_format == 'csv' or output_format == 'table':
                output_results(output_format, message_cache)

if __name__ == '__main__':
    main()
