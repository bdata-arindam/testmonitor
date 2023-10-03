import json
import threading
import logging
from confluent_kafka import Producer, KafkaError
import pandas as pd
from prometheus_read import tables  # Import the tables from prometheus_read.py

# Load Kafka setup configuration from kafka_setup.json
with open('kafka_setup.json', 'r') as kafka_config_file:
    kafka_setup = json.load(kafka_config_file)

# Kafka Producer configuration
producer_config = {
    'bootstrap.servers': kafka_setup['bootstrap_servers'],
    'acks': 'all'
}

# Create a Kafka Producer
producer = Producer(producer_config)

# Load log file configuration from setup.json
with open('setup.json', 'r') as setup_file:
    setup = json.load(setup_file)

log_file = setup.get("log_file", "prom_con_pro.log")

# Configure logging based on setup.json
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to produce data to Kafka topics
def produce_to_kafka(topic, data):
    try:
        data_json = data.to_json(orient='records')  # Convert the DataFrame to JSON
        producer.produce(topic, key=None, value=data_json.encode('utf-8'))
        producer.flush()  # Ensure all messages are sent
    except Exception as e:
        logging.error(f"Error producing data to Kafka topic '{topic}': {e}")

# Define the topics to produce to
producer_topic_configurations = kafka_setup['producers']

# Function to handle Kafka producer for a topic
def kafka_producer_thread(producer_topic_configuration):
    for topic in producer_topic_configuration["topics"]:
        logging.info(f"Producing data to Kafka topic: {topic}")

        # Get the table corresponding to the topic (query name)
        query_name = topic

        while True:
            try:
                if query_name in tables:
                    table_data = tables[query_name]
                    produce_to_kafka(topic, table_data)

                # Optionally, you can add a delay here if needed

            except Exception as e:
                logging.error(f"Error processing data for Kafka topic '{topic}': {e}")

# Create producer threads for each producer topic configuration
producer_threads = []
for producer_topic_configuration in producer_topic_configurations:
    thread = threading.Thread(target=kafka_producer_thread, args=(producer_topic_configuration,))
    producer_threads.append(thread)
    thread.start()

# Wait for all producer threads to complete
for thread in producer_threads:
    thread.join()
