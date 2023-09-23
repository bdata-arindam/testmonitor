import streamlit as st
import json
import sqlite3
import pandas as pd
from pydruid.db import connect
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.covariance import EllipticEnvelope
from sklearn.cluster import DBSCAN
from sklearn.svm import OneClassSVM
from collections import deque
import os
import cachetools

# Constants
MAX_MEMORY_ROWS = 1000
DATA_MOUNT_DIR = 'data_mounts'

# Function to read the mount configuration from a JSON file
def read_mount_configuration(config_path):
    with open(config_path, 'r') as config_file:
        config_data = json.load(config_file)
        single_mount = config_data.get("mounts", {}).get("single_mount", True)
        mount_locations = config_data.get("mounts", {}).get("mount_locations", [])
    return single_mount, mount_locations

# Function to create a cache for each mount location
def create_mount_caches(mount_locations):
    caches = {}
    for location in mount_locations:
        caches[location] = cachetools.LRUCache(maxsize=1000)  # Adjust the cache size as needed
    return caches

# Function to retain and store data in the SQLite metadata database and filesystem
def retain_and_store_data(result_df, query_id, selected_model, memory_buffer, single_mount, mount_locations, mount_caches):
    conn = sqlite3.connect('metadata.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS retained_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER,
            model TEXT,
            data_index_id INTEGER
        )
    ''')
    conn.commit()

    cursor.execute('INSERT INTO retained_data (query_id, model) VALUES (?, ?)', (query_id, selected_model))
    data_index_id = cursor.lastrowid
    conn.commit()

    if single_mount:
        mount_dirs = [DATA_MOUNT_DIR]
    else:
        mount_dirs = [os.path.join(DATA_MOUNT_DIR, location) for location in mount_locations]

    for mount_dir in mount_dirs:
        os.makedirs(mount_dir, exist_ok=True)
        data_file_path = os.path.join(mount_dir, f'data_{data_index_id}.parquet')
        result_df.to_parquet(data_file_path, index=False)
        cache_location = os.path.basename(mount_dir)
        mount_caches[cache_location][data_index_id] = result_df

    cursor.execute('UPDATE retained_data SET data_index_id=? WHERE id=?', (data_index_id, data_index_id))
    conn.commit()
    conn.close()
    memory_buffer.append((query_id, selected_model, data_index_id))
    while len(memory_buffer) > MAX_MEMORY_ROWS:
        _, _, old_data_index_id = memory_buffer.popleft()
        recover_data(old_data_index_id, single_mount, mount_locations, mount_caches)

# Function to recover data in case of mount failure
def recover_data(data_index_id, single_mount, mount_locations, mount_caches):
    for location in mount_locations:
        if data_index_id not in mount_caches[location]:
            st.warning(f"Data index ID {data_index_id} not found in cache for location {location}. Performing data recovery.")
            # Implement recovery logic here (e.g., re-download data, recreate mount)

# Function to load and process selected query with anomaly detection model
def load_and_process_selected_query(query_id, selected_model, model_params):
    # Load and preprocess data (Implement Druid query logic here)
    query_data = load_data_from_druid(query_id)
    # Perform data preprocessing and feature engineering here

    # Initialize selected anomaly detection model with parameters
    if selected_model == "Isolation Forest":
        model = IsolationForest(**model_params)
    elif selected_model == "Local Outlier Factor":
        model = LocalOutlierFactor(**model_params)
    elif selected_model == "Elliptic Envelope":
        model = EllipticEnvelope(**model_params)
    elif selected_model == "DBSCAN":
        model = DBSCAN(**model_params)
    elif selected_model == "SVM":
        model = OneClassSVM(**model_params)
    else:
        st.error("Invalid model selection")
        return None

    # Fit the model
    model.fit(query_data)

    # Predict anomalies and add anomaly score to the data
    query_data['anomaly_score'] = model.decision_function(query_data)
    return query_data

# Streamlit App
st.title('Anomaly Detection App')

# Read the mount configuration from the JSON file
config_path = 'config.json'  # Adjust the path to your configuration file
single_mount, mount_locations = read_mount_configuration(config_path)

# Create caches for each mount location
mount_caches = create_mount_caches(mount_locations)

# Create a memory buffer to store recent data
memory_buffer = deque(maxlen=MAX_MEMORY_ROWS)

# Create the directory structure for mounts
os.makedirs(DATA_MOUNT_DIR, exist_ok=True)
if not single_mount:
    for location in mount_locations:
        os.makedirs(os.path.join(DATA_MOUNT_DIR, location), exist_ok=True)

# Streamlit UI
selected_tab = st.sidebar.selectbox("Select a Tab:", ["Druid Connection", "Anomaly Detection"])

if selected_tab == "Druid Connection":
    # Implement Druid connection tab (Connection details, query setup, etc.)
    pass

elif selected_tab == "Anomaly Detection":
    st.title("Anomaly Detection")

    # Load the stored Druid queries from the database
    conn = sqlite3.connect('myapp.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id, query FROM druid_queries")
    query_list = cursor.fetchall()
    conn.close()

    if query_list:
        # Create a dropdown to select a Druid query for processing
        selected_query_id = st.selectbox("Select a Druid Query:", [f"{query_id}: {query}" for query_id, query in query_list])

        # Create a radio button to select the anomaly detection model
        selected_model = st.radio("Select Anomaly Detection Model:", ["Isolation Forest", "Local Outlier Factor", "Elliptic Envelope", "DBSCAN", "SVM"])

        # Read and display model parameters from config.json
        with open('config.json', 'r') as config_file:
            config_data = json.load(config_file)
            model_params = config_data["models"].get(selected_model, {})

        # Edit model parameters
        st.subheader(f"Edit Parameters for {selected_model}")
        for param, value in model_params.items():
            if st.checkbox(f"Edit {param}"):
                if isinstance(value, bool):
                    model_params[param] = st.checkbox(f"{param}:", value)
                else:
                    model_params[param] = st.number_input(f"{param}:", value=value)

        if st.button("Process Query"):
            # Extract the query ID from the selected value
            selected_query_id = int(selected_query_id.split(":")[0])

            # Load and process the selected query with the chosen anomaly detection model
            result_df = load_and_process_selected_query(selected_query_id, selected_model, model_params)

            # Display the processed data and update it when parameters change
            if result_df is not None:
                st.subheader("Processed Data with Anomaly Scores")
                st.dataframe(result_df)  # Display the results in a table

                # Retain and store the processed data in the metadata database and filesystem
                retain_and_store_data(result_df, selected_query_id, selected_model, memory_buffer, single_mount, mount_locations, mount_caches)
    else:
        st.warning("No Druid queries found in the database. Please execute a query in the 'Druid Connection' tab.")

# Run the Streamlit app
if __name__ == '__main__':
    st.run()
