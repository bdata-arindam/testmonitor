import requests
import json
from tabulate import tabulate
from datetime import datetime
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Function to fetch metrics from a Druid cluster
def fetch_metrics(cluster_urls, username, password):
    auth = (username, password)
    metrics = {}
    
    for cluster_url in cluster_urls:
        try:
            response = requests.get(f"{cluster_url}/status", auth=auth)
            if response.status_code == 200:
                metrics = response.json()
                # If metrics are successfully fetched from one URL, break the loop
                break
        except Exception as e:
            logging.error(f"Error fetching metrics via {cluster_url}: {str(e)}")
    
    return metrics

# Function to set up logging
def setup_logging(log_file):
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to send an email
def send_email(subject, body, sender_email, receiver_email, smtp_server):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    with smtplib.SMTP(smtp_server) as server:
        server.sendmail(sender_email, receiver_email, msg.as_string())

# Load configuration from config.json
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Set up logging
setup_logging(config['log_file'])

# Initialize a dictionary to store metrics from each cluster
cluster_metrics_data = {}

while True:
    try:
        druid_clusters = config['druid_clusters']
        
        for cluster_info in druid_clusters:
            cluster_urls = cluster_info['urls']
            username = cluster_info['username']
            password = cluster_info['password']
            cluster_name = cluster_info['name']
            
            cluster_metrics = fetch_metrics(cluster_urls, username, password)
            
            # Store metrics in the cluster_metrics_data dictionary with cluster name as the key
            cluster_metrics_data[cluster_name] = cluster_metrics
        
        date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subject = f'Druid Metrics Report - {date_time}'
        
        # Create a single email body with the specified metrics for each cluster
        email_body = ''
        for cluster_name, cluster_metrics in cluster_metrics_data.items():
            email_body += f'Cluster: {cluster_name}\n'
            email_body += f'Druid Version: {cluster_metrics["version"]}\n'
            email_body += f'Coordinator Leader Server: {cluster_metrics["coordinator"]["leader"]}\n'
            email_body += f'Number of Coordinators Running: {len(cluster_metrics["coordinator"]["coordinators"])}\n'
            email_body += f'Successful Tasks: {cluster_metrics["indexer"]["successfulTasks"]}\n'
            email_body += f'Failed Tasks: {cluster_metrics["indexer"]["failedTasks"]}\n'
            email_body += f'Pending Tasks: {cluster_metrics["indexer"]["pendingTasks"]}\n'
            email_body += f'Available Segments: {cluster_metrics["segments"]["availableSegments"]}\n'
            email_body += f'Not Available Segments: {cluster_metrics["segments"]["failedSegments"]}\n'
            email_body += f'Total Historical Usage (%): {cluster_metrics["segments"]["loadQueue"]["loadQueueSize"]}%\n'
            email_body += f'Total Middle Managers: {len(cluster_metrics["overlord"]["middleManagers"])}\n'
            email_body += f'Currently Running Peons: {len(cluster_metrics["overlord"]["peons"])}\n'
            
            # Add Last 10 Min Count for Datasource
            email_body += 'Last 10 Min Count for Datasource:\n'
            for datasource in config['druid_clusters'][0]['datasources']:
                query = datasource['last_10_min_count_query']
                response = requests.post(f"{cluster_urls[0]}/druid/v2/sql", auth=(username, password), json={"query": query})
                count = response.json()[0]['count']
                email_body += f"- {datasource['name']}: {count}\n"
            
            email_body += '\n'
        
        # Send a single email with all cluster metrics
        send_email(
            subject=subject,
            body=email_body,
            sender_email=config['email']['sender_email'],
            receiver_email=config['email']['receiver_email'],
            smtp_server=config['email']['smtp_server']
        )
        
        logging.info(f"Metrics report sent at {date_time}")
    
    except Exception as e:
        logging.error(f"Error in main loop: {str(e)}")
        time.sleep(config['recovery_interval_seconds'])  # Sleep before retrying

    time.sleep(config['execution_interval_seconds'])