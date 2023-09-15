import json
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
import numpy as np
import json
import psutil
import multiprocessing
import logging
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Setup logging
logging.basicConfig(filename='anomaly_detection.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to load and preprocess data
def load_and_preprocess_data(data_filename, feature_engineering_filename, timestamp_column, volume_column):
    try:
        # Load historical data from the CSV file
        historical_df = pd.read_csv(data_filename)

        # Select the timestamp and volume columns dynamically from config.json
        historical_df = historical_df[[timestamp_column, volume_column]]

        # Assuming your CSV file has columns: timestamp_header and volume_header
        # You may need to preprocess the data if it's not in this format

        # Rename the columns to 'timestamp' and 'volume'
        historical_df = historical_df.rename(columns={timestamp_column: 'timestamp', volume_column: 'volume'})

        # Create a time series using the 'timestamp' column as the index
        historical_df['timestamp'] = pd.to_datetime(historical_df['timestamp'])
        historical_df.set_index('timestamp', inplace=True)

        # If the feature engineering file is empty, use only timestamp
        if feature_engineering_filename:
            # Load feature engineering data from another CSV file
            feature_engineering_df = pd.read_csv(feature_engineering_filename)

            # Assuming your feature engineering data has columns: 'timestamp', 'is_holiday', 'is_weekend', 'is_national_holiday'

            # Merge the historical volume data with feature engineering data based on the 'timestamp'
            historical_df = historical_df.merge(feature_engineering_df, on='timestamp', how='left')
        else:
            historical_df['is_holiday'] = 0
            historical_df['is_weekend'] = historical_df.index.dayofweek >= 5
            historical_df['is_national_holiday'] = 0

        return historical_df

# Function to send email notification
def send_email(to_email, from_email, subject, message, smtp_server):
    try:
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject

        body = f"{message}\n\nTraining Start Time: {start_time}\nTraining End Time: {end_time}\nBest Model: {best_model}\n\nModel Details:\n"
        for model_name, accuracy in results.items():
            body += f"{model_name} - Accuracy: {accuracy:.2%}\n"

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(smtp_server)
        server.starttls()
        server.login(from_email, "")  # Add your email password here or use environment variables
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
    except Exception as e:
        logger.error(f"Error sending email notification: {str(e)}")

# Rest of the code remains the same
# ...

if __name__ == "__main__":
    try:
        # Load configuration from config.json
        with open('config.json', 'r') as config_file:
            config = json.load(config_file)

        data_filename = config.get('data_filename', 'historical_volume_data.csv')
        feature_engineering_filename = config.get('feature_engineering_filename', None)  # Empty if not provided
        timestamp_column = config.get('timestamp_column', 'timestamp')  # Default column name
        volume_column = config.get('volume_column', 'volume')  # Default column name
        max_memory_percentage = config.get('max_memory_percentage', 50)
        max_thread_count = config.get('max_thread_count', multiprocessing.cpu_count())
        best_model_json_filename = config.get('best_model_json_filename', 'best_anomaly_model.json')
        email_notification = config.get('email_notification', False)
        email_to = config.get('email_to', 'recipient@example.com')
        email_from = config.get('email_from', 'sender@example.com')
        email_subject = config.get('email_subject', 'Anomaly Detection Training Completed')
        smtp_server = config.get('smtp_server', 'smtp.example.com')

        # Define anomaly detection models
        models = {
            'Isolation Forest': IsolationForest(contamination=0.01),
            'One-Class SVM': OneClassSVM(nu=0.01)
        }

        # Load and preprocess data
        data = load_and_preprocess_data(data_filename, feature_engineering_filename, timestamp_column, volume_column)

        # Record the start time
        start_time = pd.Timestamp.now()

        # Train and evaluate models
        results = train_and_evaluate_models(data, models, max_memory_percentage, max_thread_count)

        # Record the end time
        end_time = pd.Timestamp.now()

        # Select the best model based on accuracy
        best_model = max(results, key=results.get)
        best_accuracy = results[best_model]

        logger.info(f"The best model is '{best_model}' with an accuracy of {best_accuracy:.2%}")

        # Store the best model and threshold in the specified JSON file
        threshold = np.percentile(data[f'{best_model}_anomaly_score'], 5)  # Adjust the percentile as needed
        best_model_data = {
            'model_type': best_model,
            'model_params': models[best_model].get_params(),
            'threshold': float(threshold)
        }

        with open(best_model_json_filename, 'w') as json_file:
            json.dump(best_model_data, json_file)

        if email_notification:
            message = "TRAINING COMPLETED FOR ANOMALY TRAINING\n\n" \
                      "Training completed successfully. Here are the details:\n" \
                      f"Best Model: {best_model}\n" \
                      f"Training Start Time: {start_time}\n" \
                      f"Training End Time: {end_time}\n"
            send_email(email_to, email_from, email_subject, message, smtp_server)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
