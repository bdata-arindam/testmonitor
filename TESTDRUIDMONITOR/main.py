import json
import schedule
import time
from config import load_config
from send_status_email import send_email
from druid_status import fetch_druid_status
from memory_metrics import fetch_memory_metrics
from db import initialize_database, insert_metrics_to_db

def main():
    # Load the configuration from 'config.json'
    config = load_config('config.json')

    # Initialize the database module if enabled in the config
    if config.get('enable_database', False):
        db = initialize_database(config)
    else:
        db = None

    # Define a job function to execute the tasks
    def execute_tasks():
        # Fetch Druid status
        druid_status = fetch_druid_status(config)

        # Fetch Memory Metrics
        memory_metrics = fetch_memory_metrics(config)

        # Insert metrics into the database if enabled
        if db:
            insert_metrics_to_db(db, metrics)

        # Send status email
        send_email(config, druid_status, memory_metrics)

    # Schedule the job to run at the specified interval
    execution_interval = config.get('execution_interval', 30)  # Default to 30 minutes
    schedule.every(execution_interval).minutes.do(execute_tasks)

    # Run the scheduled job continuously
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
