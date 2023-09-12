import requests
import time

def fetch_memory_metrics(config):
    """
    Fetches memory metrics for historical and middle manager nodes.

    Args:
        config (dict): The configuration dictionary containing the following keys:
            - 'cluster_name': Name of the Druid cluster.
            - 'historical_metrics_url': URL for fetching historical node metrics.
            - 'middle_manager_metrics_url': URL for fetching middle manager node metrics.
            - 'retry_count' (optional): Number of retries for fetching metrics (default is 5).

    Returns:
        dict: A dictionary containing fetched memory metrics or 'N/A' if unsuccessful.
    """
    cluster_name = config.get('cluster_name')
    historical_url = config.get('historical_metrics_url')
    middle_manager_url = config.get('middle_manager_metrics_url')
    retry_count = config.get('retry_count', 5)

    for retry in range(retry_count + 1):
        try:
            # Make an HTTP GET request to fetch memory metrics for historical nodes
            historical_metrics_response = requests.get(historical_url)
            historical_metrics_response.raise_for_status()  # Raise an exception for HTTP errors
            historical_metrics = historical_metrics_response.json()

            # Make an HTTP GET request to fetch memory metrics for middle manager nodes
            middle_manager_metrics_response = requests.get(middle_manager_url)
            middle_manager_metrics_response.raise_for_status()  # Raise an exception for HTTP errors
            middle_manager_metrics = middle_manager_metrics_response.json()

            # Calculate the total swap memory for historical and middle manager nodes
            total_swap_memory = historical_metrics['swap_memory'] + middle_manager_metrics['swap_memory']

            # Create a dictionary with memory metrics
            metrics = {
                'Cluster Name': cluster_name,
                'Total Memory (Historical)': historical_metrics['total_memory'],
                'Utilized Memory (Historical)': historical_metrics['used_memory'],
                'Total Memory (Middle Manager)': middle_manager_metrics['total_memory'],
                'Utilized Memory (Middle Manager)': middle_manager_metrics['used_memory'],
                'Total Swap Memory (Historical and Middle Manager)': total_swap_memory,
            }

            return metrics  # Return the memory metrics
        except requests.exceptions.RequestException as e:
            # Handle network-related errors
            print(f"Network error while fetching memory metrics (Attempt {retry + 1}): {str(e)}")
        except Exception as e:
            # Handle other exceptions
            print(f"An error occurred while fetching memory metrics (Attempt {retry + 1}): {str(e)}")

        if retry < retry_count:
            # Sleep for an exponentially increasing time before retrying
            time.sleep(2 ** retry)

    # Return 'N/A' if fetching memory metrics was unsuccessful
    return {
        'Cluster Name': cluster_name,
        'Total Memory (Historical)': 'N/A',
        'Utilized Memory (Historical)': 'N/A',
        'Total Memory (Middle Manager)': 'N/A',
        'Utilized Memory (Middle Manager)': 'N/A',
        'Total Swap Memory (Historical and Middle Manager)': 'N/A',
    }
