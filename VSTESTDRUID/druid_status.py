import requests
import time

def fetch_druid_status(config):
    """
    Fetches the status of Druid routers from a specified URL.

    Args:
        config (dict): The configuration dictionary containing the following keys:
            - 'cluster_name': Name of the Druid cluster.
            - 'router_url': URL of the Druid router to fetch status from.
            - 'username': Username for authentication.
            - 'password': Password for authentication.
            - 'retry_count' (optional): Number of retries for fetching status (default is 5).

    Returns:
        dict: A dictionary containing the fetched status, or 'N/A' if unsuccessful.
    """
    cluster_name = config.get('cluster_name')
    router_url = config.get('router_url')
    retry_count = config.get('retry_count', 5)

    for retry in range(retry_count + 1):
        try:
            # Make an HTTP GET request to fetch Druid router status
            response = requests.get(f"{router_url}/status", auth=(config['username'], config['password']))
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the JSON response to get the status
            druid_status = response.json()

            # Create a dictionary with status information
            status_info = {
                'Cluster Name': cluster_name,
                'Router Status': druid_status.get('status', 'N/A'),
            }

            return status_info  # Return the status information
        except requests.exceptions.RequestException as e:
            # Handle network-related errors
            print(f"Network error while fetching Druid router status (Attempt {retry + 1}): {str(e)}")
        except Exception as e:
            # Handle other exceptions
            print(f"An error occurred while fetching Druid router status (Attempt {retry + 1}): {str(e)}")

        if retry < retry_count:
            # Sleep for an exponentially increasing time before retrying
            time.sleep(2 ** retry)

    # Return 'N/A' if fetching status was unsuccessful
    return {
        'Cluster Name': cluster_name,
        'Router Status': 'N/A',
    }
