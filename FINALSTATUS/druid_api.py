import requests

def fetch_druid_status(cluster_info):
    """
    Fetch Druid status from a Druid Router using HTTPS authentication.

    Args:
        cluster_info (dict): A dictionary containing cluster information.
            - "cluster_name" (str): Name of the Druid cluster.
            - "url" (str): URL of the Druid Router.
            - "username" (str): Username for authentication.
            - "password" (str): Password for authentication.

    Returns:
        dict or None: A dictionary containing the Druid status data if successful, None on error.
    """
    cluster_name = cluster_info["cluster_name"]
    url = cluster_info["url"]
    username = cluster_info["username"]
    password = cluster_info["password"]

    # Prepare the URL for the Druid status endpoint
    status_endpoint = f"{url}/status"
    
    try:
        # Create a session for making requests with authentication
        session = requests.Session()
        session.auth = (username, password)

        # Make a GET request to the Druid status endpoint
        response = session.get(status_endpoint)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            druid_status = response.json()
            return druid_status
        else:
            print(f"Failed to fetch Druid status for {cluster_name}. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Failed to connect to {cluster_name}: {str(e)}")
        return None

if __name__ == "__main__":
    # Example usage of druid_api.py
    cluster_info = {
        "cluster_name": "Cluster1",
        "url": "https://druid-cluster1.example.com",
        "username": "cluster1_user",
        "password": "cluster1_password"
    }

    druid_status = fetch_druid_status(cluster_info)
    if druid_status:
        print("Druid Status:")
        print(druid_status)
