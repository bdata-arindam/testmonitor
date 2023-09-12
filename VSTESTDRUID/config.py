import json

def load_config(config_file):
    """
    Loads the configuration from a JSON file.

    Args:
        config_file (str): The path to the JSON configuration file.

    Returns:
        dict: A dictionary containing the loaded configuration.
    """
    try:
        # Open and read the JSON configuration file
        with open(config_file, 'r') as file:
            config = json.load(file)
        return config
    except Exception as e:
        # Handle any errors while loading the configuration
        print(f"Error loading configuration: {str(e)}")
        return {}
