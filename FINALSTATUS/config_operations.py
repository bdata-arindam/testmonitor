import json

def load_config_file(filename):
    """
    Load a configuration file and return its content as a dictionary.

    Args:
        filename (str): The name of the configuration file to load.

    Returns:
        dict: The content of the configuration file as a dictionary.
    """
    try:
        with open(filename, "r") as config_file:
            config_data = json.load(config_file)
        return config_data
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file '{filename}' not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing '{filename}': {str(e)}")

def save_config_file(filename, data):
    """
    Save a dictionary as a configuration file in JSON format.

    Args:
        filename (str): The name of the configuration file to save.
        data (dict): The dictionary containing configuration data to be saved.
    """
    try:
        with open(filename, "w") as config_file:
            json.dump(data, config_file, indent=4)
    except Exception as e:
        raise Exception(f"Error saving configuration to '{filename}': {str(e)}")
