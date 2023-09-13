import sqlite3
import mysql.connector

def initialize_database(config):
    """
    Initializes and returns a database connection based on the configuration.

    Args:
        config (dict): The configuration dictionary containing the following keys:
            - 'enable_database': Boolean to indicate if database storage is enabled.
            - 'database_type': Type of the database ('sqlite' or 'mysql').
            - 'database_name': Name of the database.
            - 'database_host' (optional): Hostname for the MySQL database.
            - 'database_username' (optional): Username for database authentication.
            - 'database_password' (optional): Password for database authentication.

    Returns:
        object: A database connection object or None if database storage is not enabled.
    """
    enable_database = config.get('enable_database', False)
    if not enable_database:
        return None  # Database storage is not enabled

    database_type = config.get('database_type', 'sqlite')
    database_name = config.get('database_name', 'metrics_db')

    if database_type == 'sqlite':
        # Initialize SQLite connection
        conn = sqlite3.connect(database_name)
    elif database_type == 'mysql':
        # Initialize MySQL connection
        conn = mysql.connector.connect(
            host=config.get('database_host', 'localhost'),
            user=config.get('database_username', 'root'),
            password=config.get('database_password', ''),
            database=database_name
        )
    else:
        raise ValueError("Unsupported database type")

    return conn

def insert_metrics_to_db(conn, metrics):
    """
    Inserts metrics data into the database.

    Args:
        conn (object): The database connection object.
        metrics (dict): A dictionary containing metrics data to be inserted into the database.
    """
    if not conn:
        return  # Database storage is not enabled

    cursor = conn.cursor()

    # Define SQL statements for inserting metrics into tables
    # Example (SQLite): "INSERT INTO metrics_table (cluster_name, memory_usage) VALUES (?, ?)"
    # Example (MySQL): "INSERT INTO metrics_table (cluster_name, memory_usage) VALUES (%s, %s)"

    try:
        # Execute SQL statements based on the metrics data
        # Example: cursor.execute(sql_statement, (metrics['Cluster Name'], metrics['Memory Usage']))
        # Commit the changes to the database
        cursor.close()
        conn.commit()
    except Exception as e:
        # Handle database insertion errors
        print(f"Error inserting metrics into the database: {str(e)}")
        conn.rollback()  # Rollback changes in case of an error
