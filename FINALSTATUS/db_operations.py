import sqlite3
import os
from datetime import datetime, timedelta

def create_database(db_path, tables):
    """
    Create an SQLite database and tables if they don't exist.

    Args:
        db_path (str): Path to the SQLite database file.
        tables (dict): A dictionary where keys are table names and values are SQL table creation statements.

    Returns:
        sqlite3.Connection: SQLite database connection.
    """
    if not os.path.exists(db_path):
        # Create a new database file if it doesn't exist
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create tables
        for table_name, table_schema in tables.items():
            cursor.execute(table_schema)
        
        conn.commit()
        conn.close()
    else:
        conn = sqlite3.connect(db_path)

    return conn

def insert_data_into_table(conn, table_name, data):
    """
    Insert data into an SQLite table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        table_name (str): Name of the table.
        data (list of tuples): Data to insert into the table.
    """
    cursor = conn.cursor()
    insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(['?']*len(data[0]))})"
    
    try:
        cursor.executemany(insert_sql, data)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

def truncate_data(conn, table_name, max_period_months):
    """
    Truncate data in an SQLite table to keep only the last N months' data.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        table_name (str): Name of the table.
        max_period_months (int): Maximum number of months to keep data for.
    """
    cursor = conn.cursor()

    # Calculate the date N months ago from the current date
    truncate_date = (datetime.now() - timedelta(days=max_period_months * 30)).strftime('%Y-%m-%d')

    # SQL query to delete rows older than truncate_date
    delete_sql = f"DELETE FROM {table_name} WHERE date_column < ?"

    try:
        cursor.execute(delete_sql, (truncate_date,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

def get_last_two_months_data(conn, table_name):
    """
    Retrieve the last two months' data from an SQLite table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        table_name (str): Name of the table.

    Returns:
        list of tuples: Retrieved data from the table.
    """
    cursor = conn.cursor()

    # Calculate the date two months ago from the current date
    two_months_ago = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')

    # SQL query to select rows within the last two months
    select_sql = f"SELECT * FROM {table_name} WHERE date_column >= ?"

    cursor.execute(select_sql, (two_months_ago,))
    result = cursor.fetchall()
    return result

if __name__ == "__main__":
    # Example usage of db_operations.py
    db_path = "druid_status.db"

    # Define database tables with schemas
    tables = {
        "druid_status": """
            CREATE TABLE IF NOT EXISTS druid_status (
                date_column TEXT,
                cluster_name TEXT,
                druid_version TEXT,
                coordinator_status TEXT,
                total_datasources INTEGER,
                total_segments INTEGER,
                -- Add other columns here
                PRIMARY KEY (date_column, cluster_name)
            )
        """
    }

    # Create or connect to the database
    conn = create_database(db_path, tables)

    # Insert sample data into the druid_status table
    sample_data = [
        ("2023-01-01", "Cluster1", "0.23.0", "Active", 10, 100),
        ("2023-01-01", "Cluster2", "0.24.0", "Active", 8, 80),
    ]

    insert_data_into_table(conn, "druid_status", sample_data)

    # Truncate data to keep only the last 2 months
    max_period_months = 2
    truncate_data(conn, "druid_status", max_period_months)

    # Retrieve and print the last two months' data
    last_two_months_data = get_last_two_months_data(conn, "druid_status")
    print(last_two_months_data)

    conn.close()
