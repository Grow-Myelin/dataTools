from .dataIngestion import DataIngestor
import sqlite3


def process_data(db_path: str, table_names: list, min_date: str) -> tuple:
    """
    Process and scale data from the database.

    Args:
        db_path (str): Path to the SQLite database.
        table_names (list): List of table names to process.
        min_date (str): Minimum date for filtering data.

    Returns:
        tuple: A tuple containing list of DataFrames and scaled DataFrame.
    """
    data_ingestor = DataIngestor(db_path, table_names, min_date)
    dfs = data_ingestor.fetch_and_process_data()
    feature_df_scaled = data_ingestor.scale_data(dfs)
    return dfs, feature_df_scaled


def drop_table(db_path: str, table_name: str) -> None:
    """
    Drop a table from the database.

    Args:
        db_path (str): Path to the SQLite database.
        table_name (str): Name of the table to be dropped.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(f"DROP TABLE {table_name}")
    except sqlite3.Error as e:
        print(f"Error dropping table {table_name}: {e}")
