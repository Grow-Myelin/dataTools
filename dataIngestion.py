import sqlite3
import pandas as pd
from sklearn.preprocessing import StandardScaler
from typing import List

class DataIngestor:
    """
    A class to ingest and process financial data from a SQLite database.
    """

    def __init__(self, db_path: str, table_names: list, min_date: str):
        """
        Initialize the DataIngestor with database path, table names, and minimum date.

        Args:
            db_path (str): Path to the SQLite database.
            table_names (list): List of table names to process.
            min_date (str): Minimum date for filtering data.
        """
        self.db_path = db_path
        self.table_names = table_names
        self.min_date = min_date

    def get_column_names(self, conn: sqlite3.Connection, table_name: str) -> list:
        """
        Retrieve column names for a given table in the database.

        Args:
            conn (sqlite3.Connection): A connection object to the SQLite database.
            table_name (str): Name of the table to retrieve columns from.

        Returns:
            list: A list of column names.
        """
        try:
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            return [info[1] for info in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error getting column names from {table_name}: {e}")
            return []

    def fetch_and_process_data(self) -> list:
        """
        Fetch and process data from the database for each table.

        Returns:
            list: A list of processed pandas DataFrames.
        """
        dfs = []
        try:
            with sqlite3.connect(self.db_path) as conn:
                for query, table_name in zip(
                    [
                        f"SELECT * from {table} WHERE DATE > '{self.min_date}'"
                        for table in self.table_names
                    ],
                    self.table_names,
                ):
                    data = conn.execute(query).fetchall()
                    column_names = self.get_column_names(conn, table_name)
                    df = pd.DataFrame(data, columns=column_names)
                    df["date"] = pd.to_datetime(df["date"])
                    df.set_index("date", inplace=True)
                    df.drop_duplicates(inplace=True)
                    df[column_names[-2]] = pd.to_numeric(
                        df[column_names[-2]], errors="coerce"
                    )
                    dfs.append(df[column_names[-2]])

            common_dates = dfs[0].index
            for df in dfs[1:]:
                common_dates = common_dates.intersection(df.index)
            dfs = [df.loc[df.index.intersection(common_dates)].bfill() for df in dfs]

            return dfs

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return []

    def scale_data(self, dfs: list) -> pd.DataFrame:
        """
        Scale the data using StandardScaler.

        Args:
            dfs (list): List of pandas DataFrames to scale.

        Returns:
            pd.DataFrame: A DataFrame of scaled features.
        """
        feature_df = pd.concat(dfs, axis=1)
        feature_df.columns = self.table_names
        scaler = StandardScaler()
        feature_df_scaled = pd.DataFrame(
            scaler.fit_transform(feature_df), columns=feature_df.columns
        )
        return feature_df_scaled



def main() -> None:
    """
    Main function to process and display data.
    """
    db_path = "finance_data.db"
    table_names = ["CORESTICKM159SFRBATL", "UNRATE"]
    min_date = "2000-01-01"
    dfs, feature_df_scaled = process_data(db_path, table_names, min_date)
    print(dfs, feature_df_scaled)


if __name__ == "__main__":
    main()
