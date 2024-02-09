import sqlite3
import requests
from datetime import datetime
from typing import List, Optional

class DataFetcher:
    """
    A class to fetch and insert financial and economic data into a SQLite database.

    This class is designed to handle two types of data: stock data and economic data.
    It provides methods to create database tables, fetch data from APIs, and insert
    this data into the appropriate tables.

    Attributes:
        db_path (str): The file path for the SQLite database.
    """

    def __init__(self, db_path: str) -> None:
        """
        Initialize the DataFetcher class with a database path.

        Args:
            db_path (str): The file path for the SQLite database.

        Returns:
            None
        """
        self.db_path = db_path

    def create_table(self, table_name: str, columns: List[str], is_stock: bool) -> None:
        """
        Create a table in the SQLite database with specified columns.

        Args:
            table_name (str): The name of the table to create.
            columns (List[str]): A list of column names for the table.
            is_stock (bool): Indicator of whether the data is stock data (True) or economic data (False).

        Returns:
            None
        """
        try:
            conn = sqlite3.connect(self.db_path)
            column_definitions = ", ".join([f'"{col}" TEXT' for col in columns]) + ', "record_load_timestamp" TEXT'
            table_def = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            table_def += "ticker TEXT, " if is_stock else ""
            table_def += column_definitions + ");"
            conn.execute(table_def)
            conn.commit()
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
        finally:
            if conn:
                conn.close()

    def fetch_and_insert_stock_data(
        self,
        stock_ticker: str,
        api_key: str,
        report_link: str,
        from_date: str,
        to_date: str,
    ) -> None:
        """
        Fetch stock data from an API and insert it into the database.

        Args:
            stock_ticker (str): The stock ticker symbol.
            api_key (str): The API key for authentication.
            report_link (str): The endpoint link for the API request.
            from_date (str): Start date for the data.
            to_date (str): End date for the data.

        Returns:
            None
        """
        try:
            url = f"https://financialmodelingprep.com/api/v3/{report_link}/{stock_ticker}?from={from_date}&to={to_date}&apikey={api_key}"
            response = requests.get(url)
            response.raise_for_status()

            financial_statement = response.json()
            if not financial_statement:
                return

            first_statement = financial_statement[0]
            columns = list(first_statement.keys())
            self.create_table(report_link.replace("-", "_"), columns, True)
            self._insert_data(report_link.replace("-", "_"), columns, financial_statement, stock_ticker)
        except Exception as e:
            print(f"Error fetching stock data: {e}")

    def fetch_and_insert_economic_data(self, api_key: str, report_link: str) -> None:
        """
        Fetch economic data from an API and insert it into the database.

        Args:
            api_key (str): The API key for authentication.
            report_link (str): The endpoint link for the API request.

        Returns:
            None
        """
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={report_link}&api_key={api_key}&file_type=json"
            response = requests.get(url)
            if response.status_code != 200:
                raise Exception(
                    f"API request failed with status code: {response.status_code}"
                )

            economic_data = response.json()["observations"]
            if not economic_data:
                return
            first_statement = economic_data[0]
            columns = list(first_statement.keys())
            table_name = report_link.replace("-", "_")
            self.create_table(table_name, columns, False)
            conn = sqlite3.connect(self.db_path)
            current_timestamp = datetime.now().isoformat()
            for statement in economic_data:
                values = [statement.get(col, None) for col in columns] + [
                    current_timestamp
                ]
                placeholders = ", ".join(["?"] * len(values))
                column_names = (
                    ", ".join(['"' + col + '"' for col in columns])
                    + ', "record_load_timestamp"'
                )
                existing_row = conn.execute(f'SELECT 1 FROM {table_name} WHERE date = ?',(statement['date']))
                if not existing_row:
                    conn.execute(
                    f"""
                        INSERT INTO {table_name} (
                            {column_names}
                        )
                        VALUES ({placeholders});
                    """,
                        values,
                    )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error fetching economic data: {e}")

    def _insert_data(
        self,
        table_name: str,
        columns: List[str],
        data: List[dict],
        ticker: Optional[str] = None,
    ) -> None:
        """
        Insert data into a specified table in the database.

        Args:
            table_name (str): The name of the table to insert data into.
            columns (List[str]): The column names for data insertion.
            data (List[Dict]): The data to be inserted.
            ticker (Optional[str]): The stock ticker symbol, if applicable.

        Returns:
            None
        """
        try:
            conn = sqlite3.connect(self.db_path)
            current_timestamp = datetime.now().isoformat()
            for statement in data:
                values = ([ticker] if ticker else []) + [statement.get(col, '') for col in columns] + [current_timestamp]
                placeholders = ", ".join(["?"] * len(values))
                column_names = ("ticker, " if ticker else "") + ", ".join([f'"{col}"' for col in columns]) + ', "record_load_timestamp"'
                conn.execute(f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders});", values)
            conn.commit()
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
        finally:
            if conn:
                conn.close()



def fetch_stocks_data(
    db_path: str,
    stocks: List[str],
    api_key: str,
    report_link: str,
    from_date: str,
    to_date: str
) -> None:
    """
    Fetch and insert stock data for multiple stocks into the database.

    Args:
        db_path (str): The file path for the SQLite database.
        stocks (List[str]): A list of stock ticker symbols.
        api_key (str): The API key for authentication.
        report_link (str): The endpoint link for the API request.
        from_date (str): The start date for fetching stock data.
        to_date (str): The end date for fetching stock data.
    """
    data_fetcher = DataFetcher(db_path)
    for stock in stocks:
        data_fetcher.fetch_and_insert_stock_data(
            stock, api_key, report_link, from_date, to_date
        )

def fetch_economic_data(db_path: str, table_names: List[str], api_key: str) -> None:
    """
    Fetch and insert economic data for multiple series into a SQLite database.

    This function iterates over a list of economic data series identifiers, fetching 
    and inserting their corresponding data into a database using a DataFetcher instance.

    Args:
        db_path (str): The file path for the SQLite database.
        table_names (List[str]): A list of economic data series identifiers.
        api_key (str): The API key for authentication.
    """
    try:
        data_fetcher = DataFetcher(db_path)
        for table in table_names:
            data_fetcher.fetch_and_insert_economic_data(api_key, table)
    except Exception as e:
        print(f"An error occurred while fetching economic data: {e}")