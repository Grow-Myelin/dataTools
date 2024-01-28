import sqlite3
import requests

class DataFetcher:
    def __init__(self, db_path):
        self.db_path = db_path

    def create_table(self, table_name, columns):
        conn = sqlite3.connect(self.db_path)
        column_definitions = ', '.join([f'"{col}" TEXT' for col in columns])
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                {column_definitions}
            );
        ''')
        conn.commit()
        conn.close()

    def fetch_and_insert_stock_data(self, stock_ticker, api_key, report_link, from_date, to_date):
        url = f'https://financialmodelingprep.com/api/v3/{report_link}/{stock_ticker}?limit=100&from={from_date}&to={to_date}&apikey={api_key}'
        response = requests.get(url)
        table_name = report_link.replace("-", "_")
        if response.status_code == 200:
            financial_statement = response.json()
            if not financial_statement:
                return
            first_statement = financial_statement[0]
            columns = list(first_statement.keys())
            self.create_table(table_name, columns)
            self._insert_data(table_name, columns, financial_statement, stock_ticker)
        else:
            print(f'Failed to retrieve data for {stock_ticker}. Status code: {response.status_code}')

    def fetch_and_insert_economic_data(self, api_key, report_link):
        url = f'https://api.stlouisfed.org/fred/series/observations?series_id={report_link}&api_key={api_key}&file_type=json'
        response = requests.get(url)
        table_name = report_link.replace("-", "_")
        if response.status_code == 200:
            economic_data = response.json()['observations']
            if not economic_data:
                return
            first_statement = economic_data[0]
            columns = list(first_statement.keys())
            self.create_table(table_name, columns)
            self._insert_data(table_name, columns, economic_data)
        else:
            print(f'Failed to retrieve data for {report_link}. Status code: {response.status_code}')

    def _insert_data(self, table_name, columns, data, ticker=None):
        conn = sqlite3.connect(self.db_path)
        for statement in data:
            values = [ticker] + [statement.get(col, None) for col in columns] if ticker else [statement.get(col, None) for col in columns]
            placeholders = ', '.join(['?'] * len(values))
            column_names = ', '.join(['ticker'] + ['"' + col + '"' for col in columns]) if ticker else ', '.join(['"' + col + '"' for col in columns])
            conn.execute(f'''
                INSERT INTO {table_name} (
                    {column_names}
                )
                VALUES ({placeholders});
            ''', values)
        conn.commit()
        conn.close()

def fetch_stocks_data(db_path, stocks, api_key):
    data_fetcher = DataFetcher(db_path)
    for stock in stocks:
        data_fetcher.fetch_and_insert_stock_data(stock, api_key, 'historical-price-full', '2014-01-01', '2024-01-01')

def fetch_economic_data(db_path, table_names, api_key):
    data_fetcher = DataFetcher(db_path)
    for table in table_names:
        data_fetcher.fetch_and_insert_economic_data(api_key, table)
