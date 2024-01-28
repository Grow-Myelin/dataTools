import sqlite3
import pandas as pd
from sklearn.preprocessing import StandardScaler

class DataIngestor:
    def __init__(self, db_path, table_names, min_date):
        self.db_path = db_path
        self.table_names = table_names
        self.min_date = min_date

    def get_column_names(self, conn, table_name):
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        return [info[1] for info in cursor.fetchall()]

    def fetch_and_process_data(self):
        queries = [f"SELECT * from {table_name} WHERE DATE > '{self.min_date}'" for table_name in self.table_names]
        dfs = []

        with sqlite3.connect(self.db_path) as conn:
            for query, table_name in zip(queries, self.table_names):
                data = conn.execute(query).fetchall()
                column_names = self.get_column_names(conn, table_name)
                df = pd.DataFrame(data, columns=column_names)
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
                df = df[~df.index.duplicated(keep='first')]
                df[column_names[-1]] = pd.to_numeric(df[column_names[-1]], errors='coerce')
                dfs.append(df[column_names[-1]])

        # Ensure all DataFrames have the same date index
        common_dates = dfs[0].index
        for df in dfs[1:]:
            common_dates = common_dates.intersection(df.index)
        dfs = [df.loc[df.index.intersection(common_dates)].bfill() for df in dfs]

        return dfs

    def scale_data(self, dfs):
        feature_df = pd.concat(dfs, axis=1)
        feature_df.columns = self.table_names
        scaler = StandardScaler()
        feature_df_scaled = scaler.fit_transform(feature_df)
        feature_df_scaled = pd.DataFrame(feature_df_scaled, columns=feature_df.columns)
        return feature_df_scaled

def process_data(db_path,table_names,min_date):
    data_ingestor = DataIngestor(db_path, table_names, min_date)
    dfs = data_ingestor.fetch_and_process_data()
    feature_df_scaled = data_ingestor.scale_data(dfs)
    return dfs,feature_df_scaled
def main():
    db_path = 'finance_data.db'
    table_names = ['CORESTICKM159SFRBATL', 'UNRATE']
    #, 'VIXCLS', 'SP500']
    min_date = '2000-01-01'
    print(process_data(db_path,table_names,min_date))

if __name__ == "__main__":
    main()
