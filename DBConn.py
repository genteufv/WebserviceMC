from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import pandas as pd
import getpass

import warnings
warnings.filterwarnings('ignore')

TABLES = [
    'PROCUFV_GETCADASTROIMOB_TEMP',
    'PROCUFV_GETCADASTROMOB_TEMP',
    'PROCUFV_GETDADOSITBI_TEMP'
]

class DBConnection:
    def __init__(self, database_uri: str):
        self.database_uri = database_uri
        self.engine = None

    def connect(self):
        self.engine = create_engine(self.database_uri)

    def execute(self, query: str) -> pd.DataFrame:
        if not self.engine:
            raise ConnectionError("The connection to the database is not open.")
        return pd.read_sql(query, self.engine)

    def send(self, data: pd.DataFrame, table_name: str, schema : str):
        if not self.engine:
            raise ConnectionError("The connection to the database is not open.")
        data.to_sql(table_name, self.engine, if_exists='replace', index=False, schema=schema)

    def close(self):
        if self.engine:
            self.engine.dispose()

def main():

    print("SQL SERVER")
    sqls_host     = input("host    : ")
    sqls_port     = input("port    : ")
    sqls_database = input("database: ")
    sqls_user     = input("user    : ")
    sqls_password = getpass.getpass("password: ")

    print("\nPOSTGRESQL SERVER")
    postgresql_host     = input("host    : ")
    postgresql_port     = input("port    : ")
    postgresql_database = input("database: ")
    postgresql_user     = input("user    : ")
    postgresql_password = getpass.getpass("password: ")

    schema = "dados"

    print("\nConnecting to SQL Server...")
    sqls_conn = DBConnection(
        database_uri=f"mssql+pyodbc://{sqls_user}:{sqls_password}@{sqls_host}:{sqls_port}/{sqls_database}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    sqls_conn.connect()

    print("Connecting to PostgreSQL...")
    postgres_conn = DBConnection(
        database_uri=f"postgresql+psycopg2://{postgresql_user}:{postgresql_password}@{postgresql_host}:{postgresql_port}/{postgresql_database}"
    )
    postgres_conn.connect()

    for table in TABLES:
        query = f"select * from {sqls_database}.{schema}.{table}"
        data  = sqls_conn.execute(query)

        postgres_conn.send(data, table, schema)

    sqls_conn.close()
    postgres_conn.close()

if __name__ == "__main__":
    main()
