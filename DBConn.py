from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import pandas as pd
import getpass
import pyodbc

import warnings
warnings.filterwarnings('ignore')

TABLES = [
    'PROCUFV_GETCADASTROIMOB_TEMP',#
    'PROCUFV_GETCADASTROMOB_TEMP',#
    'PROCUFV_GETDADOSITBI_TEMP'#
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


    schemapg = "dados"
    schemasql = "dbo"

    print("\nConnecting to SQL Server...")
    try:
        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={sqls_host};'
            f'DATABASE={sqls_database};'
            f'UID={sqls_user};'
            f'PWD={sqls_password}'
        )
        print("Conexão estabelecida com sucesso!")
        sqls_conn = connection.cursor()
    except pyodbc.Error as ex:
        print(f"Erro ao conectar: {ex}")

    print("Connecting to PostgreSQL...")
    postgres_conn = DBConnection(
        database_uri=f"postgresql+psycopg2://{postgresql_user}:{postgresql_password}@{postgresql_host}:{postgresql_port}/{postgresql_database}"
    )
    postgres_conn.connect()

    print("Data Base's connected...")

    for table in TABLES:
        query = f"select TOP 20 * from {sqls_database}.{schemasql}.{table}"
        #print (query)
        dataCursor = sqls_conn.execute(query)
        dataAll = dataCursor.fetchall()
        columns = [column[0] for column in dataCursor.description]
        data = pd.DataFrame.from_records(dataAll, columns=columns)
        #print("data: ", data)
        postgres_conn.send(data, table, schemapg)

    sqls_conn.close()
    postgres_conn.close()

if __name__ == "__main__":
    main()