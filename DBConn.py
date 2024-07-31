import abc
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import psycopg2
import pyodbc
import warnings
warnings.filterwarnings('ignore')

TABLES = [
    'PROCUFV_GETCADASTROIMOB_TEMP',
    'PROCUFV_GETCADASTROMOB_TEMP',
    'PROCUFV_GETDADOSITBI_TEMP'
]

QUERIES = {
    TABLES[0] : "",
    TABLES[1] : "",
    TABLES[2] : ""
}

class DBConnection:
    def __init__(self, database_uri: str):
        self.database_uri = database_uri
        self.engine = None

    def connect(self):
        """Abre a conexão com o banco de dados usando SQLAlchemy."""
        self.engine = create_engine(self.database_uri)

    def read_data(self, query: str) -> pd.DataFrame:
        """Executa uma query SQL e retorna os resultados como DataFrame."""
        if not self.engine:
            raise ConnectionError("A conexão com o banco de dados não está aberta.")
        return pd.read_sql(query, self.engine)

    def load_data(self, data: pd.DataFrame, table_name: str):
        """Carrega os dados em uma tabela do banco de dados."""
        if not self.engine:
            raise ConnectionError("A conexão com o banco de dados não está aberta.")
        data.to_sql(table_name, self.engine, if_exists='replace', index=False, schema="dados")

    def close(self):
        """Fecha a conexão com o banco de dados."""
        if self.engine:
            self.engine.dispose()

def main():

    # SQL SERVER
    port     = 1433
    host     = ''
    database = ''
    user     = ''
    password = ''
    password_pg = ''

    # POSTGRESQL
    # port     = 5432
    # server   = ''
    # database = ''
    # username = ''
    # password = ''

    # host     = input("host    : ")
    # port     = input("port    : ")
    # database = input("database: ")
    # user     = input("user    : ")
    # password = getpass.getpass("password: ")

    print("Conectando ao SQL Server...")
    sqlserver_conn = DBConnection(
        database_uri=f"mssql+pyodbc://{user}:{password}@{host}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    sqlserver_conn.connect()

    print("Conectando ao PostgreSQL...")
    postgres_conn = DBConnection(
        database_uri=f"postgresql+psycopg2://{user}:{password_pg}@{host}:{5432}/{database}"
    )
    postgres_conn.connect()

    for count in 3:
        query = f"SELECT * FROM {database}.dados.{TABLES[count]};"
        data = sqlserver_conn.read_data(query)
        postgres_conn.load_data(data, TABLES[count])

    print(data.head(10))

    # for table, query in QUERIES.items():
        # postgres_conn.load_data(query, table)

    sqlserver_conn.close()
    postgres_conn.close()

if __name__ == "__main__":
    main()