from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import json
import os

import warnings
warnings.filterwarnings('ignore')

TABLES = [
    # 'PROCUFV_GETCADASTROIMOB_TEMP',
    'PROCUFV_GETCADASTROMOB_TEMP',
    'PROCUFV_GETDADOSITBI_TEMP'
]

DATE_COL = {
    # TABLES[0] : 'DATAATUALIZACAO',
    TABLES[0] : 'DTINSCRICAOMUNICIPIO',
    TABLES[1] : 'DT_FISCALIZACAO'
}

LOG_FILE_PATH = 'webservice-mc_log.txt'

class DBConnection:
    def __init__(self, database_uri: str):
        self.database_uri = database_uri
        self.engine = None

    def connect(self):
        self.engine = create_engine(self.database_uri)

    def read_sql(self, query: str) -> pd.DataFrame:
        if not self.engine:
            raise ConnectionError("The connection to the database is not open.")
        return pd.read_sql(query, self.engine)

    def to_sql(self, data: pd.DataFrame, table_name: str, schema : str):
        if not self.engine:
            raise ConnectionError("The connection to the database is not open.")
        data.to_sql(table_name, self.engine, if_exists='replace', index=False, schema=schema)

    def close(self):
        if self.engine:
            self.engine.dispose()

def decrypt(encrypted_file_path, private_key_path):
    
    with open(private_key_path, "rb") as key_file:
        private_key = load_pem_private_key(key_file.read(), password=None)

    with open(encrypted_file_path, "rb") as enc_file:
        encrypted_data = enc_file.read()

    decrypted_data = private_key.decrypt(
        encrypted_data,
        padding.PKCS1v15()
    )
    
    try:
        data = json.loads(decrypted_data)
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
        return None
    
    return data

def last_log_datetime(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            lines = f.readlines()
            if lines:
                last_line = lines[-1].strip()
                try:
                    return datetime.fromisoformat(last_line)
                except ValueError:
                    print("Formato de data e hora inválido na última linha do log.")
                    return None
    return None

def main():

    # Get SQL Server and PostgreSQL credentials
    sqls_cred = decrypt('credentials_SQLS.enc', 'private.pem')
    pg_cred = decrypt('credentials_PostgreSQL.enc', 'private.pem')

    # Connect to the databases
    print("\nConnecting to SQL Server...")
    sqls_conn = DBConnection(
        f"mssql+pyodbc://{sqls_cred['username']}:{sqls_cred['password']}@"
        f"{sqls_cred['host']}:{sqls_cred['port']}/"
        f"{sqls_cred['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    sqls_conn.connect()

    print("Connecting to PostgreSQL...")
    pg_conn = DBConnection(
        f"postgresql+psycopg2://{pg_cred['username']}:{pg_cred['password']}@"
        f"{pg_cred['host']}:{pg_cred['port']}/"
        f"{pg_cred['database']}"
    )
    pg_conn.connect()
    
    # Send data from SQL Server to PostgreSQL
    last_datetime = last_log_datetime(LOG_FILE_PATH)

    for table in TABLES:
        
        query = f"select * from {sqls_cred['database']}.{sqls_cred['schema']}.{table}"

        if last_datetime:
            query += f" where \"{DATE_COL[table]}\" > '{last_datetime}'"

        data = sqls_conn.read_sql(query)
        pg_conn.to_sql(data, table, pg_cred['schema'])

    # Save datetime
    with open(LOG_FILE_PATH, 'a') as f:
        f.write(datetime.now().isoformat() + '\n')

    sqls_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    main()
