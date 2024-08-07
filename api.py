from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import pandas as pd
import getpass
import logging
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import json

from flask import Flask, request, jsonify

import warnings
warnings.filterwarnings('ignore')

app = Flask(__name__)

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

    def to_sql(self, data: pd.DataFrame, table_name: str, schema: str):
        if not self.engine:
            raise ConnectionError("The connection to the database is not open.")
        data.to_sql(table_name, self.engine, if_exists='replace', index=False, schema=schema)

    def close(self):
        if self.engine:
            self.engine.dispose()

def log_data():
    logging.basicConfig(
        filename='api.log',
        filemode='a',
        format='%(message)s',
        level=logging.INFO
    )

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
    
# Rota para obter uma view do banco 
@app.route('/get_view/<table_name>', methods=['POST'])
def get_view(table_name):

    data = decrypt('credentials.enc', 'private.pem')

    postgresql_user = data.get("username")
    postgresql_password = data.get("password")
    postgresql_host = data.get("host")
    postgresql_port = data.get("port")
    postgresql_database = data.get("database")
    postgresql_schema = data.get("schema")

    print("Connecting to PostgreSQL...")
    postgres_conn = DBConnection(
        database_uri=f"postgresql+psycopg2://{postgresql_user}:{postgresql_password}@{postgresql_host}:{postgresql_port}/{postgresql_database}"
    )
    postgres_conn.connect()

    query = f"select * from {postgresql_database}.{postgresql_schema}.\"{table_name}\""
    data = postgres_conn.read_sql(query)
    postgres_conn.close()

    json_data = data.to_json(orient='records')

    log_data()
    
    return json_data
    
# Rota principal
@app.route('/', methods=['GET'])
def index():
    return "API Flask está funcionando!", 200

if __name__ == '__main__':
    app.run(debug=True)
