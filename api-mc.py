from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import pandas as pd
import getpass
import logging
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import json
from datetime import datetime

from flask import Flask, request, jsonify

import warnings
warnings.filterwarnings('ignore')

app = Flask(__name__)

TABLES = [
    'wbs_amostras_itbi_p',
    'wbs_amostras_oferta_p',
    'wbs_cad_imobiliarios_p',
    'wbs_cad_logradouro_l',
    'wbs_cad_mobiliarios_p',
    'wbs_expansao_urbana_a',
    'wbs_faces_quadra_a',
    'wbs_perimetro_urbano_a',
    'wbs_plano_diretor_a',
    'wbs_subzonas_fiscais_a',
    'wbs_zonas_fiscais_a'
]

LOG_FILE_PATH = 'logs/api-mc_log.txt'
column_date_modify = 'data_modificacao'

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

def write_log_api(table_name):
    with open(LOG_FILE_PATH, 'a') as file:

        data_atual = datetime.now()
        data_str = data_atual.strftime('%d/%m/%Y %H:%M')

        string_log = data_str + " " + table_name

        file.write(string_log + '\n')


def last_log_datetime(table_name):
    ultima_data_hora = None
    with open(LOG_FILE_PATH, 'r') as file:
        for linha in file:
            partes = linha.strip().split(' ', 3)
            data_hora = partes[0] + " " + partes[1]
            tabela_acessada = partes[2]

            if tabela_acessada == table_name:
                ultima_data_hora = data_hora
    return ultima_data_hora
            
    
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


def connection_sql(data):

    print("Connecting to PostgreSQL...")
    postgres_conn = DBConnection(
        database_uri=f"postgresql+psycopg2://{data.get("username")}:{data.get("password")}@{data.get("host")}:{data.get("port")}/{data.get("database")}"
    )
    postgres_conn.connect()

    return postgres_conn

    
@app.route('/get_recent_views/<table_name>', methods=['GET'])
def get_recent_views(table_name):

    data_file = decrypt('credentials.enc', 'private.pem')

    postgres_conn = connection_sql(data_file)

    recent_date = last_log_datetime(table_name)
    print(recent_date)

    if recent_date != None:
        date_datetime = datetime.strptime(recent_date, "%d/%m/%Y %H:%M")
        query = f"select * from {data_file.get("database")}.{data_file.get("schema")}.\"{table_name}\" where \"{column_date_modify}\" >= \'{date_datetime}\'"
    else:
        query = f"select * from {data_file.get("database")}.{data_file.get("schema")}.\"{table_name}\""

    data = postgres_conn.read_sql(query)
    postgres_conn.close()

    json_data = data.to_json(orient='records', date_format="iso")

    write_log_api(table_name)
    
    return json_data


@app.route('/get_view/<table_name>/data/<date_str>', methods=['GET'])
def get_date_view(table_name,date_str):

    data_file = decrypt('credentials.enc', 'private.pem')

    postgres_conn = connection_sql(data_file)

    date_datetime = datetime.strptime(date_str, "%d%m%Y")
    query = f"select * from {data_file.get("database")}.{data_file.get("schema")}.\"{table_name}\" where \"{column_date_modify}\" = \'{date_str}\'"

    data = postgres_conn.read_sql(query)
    postgres_conn.close()

    json_data = data.to_json(orient='records', date_format="iso")

    write_log_api(table_name)

    return json_data


@app.route('/get_view/<table_name>/periodo/<period>', methods=['GET'])
def get_period_view(table_name,period):

    data_file = decrypt('credentials.enc', 'private.pem')

    postgres_conn = connection_sql(data_file)

    data_str_start= period[:8]
    data_str_end = period[8:]

    start_date = datetime.strptime(data_str_start, "%d%m%Y")
    end_date = datetime.strptime(data_str_end, "%d%m%Y")
    query = f"select * from {data_file.get("database")}.{data_file.get("schema")}.\"{table_name}\" where \"{column_date_modify}\" between \'{data_str_start}\' and \'{data_str_end}\'"

    data = postgres_conn.read_sql(query)
    postgres_conn.close()

    json_data = data.to_json(orient='records', date_format="iso")

    write_log_api(table_name)

    return json_data


# Rota principal
@app.route('/', methods=['GET'])
def index():
    return "API Flask está funcionando!", 200

if __name__ == '__main__':
    app.run(debug=True)
