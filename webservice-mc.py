from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import json
import os
import pyodbc
import warnings

warnings.filterwarnings("ignore")

TABLES = [
    ('PROCUFV_GETCADASTROIMOB_TEMP', 'PROCUFV_GETCADASTROIMOB_TEMP.DATAATUALIZACAO'),
    ('PROCUFV_GETCADASTROMOB_TEMP' , 'PROCUFV_GETCADASTROMOB_TEMP.DTINSCRICAOMUNICIPIO'),
    ('PROCUFV_GETDADOSITBI_TEMP'   , 'PROCUFV_GETDADOSITBI_TEMP.DT_FISCALIZACAO')
]

LOG_FILE_PATH = 'logs/webservice-mc_log.txt'
BATCH_SIZE = 10000  # Tamanho do lote para processamento

def get_last_log_datetime(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            lines = f.readlines()
            if lines:
                return lines[-1].strip()
    return None

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

src = decrypt('credentials_SQLS.enc', 'private.pem')       # SQL SERVER CREDENTIALS (hostname, database, port, password etc...)
tar = decrypt('credentials_PostgreSQL.enc', 'private.pem') # POSTGRESQL CREDENTIALS (hostname, database, port, password etc...)

try:
    connection = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={src['host']};"
        f"DATABASE={src['database']};"
        f"UID={src['username']};"
        f"PWD={src['password']}"
    )
    print("Conexão estabelecida com sucesso!")
    sqls_conn = connection.cursor()
except pyodbc.Error as ex:
    print(f"Erro ao conectar: {ex}")

tar_engine = create_engine(
    f"postgresql+psycopg2://{tar['username']}:{tar['password']}@{tar['host']}:{tar['port']}/{tar['database']}"
)

print("conectou sql")
log = get_last_log_datetime(LOG_FILE_PATH)

# Send new data from 'src' (SQL SERVER) to 'tar' (POSTGRESQL)
for table, date_column in TABLES:

    query = f"select * from {src['database']}.{src['schema']}.{table}"

    if log:
        curr_datetime = f"convert(datetime, '{log}', 120)"

        if table == TABLES[1][0]:  # if is 'PROCUFV_GETCADASTROMOB_TEMP' table
            query += f" where convert(datetime, {src['database']}.{src['schema']}.{date_column}, 103) > {curr_datetime}"  # cast nvarchar(yyyy/mm/dd) to datetime
        else:
            query += f" where {src['database']}.{src['schema']}.{date_column} > {curr_datetime}"

    offset = 0
    while True:
        batch_query = f"{query} ORDER BY {date_column} OFFSET {offset} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY"
        data = pd.read_sql(batch_query, connection)
        print("Lote de dados do SQL--", data)

        if data.empty:
            print("Todos os dados foram processados.")
            break
        
        data.to_sql(
            table.split('.')[-1], 
            tar_engine, 
            if_exists='replace' if log is None and offset == 0 else 'append', 
            index=False, 
            schema=tar['schema']
        )
        print(f"Lote de {len(data)} registros inserido na tabela {table}.")

        offset += BATCH_SIZE

# Save into log the current datetime
with open(LOG_FILE_PATH, 'a') as f:
    f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')

tar_engine.dispose()
