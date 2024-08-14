from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import json
import os

TABLES = [
    ('montes_claros.dados.PROCUFV_GETCADASTROIMOB_TEMP', 'montes_claros.dados.PROCUFV_GETCADASTROIMOB_TEMP.DATAATUALIZACAO'),
    ('montes_claros.dados.PROCUFV_GETCADASTROMOB_TEMP' , 'montes_claros.dados.PROCUFV_GETCADASTROMOB_TEMP.DTINSCRICAOMUNICIPIO'),
    ('montes_claros.dados.PROCUFV_GETDADOSITBI_TEMP'   , 'montes_claros.dados.PROCUFV_GETDADOSITBI_TEMP.DT_FISCALIZACAO')
]

LOG_FILE_PATH = 'logs/webservice-mc_log.txt'

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

src_engine = create_engine( # SQL SERVER ENGINE
    f"mssql+pyodbc://{src['username']}:{src['password']}"
    f"@{src['host']}:{src['port']}"
    f"/{src['database']}?driver=ODBC+Driver+17+for+SQL+Server"
)

tar_engine = create_engine( # POSTGRESQL ENGINE
    f"postgresql+psycopg2://{tar['username']}:{tar['password']}"
    f"@{tar['host']}:{tar['port']}"
    f"/{tar['database']}"
)

log = get_last_log_datetime(LOG_FILE_PATH)

# Send new data from 'src' (SQL SERVER) to 'tar' (POSTGRESQL)
for table, date_column in TABLES:

    query = f"select * from {table}"

    if log:

        curr_datetime = f"convert(datetime, '{log}', 120)"

        if table == TABLES[1][0]: # if is 'PROCUFV_GETCADASTROMOB_TEMP' table
            query += f" where convert(datetime, {date_column}, 103) > {curr_datetime}" # cast nvarchar(yyyy/mm/dd) to datetime
        else:
            query += f" where {date_column} > {curr_datetime}"

    data = pd.read_sql(query, src_engine)
    data.to_sql(table.split('.')[-1], tar_engine, if_exists='append', index=False, schema=tar['schema'])
    # print(data[date_column.split('.')[-1]])

# Save into log the current datetime
with open(LOG_FILE_PATH, 'a') as f:
    f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')

src_engine.dispose()
tar_engine.dispose()
