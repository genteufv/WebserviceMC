from   service.utils import decrypt, date_log_event, get_last_log                
from   sqlalchemy    import create_engine
from   datetime      import datetime
import warnings
import pandas        as     pd
import pyodbc

warnings.filterwarnings("ignore")

TABLES = [
    ('PROCUFV_GETCADASTROIMOB_TEMP', 'PROCUFV_GETCADASTROIMOB_TEMP.DATAATUALIZACAO'),
    ('PROCUFV_GETCADASTROMOB_TEMP' , 'PROCUFV_GETCADASTROMOB_TEMP.DTINSCRICAOMUNICIPIO'),
    ('PROCUFV_GETDADOSITBI_TEMP'   , 'PROCUFV_GETDADOSITBI_TEMP.DT_FISCALIZACAO')
]

LOG_FILE_PATH = 'logs/webservice-mc_log.txt'
BATCH_SIZE = 10000  # Tamanho do lote para processamento

src = decrypt('credentials_SQLS.enc', 'private.pem')       # SQL SERVER CREDENTIALS (host, database, port, password etc...)
tar = decrypt('credentials_PostgreSQL.enc', 'private.pem') # POSTGRESQL CREDENTIALS (host, database, port, password etc...)

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
log = get_last_log(LOG_FILE_PATH)

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
date_log_event(
    LOG_FILE_PATH, 
    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
)

tar_engine.dispose()