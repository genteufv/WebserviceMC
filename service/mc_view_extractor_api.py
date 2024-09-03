from   utils      import decrypt, date_log_event, get_last_log
from   sqlalchemy import create_engine
from   datetime   import datetime
from   flask      import Flask, request, jsonify
import pandas     as     pd
import requests

app = Flask(__name__)

VIEWS = [
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

LOG_FILE_PATH       = 'logs/api-mc_log.txt'
COLUMN_DATE_MODIFY  = 'data_modificacao'
ENCRYPTED_FILE_NAME = 'credentials_PostgreSQL.enc'
PRIVATE_KEY_NAME    = 'private.pem'
EXTERNAL_LOGIN_URL  = 'http://200.235.135.49:413/api/login'
EXTERNAL_AUTH_URL   = 'http://200.235.135.49:413/api/authenticate'

def get_last_log_datetime(table_name):
    """
    Retrieve the datetime of the last log entry for a specific table from the log file.
    Args:
        table_name (str): The name of the table to check in the log.
    Returns:
        datetime: The datetime of the last log entry if found, otherwise None.
    """
    log = get_last_log(LOG_FILE_PATH)

    if log == None:
        return None
    
    splited_log = log.split()

    if len(splited_log) != 3:
        return None
    
    last_datetime = splited_log[0] + ' ' + splited_log[1]
    table = splited_log[2]

    if table == table_name:
        return datetime.strptime(last_datetime, "%d/%m/%Y %H:%M")
    
    return None
            
def connect_pg(cred):
    """
    Create a connection to the PostgreSQL database using the provided credentials.
    Args:
        cred (dict): A dictionary containing database connection credentials.
    Returns:
        sqlalchemy.engine.base.Engine: The database engine object.
    """
    engine = create_engine(
        f"postgresql+psycopg2://{cred['username']}:{cred['password']}"
        f"@{cred['host']}:{cred['port']}"
        f"/{cred['database']}"
    )
    
    if not engine:
        raise ConnectionError("The connection to the database is not open.")
    
    return engine
 
@app.route('/get_recent_views/<table_name>', methods=['GET'])
def get_recent_views(table_name):
    """
    Fetch recent views of a table based on the last log datetime.
    Args:
        table_name (str): The name of the table to query.
    Returns:
        json: JSON representation of the query results.
    """
    credential = decrypt(ENCRYPTED_FILE_NAME, PRIVATE_KEY_NAME)
    engine     = connect_pg(credential)

    last_datetime = get_last_log_datetime(table_name)

    query = (f"SELECT * "
             f"FROM {credential['database']}.{credential['schema']}.\"{table_name}\" ")

    if last_datetime != None:
        query += f"WHERE \"{COLUMN_DATE_MODIFY}\"::date >= \'{last_datetime}\'"
    
    data = pd.read_sql(query, engine)

    # date_log_event(
    #     LOG_FILE_PATH, 
    #     datetime.now().strftime('%d/%m/%Y %H:%M') + " " + table_name
    # )
    engine.dispose()
    
    return data.to_json(orient='records', date_format="iso")

@app.route('/get_view/<table_name>/data/<date_str>', methods=['GET'])
def get_date_view(table_name, date_str):
    """
    Fetch views of a table for a specific date.
    Args:
        table_name (str): The name of the table to query.
        date_str (str): The date string in 'ddmmyyyy' format.
    Returns:
        json: JSON representation of the query results.
    """
    credential = decrypt(ENCRYPTED_FILE_NAME, PRIVATE_KEY_NAME)
    engine     = connect_pg(credential)

    date_datetime = datetime.strptime(date_str, "%d%m%Y")

    query = (f"SELECT * "
             f"FROM {credential['database']}.{credential['schema']}.\"{table_name}\" "
             f"WHERE \"{COLUMN_DATE_MODIFY}\"::date = \'{date_datetime}\'")

    data = pd.read_sql(query, engine)

    engine.dispose()
 
    return data.to_json(orient='records', date_format="iso")

@app.route('/get_view/<table_name>/periodo/<period>', methods=['GET'])
def get_period_view(table_name, period):
    """
    Fetch views of a table for a specific period.
    Args:
        table_name (str): The name of the table to query.
        period (str): The period string in 'ddmmyyyy' format (first 8 digits as start date, last 8 digits as end date).
    Returns:
        json: JSON representation of the query results.
    """
    credential = decrypt(ENCRYPTED_FILE_NAME, PRIVATE_KEY_NAME)
    engine     = connect_pg(credential)

    beg_date = datetime.strptime(period[:8], "%d%m%Y")
    end_date = datetime.strptime(period[8:], "%d%m%Y")

    query = (f"SELECT * "
             f"FROM {credential['database']}.{credential['schema']}.\"{table_name}\" "
             f"WHERE \"{COLUMN_DATE_MODIFY}\"::date between \'{beg_date}\' and \'{end_date}\'")

    data = pd.read_sql(query, engine)

    engine.dispose()

    return data.to_json(orient='records', date_format="iso")

def validate_api_key(api_key):
    """
    Validate an API key by sending a request to an external authentication service.
    Args:
        api_key (str): The API key to validate.
    Returns:
        bool: True if the API key is valid, otherwise False.
    """
    response = requests.post(EXTERNAL_AUTH_URL, json={'apikey': api_key})
    return response.status_code == 200

@app.before_request
def check_api_key():
    """
    Middleware to check the API key for GET requests. Returns a 403 error if the API key is missing or invalid.
    """
    if request.path != '/' and request.method == 'GET':
        api_key = request.headers.get('apikey')
        if not api_key or not validate_api_key(api_key):
            return jsonify({'error': 'Invalid API key'}), 403

@app.route('/login', methods=['POST'])
def login():
    """
    Handle user login and return an API token upon successful authentication.
    Returns:
        json: JSON response with the API token or error message.
    """
    data     = request.json
    email    = data.get('email')
    password = data.get('password') 

    if not email or not password:
        return jsonify({'error': 'Email and password are required'}), 400

    response = requests.post(EXTERNAL_LOGIN_URL, json=data)

    if response.status_code == 200:
        api_token = response.json().get('apikey')
        return jsonify({'apikey': api_token}), 200
    else:
        return jsonify({'error': 'Login failed'}), response.status_code

@app.route('/', methods=['GET'])
def index():
    return "API Flask está funcionando!", 200

if __name__ == '__main__':
    app.run(debug=True, port=3600)