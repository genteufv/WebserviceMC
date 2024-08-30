from   cryptography.hazmat.primitives.asymmetric    import padding
from   cryptography.hazmat.primitives.serialization import load_pem_private_key
import json
import os

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

def date_log_event(filename, log):
    with open(filename, 'a') as file:
        file.write(log + '\n')

def get_last_log(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            lines = f.readlines()
            if lines:
                return lines[-1].strip()
    return None