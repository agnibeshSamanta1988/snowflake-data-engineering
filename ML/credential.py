# credential.py
# Securely store your Snowflake connection parameters here.
# NEVER commit this file to version control (add to .gitignore).
# Use Snowflake account identifier (e.g., 'xy12345.us-east-1'), username, password/private key, etc.
# Full docs: https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session#creating-a-session [web:1]

params = {
    "account": "<account>",  # e.g., 'xy12345.east-us-2.azure'
    "user": "<user>",
    "password": "<password>",  # Or use private_key for key-pair auth
    "warehouse": "<warehouse>",  # Default warehouse
    "database": "<database>",  # Matches your notebook
    "schema": "<schema>"        # Matches your notebook
}

# Optional: For key-pair authentication (recommended for production)
# from cryptography.hazmat.primitives import serialization
# with open("rsa_key.p8", "rb") as key:
#     p_key= serialization.load_pem_private_key(
#         key.read(),
#         password="<your_key_passphrase>".encode(),
#     )
# params["private_key"] = p_key

# You can get the detail by executing this select statement
#SELECT 
#  CURRENT_ACCOUNT() as account_locator,  -- Account ID
#  CURRENT_USER() as username,
#  CURRENT_WAREHOUSE() as warehouse,
#  CURRENT_DATABASE() as database,
#  CURRENT_SCHEMA() as schema;
