import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

def add_user_to_role(account, user, warehouse, role, database_owner_role, user_to_add):

    conn_params = {
        'account': account,
        'user': user,
        'warehouse': warehouse,
        'role': role,
        'authenticator': 'externalbrowser'
    }

    # Establish connection
    conn = snowflake.connector.connect(**conn_params)
    cursor = conn.cursor()

    try:

        cursor.execute(f"SHOW ROLES LIKE '{database_owner_role}'")
        if cursor.fetchone():
            cursor.execute(f"GRANT ROLE {database_owner_role} TO USER \"{user_to_add.upper()}\"")
            print(f"User {user_to_add} added to role {database_owner_role} successfully.")
        else:
            print(f"Role {database_owner_role} does not exist.")

    except ProgrammingError as e:
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        conn.close()

# parameters
load_dotenv()

account = os.getenv('ACCOUNT')
user = os.getenv('USER')
warehouse = os.getenv('WAREHOUSE')
role = os.getenv('ROLE')

database_owner_role = os.getenv('DATABASE_OWNER_ROLE')
user_to_add = os.getenv('USER_TO_ADD')

add_user_to_role(account, user, warehouse, role, database_owner_role, user_to_add)    