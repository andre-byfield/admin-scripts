import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

def create_database(account, user, warehouse, role, database_owner_role, database_name):

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
        # Create the role if it doesn't exist
        cursor.execute(f"SHOW ROLES LIKE '{database_owner_role}'")
        if not cursor.fetchone():
            print(f"Role {database_owner_role} does not exist. Creating it...")
            cursor.execute(f"CREATE ROLE IF NOT EXISTS {database_owner_role}")
            print(f"Role {database_owner_role} created successfully.")
            cursor.execute(f"GRANT OWNERSHIP ON ROLE {database_owner_role} TO ROLE SECURITYADMIN") # transfer ownership to SECURITY
            cursor.execute(f"GRANT ROLE {database_owner_role} TO ROLE SYSADMIN") # grant new role to SYSADMIN
            cursor.execute(f"GRANT ROLE {database_owner_role} TO USER \"{user.upper()}\"") # let's add the executor to the new role
        else:
            print(f"Role {database_owner_role} already exists.")

        # Create the database
        cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {database_name}")
            print(f"Database {database_name} created successfully.")
        else:
            print(f"Database {database_name} already exists.")

        # Transfer ownership of the database and public schema to the specified role
        cursor.execute(f"GRANT OWNERSHIP ON DATABASE {database_name} TO ROLE {database_owner_role}")
        print(f"Ownership of database {database_name} transferred to role {database_owner_role}.")

        cursor.execute(f"GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE {database_name} TO ROLE {database_owner_role}")
        print(f"Ownership of all schemas in database {database_name} transferred to role {database_owner_role}.")

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
database_name = os.getenv('DATABASE_NAME')

create_database(account, user, warehouse, role, database_owner_role, database_name)