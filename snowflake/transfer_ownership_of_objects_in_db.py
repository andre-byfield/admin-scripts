import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

def transfer_ownership(account, user, warehouse, role, database_owner_role, database_name, schema_name):

    counter = 0

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
        # let's build a query to collect all of the schema we'll be working with today
        schema_query = f"""
SELECT schema_name, schema_owner
FROM {database_name}.information_schema.schemata
WHERE schema_name not in ('INFORMATION_SCHEMA', 'PUBLIC')
"""
        # If no schema_name is passed, we'll transfer ownership of the database and everything it contains
        if schema_name is None or schema_name.strip() == "":
            # Transfer database ownership if no schema is passed
            cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
            databases = cursor.fetchall()

            for database in databases:
                database_name = database[1]
                database_owner = database[5]

                if database_owner != database_owner_role:
                    cursor.execute(f"GRANT OWNERSHIP ON DATABASE {database_name} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                    print(f"Transferred ownership of database {database_name} to {database_owner_role}")
                    counter += 1
        else:
            # Otherwise, just get transfer the schema and objects it contains
            schema_query += f"AND schema_name = '{schema_name}'"

        # Get schema
        cursor.execute(schema_query)    
        schemas = cursor.fetchall()

        for schema in schemas:
            schema_name = schema[0]  # Schema name is in the first column
            schema_owner = schema[1] # Schema owner is in the second column

            # Transfer schema ownership
            if schema_owner != database_owner_role:
                cursor.execute(f"GRANT OWNERSHIP ON SCHEMA {database_name}.{schema_name} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                print(f"Transferred ownership of schema {schema_name} to {database_owner_role}")
                counter += 1

            # Transfer ownership of tables
            cursor.execute(f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}")
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[1]  # Table name is in the second column
                table_owner = table[9] # Table owner is in the tenth column

                if table_owner != database_owner_role:
                    cursor.execute(f"GRANT OWNERSHIP ON TABLE {database_name}.{schema_name}.{table_name} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                    print(f"Transferred ownership of table {table_name} in schema {schema_name} to {database_owner_role}")
                    counter += 1

            # Transfer ownership of views
            cursor.execute(f"SHOW VIEWS IN SCHEMA {database_name}.{schema_name}")
            views = cursor.fetchall()

            for view in views:
                view_name = view[1]  # view name is in the second column
                view_owner = view[5] # view owner is in the sixth column

                if view_owner != database_owner_role:
                    cursor.execute(f"GRANT OWNERSHIP ON VIEW {database_name}.{schema_name}.{view_name} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                    print(f"Transferred ownership of view {view_name} in schema {schema_name} to {database_owner_role}")
                    counter += 1

            # Transfer ownership of file formats
            cursor.execute(f"SHOW FILE FORMATS IN SCHEMA {database_name}.{schema_name}")
            file_formats = cursor.fetchall()

            for file_format in file_formats:
                file_format_name = file_format[1]  # file format name is in the second column
                file_format_owner = file_format[5] # file format owner is in the sixth column

                if file_format_owner != database_owner_role:
                    cursor.execute(f"GRANT OWNERSHIP ON FILE FORMAT {database_name}.{schema_name}.{file_format_name} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                    print(f"Transferred ownership of file format {file_format_name} in schema {schema_name} to {database_owner_role}")
                    counter += 1

            # Transfer ownership of sequences
            cursor.execute(f"SHOW SEQUENCES IN SCHEMA {database_name}.{schema_name}")
            sequences = cursor.fetchall()

            for sequence in sequences:
                sequence_name = sequence[0]  # sequence name is in the first column
                sequence_owner = sequence[6] # sequence owner is in the seventh column

                if sequence_owner != database_owner_role:
                    cursor.execute(f"GRANT OWNERSHIP ON SEQUENCE {database_name}.{schema_name}.{sequence_name} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                    print(f"Transferred ownership of sequence {sequence_name} in schema {schema_name} to {database_owner_role}")
                    counter += 1

            # Transfer ownership of tasks
            cursor.execute(f"SHOW TASKS IN SCHEMA {database_name}.{schema_name}")
            tasks = cursor.fetchall()

            for task in tasks:
                task_name = task[1]  # task name is in the second column
                task_owner = task[5] # task owner is in the sixth column

                if task_owner != database_owner_role:
                    cursor.execute(f"GRANT OWNERSHIP ON TASK {database_name}.{schema_name}.{task_name} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                    print(f"Transferred ownership of task {task_name} in schema {schema_name} to {database_owner_role}")
                    counter += 1

            # Transfer ownership of INTERNAL stages
            cursor.execute(f"SELECT STAGE_NAME, STAGE_OWNER FROM {database_name}.INFORMATION_SCHEMA.STAGES WHERE STAGE_TYPE = 'Internal Named' AND STAGE_SCHEMA = '{schema_name}'")
            stages = cursor.fetchall()

            for stage in stages:
                stage_name = stage[0]  # stage name is in the first column
                stage_owner = stage[1] # stage owner is in the second column

                if stage_owner != database_owner_role:
                    cursor.execute(f"GRANT OWNERSHIP ON STAGE {database_name}.{schema_name}.{stage_name} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                    print(f"Transferred ownership of stage {stage_name} in schema {schema_name} to {database_owner_role}")
                    counter += 1

            # Transfer ownership of functions
            cursor.execute(f"SELECT FUNCTION_NAME, FUNCTION_OWNER, ARGUMENT_SIGNATURE FROM {database_name}.INFORMATION_SCHEMA.FUNCTIONS WHERE IS_EXTERNAL = 'NO' AND FUNCTION_SCHEMA = '{schema_name}'")
            functions = cursor.fetchall()

            for function in functions:
                function_name = function[0]  # function name is in the first column
                function_owner = function[1] # function owner is in the second column
                function_signature = function[2] # argument signature is the third column

                if function_owner != database_owner_role:
                    # we've got to parse the data types out of our signature
                    # let's remove the parentheses, swap commas with a space, then tokenize
                    function_tokens = function_signature.replace("(", "").replace(")", "").replace(",", " ").split()

                    # let's build our argument string; each odd index will be a datatype
                    function_argument_string = ""
                    for i in range(len(function_tokens)):
                        if i%2 == 1:
                            function_argument_string += function_tokens[i] + ','
                        
                    function_argument_string = "(" + function_argument_string[:-1] + ")"

                    if function_owner != database_owner_role:
                        cursor.execute(f"GRANT OWNERSHIP ON FUNCTION {database_name}.{schema_name}.{function_name}{function_argument_string} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                        print(f"Transferred ownership of function {function_name} in schema {schema_name} to {database_owner_role}")
                        counter += 1

            # Transfer ownership of procedures
            cursor.execute(f"SELECT PROCEDURE_NAME, PROCEDURE_OWNER, ARGUMENT_SIGNATURE FROM {database_name}.INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_SCHEMA = '{schema_name}'")
            procedures = cursor.fetchall()

            for procedure in procedures:
                procedure_name = procedure[0]  # procedure name is in the first column
                procedure_owner = procedure[1] # procedure owner is in the second column
                procedure_signature = procedure[2] # argument signature is the third column

                if procedure_owner != database_owner_role:
                    # we've got to parse the data types out of our signature
                    # let's remove the parentheses, swap commas with a space, then tokenize
                    procedure_tokens = procedure_signature.replace("(", "").replace(")", "").replace(",", " ").split()

                    # let's build our argument string; each odd index will be a datatype
                    procedure_argument_string = ""
                    for i in range(len(procedure_tokens)):
                        if i%2 == 1:
                            procedure_argument_string += procedure_tokens[i] + ','
                        
                    procedure_argument_string = "(" + procedure_argument_string[:-1] + ")"

                    if procedure_owner != database_owner_role:
                        cursor.execute(f"GRANT OWNERSHIP ON PROCEDURE {database_name}.{schema_name}.{procedure_name}{procedure_argument_string} TO ROLE {database_owner_role} COPY CURRENT GRANTS")
                        print(f"Transferred ownership of procedure {procedure_name} in schema {schema_name} to {database_owner_role}")
                        counter += 1

        # Let's wrap this up
        if counter == 0:
            print("Nothing to do.")
        else:
            print("Ownership transfer completed successfully.")

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
schema_name = os.getenv('SCHEMA_NAME') # empty string will transfer all schema in database

# Execute the transfer
transfer_ownership(account, user, warehouse, role, database_owner_role, database_name, schema_name)
