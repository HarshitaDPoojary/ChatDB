import pymysql
import pandas as pd
import os
from tabulate import tabulate
from nltk.stem import WordNetLemmatizer
from rapidfuzz import process, fuzz

# Function to connect to MySQL
def connect_to_mysql(db_name=None):
    """
    Connect to MySQL. If db_name is provided, connect to that database.
    """
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="harshita@97",  # Replace with your MySQL root password
        database=db_name
    )
    return connection

# Function to create the database if it doesn't exist
def create_database(db_name="dsci551"):
    """
    Create a database if it does not already exist.
    """
    connection = connect_to_mysql()
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`;")
    connection.commit()
    connection.close()
    print(f"Database `{db_name}` is ready.")

# Function to reset the database (drop all tables)
def reset_database(connection):
    """
    Drop all tables in the database.
    """
    cursor = connection.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS `{table[0]}`;")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    connection.commit()
    print("All existing tables dropped.")

# Function to infer MySQL column types from Pandas DataFrame
def infer_column_types(df):
    """
    Infer MySQL column types from a Pandas DataFrame.
    """
    column_types = {}
    for column in df.columns:
        dtype = df[column].dtype
        if pd.api.types.is_integer_dtype(dtype):
            column_types[column] = "INT"
        elif pd.api.types.is_float_dtype(dtype):
            column_types[column] = "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            column_types[column] = "DATETIME"
        else:
            max_length = df[column].astype(str).map(len).max()
            column_types[column] = f"VARCHAR({max_length if max_length > 0 else 255})"
    return column_types

# # Function to create a table and insert data into MySQL
# def upload_csv_to_mysql(file_path, connection):
#     """
#     Upload a CSV file into MySQL by creating a table dynamically.
#     """
#     table_name = os.path.splitext(os.path.basename(file_path))[0]
#     try:
#         df = pd.read_csv(file_path, encoding="utf-8")
#     except UnicodeDecodeError:
#         print(f"UTF-8 decoding failed for {file_path}, trying ISO-8859-1.")
#         df = pd.read_csv(file_path, encoding="ISO-8859-1")
    
#     column_types = infer_column_types(df)

#     # Generate SQL for creating table
#     columns_def = ", ".join([
#         f"`{col.replace(' ', '_').lower()}` {dtype}" for col, dtype in column_types.items()
#     ])
#     create_table_query = f"CREATE TABLE `{table_name}` ({columns_def});"

#     cursor = connection.cursor()
#     try:
#         cursor.execute(create_table_query)
#         print(f"Table `{table_name}` created successfully.")

#         # Insert data
#         for _, row in df.iterrows():
#             values = []
#             for col, value in zip(df.columns, row.values):
#                 # Get the data type for the column
#                 column_type = column_types[col]
#                 if pd.isna(value) or value == "NULL":  # Check for NaN or NULL
#                     values.append("NULL")
#                 elif column_type in ["INT", "FLOAT"]:  # No quotes for numbers
#                     values.append(str(value))
#                 else:  # Escape and quote strings
#                     escaped_value = str(value).replace("'", "''")
#                     values.append(f"'{escaped_value}'")
#             values = ", ".join(values)
#             insert_query = f"INSERT INTO `{table_name}` VALUES ({values});"
#             cursor.execute(insert_query)
#         connection.commit()
#         print(f"Data from `{file_path}` inserted into `{table_name}`.")
#     except Exception as e:
#         print(f"Error processing `{file_path}`: {e}")



# Initialize NLTK lemmatizer
lemmatizer = WordNetLemmatizer()

def get_singular_table_name(file_name):
    """
    Get the singular form of the table name from the CSV file name.
    """
    table_name = os.path.splitext(file_name)[0]
    singular_name = lemmatizer.lemmatize(table_name.lower())  # Convert to singular
    return singular_name


def find_foreign_keys(all_dataframes):
    """
    Infer foreign key relationships based on primary keys and table names.
    """
    foreign_keys = {}
    table_columns = {table: df.columns.tolist() for table, df in all_dataframes.items()}

    # Detect primary keys for each table
    primary_keys = {}
    for table in table_columns:
        singular_table_name = get_singular_table_name(table)  # Singular form of the table name
        candidate_primary_keys = [f"{singular_table_name}_id", f"{singular_table_name}id", f"{singular_table_name.replace('_','')}id" ]
        # print(candidate_primary_keys)
        for candidate_primary_key in candidate_primary_keys:
            if candidate_primary_key in table_columns[table]:
                primary_keys[table] = candidate_primary_key

    # Detect foreign keys based on primary keys
    for table, columns in table_columns.items():
        for column in columns:
            # Check if the column matches a primary key in another table
            for referenced_table, primary_key in primary_keys.items():
                if column == primary_key and referenced_table != table:
                    foreign_keys.setdefault(table, []).append((column, referenced_table))

    return foreign_keys

def upload_csv_to_mysql(file_path, connection, all_dataframes, foreign_keys, batch_size=1000):
    """
    Upload a CSV file into MySQL by creating a table dynamically.
    Handles primary key and foreign key constraints and uses batch inserts for faster data insertion.
    """
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    df = all_dataframes[table_name]
    column_types = infer_column_types(df)

    # Identify the primary key for the current table
    singular_table_name = get_singular_table_name(table_name)  # Singular form of the table name
    possible_primary_keys = [f"{singular_table_name}_id", f"{singular_table_name}id", f"{singular_table_name.replace('_', '')}id"]

    # Generate SQL for creating table
    columns_def = []
    for col, dtype in column_types.items():
        col_new = col.replace(' ', '_').lower()
        column_def = f"`{col.replace(' ', '_').lower()}` {dtype}"
        if col_new in possible_primary_keys:  # Declare the primary key
            column_def += " PRIMARY KEY"
        # Add foreign key constraints if applicable
        for fk_col, referenced_table in foreign_keys.get(table_name, []):
            if col_new == fk_col:
                column_def += f" REFERENCES {referenced_table}({fk_col})"
                break
        columns_def.append(column_def)
    columns_def = ", ".join(columns_def)
    create_table_query = f"CREATE TABLE `{table_name}` ({columns_def});"

    cursor = connection.cursor()
    try:
        print(create_table_query)  # Debugging: Print the create table query
        cursor.execute(create_table_query)
        print(f"Table `{table_name}` created successfully.")

        # Prepare columns for the INSERT statement
        columns_list = ", ".join([f"`{col.replace(' ', '_').lower()}`" for col in df.columns])

        # Batch insert records
        rows = []
        for _, row in df.iterrows():
            values = []
            for col, value in zip(df.columns, row.values):
                column_type = column_types[col]
                if pd.isna(value) or value == "NULL":
                    values.append("NULL")
                elif column_type in ["INT", "FLOAT"]:
                    values.append(str(value))
                else:
                    escaped_value = str(value).replace("'", "''")
                    values.append(f"'{escaped_value}'")
            rows.append(f"({', '.join(values)})")

            # Insert in batches
            if len(rows) >= batch_size:
                insert_query = f"INSERT INTO `{table_name}` ({columns_list}) VALUES {', '.join(rows)};"
                cursor.execute(insert_query)
                rows = []  # Clear the batch after insertion

        # Insert remaining rows
        if rows:
            insert_query = f"INSERT INTO `{table_name}` ({columns_list}) VALUES {', '.join(rows)};"
            cursor.execute(insert_query)

        connection.commit()
        print(f"Data from `{file_path}` inserted into `{table_name}`.")
    except Exception as e:
        print(f"Error processing `{file_path}`: {e}")

def process_csv_folder(folder_path, connection):
    """
    Process all CSV files in a folder, dynamically detect schema, and handle foreign key relationships.
    """
    all_dataframes = {}
    foreign_keys = {}

    # Read all CSV files and infer schema
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            table_name = os.path.splitext(file_name)[0]
            try:
                df = pd.read_csv(file_path, encoding="utf-8")
            except UnicodeDecodeError:
                # print(f"UTF-8 decoding failed for {file_path}, trying ISO-8859-1.")
                df = pd.read_csv(file_path, encoding="ISO-8859-1")
            all_dataframes[table_name] = df

    # Find foreign keys across all dataframes
    foreign_keys = find_foreign_keys(all_dataframes)

    # Split tables into independent and dependent
    independent_tables = [
        table for table in all_dataframes if table not in foreign_keys
    ]
    dependent_tables = [
        table for table in all_dataframes if table in foreign_keys
    ]

    # Upload independent tables first
    for table in independent_tables:
        file_path = os.path.join(folder_path, f"{table}.csv")
        upload_csv_to_mysql(file_path, connection, all_dataframes, foreign_keys)

    # Upload dependent tables
    for table in dependent_tables:
        file_path = os.path.join(folder_path, f"{table}.csv")
        upload_csv_to_mysql(file_path, connection, all_dataframes, foreign_keys)


def execute_query(connection, query):
    """
    Executes a query and displays the results in a formatted table.

    Args:
        connection: Database connection object.
        query (str): SQL query to execute.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        print("Query: ", query)

        print("\nQuery Results:")
        print(tabulate(results, headers=columns, tablefmt="outline"))
    except Exception as e:
        print(f"Error executing query: {e}")