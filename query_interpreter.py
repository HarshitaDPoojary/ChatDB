import pymysql
from difflib import get_close_matches
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re
from nltk.stem import WordNetLemmatizer
import os

# Download NLTK data files (ensure this runs only once)
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Initialize NLTK components
lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words('english'))

# Natural language to SQL operator mapping
NL_TO_SQL_OPERATOR = {
    "less than": "<",
    "at most": "<=",
    "less than or equal": "<=",  # Handle missing "to"
    "greater than": ">",
    "more than": ">",
    "at least": ">=",
    "greater than or equal to": ">=",  # Handle missing "to"
    "equal": "=",
    "equal to": "=",
    "is equal": "=",
    "is not equal to": "!=",
    "not equal to": "!=",
}

# Natural language to SQL aggregation mapping
NL_TO_SQL_AGGREGATION = {
    "total": "SUM",
    "sum": "SUM",
    "average": "AVG",
    "mean": "AVG",
    "maximum": "MAX",
    "max": "MAX",
    "minimum": "MIN",
    "min": "MIN",
    "count": "COUNT",
    "number of": "COUNT",
}

# Wrap identifiers with backticks
def wrap_identifier(identifier):
    return f"`{identifier}`"


# Remove backticks from column or table names for descriptions
def clean_identifier(identifier):
    return identifier.replace("`", "")

# Connect to the database
def connect_to_database(db_name="dsci551"):
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="harshita@97",  # Replace with your MySQL root password
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection

# Extract database schema
def get_database_schema(connection):
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    schema = {}
    for table in tables:
        cursor.execute(f"DESCRIBE {wrap_identifier(table)};")
        columns = [row[0] for row in cursor.fetchall()]  
        # Access the first element of each tuple
        schema[table] = columns
    return schema

def preprocess_query(query, schema):
    """
    Preprocess the query to tokenize it, handle multi-word keywords, treat quoted strings as single tokens,
    and maintain proper case for quoted strings while removing enclosing quotes after tokenization.
    """
    modified_query = query
    all_columns = [col for cols in schema.values() for col in cols]

    # Ensure column names are preserved and not modified
    for column in all_columns:
        column_with_spaces = column.replace("_", " ")  # Reverse schema format for comparison
        if column_with_spaces in query:
            query = query.replace(column_with_spaces, column)

    # Preprocess to keep multi-word keywords as a single token
    for phrase in NL_TO_SQL_OPERATOR.keys():
        # Replace spaces in multi-word keywords with underscores temporarily
        query = re.sub(rf'\b{re.escape(phrase)}\b', phrase.replace(" ", "_"), query, flags=re.IGNORECASE)

    # Handle quoted strings as single tokens
    quoted_strings = re.findall(r'"(.*?)"', query)
    quoted_replacements = {}
    for idx, quoted in enumerate(quoted_strings):
        placeholder = f"_QUOTED_{idx}_"
        quoted_replacements[placeholder] = quoted  # Map placeholders to original quoted strings
        query = query.replace(f'"{quoted}"', placeholder)

    # Tokenize and lemmatize the query
    tokens = word_tokenize(query, language='english', preserve_line=True)
    filtered_tokens = []
    for token in tokens:
        if token in quoted_replacements:
            # Restore original quoted string without enclosing quotes
            filtered_tokens.append(quoted_replacements[token])
        else:
            # Lowercase other tokens and lemmatize
            token_lower = lemmatizer.lemmatize(token.lower())
            if token_lower not in stop_words or token_lower in all_columns:
                filtered_tokens.append(token_lower)

    # Revert multi-word keywords back to their original form in tokens
    filtered_tokens = [
        token.replace("_", " ") if "_QUOTED_" not in token and token not in all_columns else token
        for token in filtered_tokens
    ]

    return query, filtered_tokens



# Fuzzy match using difflib
def fuzzy_match_difflib(token, items, cutoff=0.7):
    matches = get_close_matches(token, items, n=1, cutoff=cutoff)
    return matches[0] if matches else None

# Map tokens to schema
def map_tokens_to_schema(tokens, schema):
    mapped = {"tables": [],"all_columns": {}, "columns": {}, "operations": [], "conditions": []}
    all_tables = list(schema.keys())

    # Match tables
    for token in tokens:
        matched_table = fuzzy_match_difflib(token, all_tables)
        if matched_table and matched_table not in mapped["tables"]:
            mapped["tables"].append(matched_table)
    
    table_tokens = set(mapped["tables"])  # Use a set for faster lookup
    filtered_tokens = [token for token in tokens if token not in table_tokens]

    # Match columns within identified tables
    if mapped["tables"]:
        for table in mapped["tables"]:
            mapped["all_columns"][table] = schema[table]
            mapped["columns"][table] = []
            for token in filtered_tokens:
                matched_column = fuzzy_match_difflib(token, schema[table])
                # print('matched_column', matched_column)
                if matched_column and matched_column not in mapped["columns"][table]:
                    mapped["columns"][table].append(matched_column)

            # all_columns.extend(schema[table])

        # for token in tokens:
        #     matched_column = fuzzy_match_difflib(token, all_columns)
        #     if matched_column and matched_column not in mapped["columns"]:
        #         mapped["columns"].append(matched_column)


    return mapped

# Detect conditions for WHERE clause
def detect_where_conditions(tokens, schema):
    conditions = []
    all_columns = [col for cols in schema.values() for col in cols]

    i = 0
    while i < len(tokens):
        if tokens[i] in all_columns:
            # Check for natural language operators
            for phrase_length in [3, 2, 1]:
                potential_phrase = " ".join(tokens[i + 1:i + 1 + phrase_length])
                if potential_phrase in NL_TO_SQL_OPERATOR:
                    operator = NL_TO_SQL_OPERATOR[potential_phrase]
                    value_index = i + 1 + phrase_length
                    value = tokens[value_index] if value_index < len(tokens) else None
                    if value and value.isdigit():
                        value = int(value)
                    elif value:
                        value = f"'{value}'"
                    conditions.append((tokens[i], operator, value))
                    i = value_index + 1
                    break
            else:
                i += 1
        else:
            i += 1
    return conditions

# Detect JOIN context
def detect_join(query, tokens):
    join_keywords = ["join", "combine", "merge", "along with"]
    query_lower = query.lower()
    return any(token in join_keywords for token in tokens)  or any(word in query_lower for word in join_keywords)

lemmatizer = WordNetLemmatizer()

def get_singular_table_name(table_name):
    """
    Get the singular form of the table name from the CSV file name.
    """
    singular_name = lemmatizer.lemmatize(table_name.lower())  # Convert to singular
    return singular_name

def find_related_tables(connection, schema):
    """
    Efficiently infer relationships between tables based on common columns and primary/foreign key patterns.
    """
    related_tables = {}
    table_columns = {table: set(columns) for table, columns in schema.items()}

    # Detect primary keys for each table
    primary_keys = {}
    for table, columns in table_columns.items():
        singular_table_name = get_singular_table_name(table)  # Singular form of the table name
        candidate_primary_keys = [f"{singular_table_name}_id", f"{singular_table_name}id", f"{singular_table_name.replace('_','')}id" ]
        # print(candidate_primary_keys)
        for candidate_primary_key in candidate_primary_keys:
            if candidate_primary_key in table_columns[table]:
                primary_keys[table] = candidate_primary_key

    # Identify relationships based on matching column names
    for table1, columns1 in table_columns.items():
        for table2, columns2 in table_columns.items():
            if table1 != table2:
                # Find common columns
                common_columns = columns1 & columns2

                # Check for valid join conditions based on primary/foreign key patterns
                valid_common_columns = [
                    col for col in common_columns
                    if col in primary_keys.values() or col.endswith("_id")
                ]

                if valid_common_columns:
                    related_tables[(table1, table2)] = valid_common_columns

    return related_tables

# # Find related tables
# def find_related_tables(connection, schema):
#     related_tables = {}
#     cursor = connection.cursor()

#     for table1, columns1 in schema.items():
#         for table2, columns2 in schema.items():
#             if table1 != table2:
#                 # Find common columns
#                 common_columns = list(set(columns1) & set(columns2))

#                 valid_common_columns = []
#                 for column in common_columns:
#                     # Check if joining on this column produces results
#                     join_query = (
#                         f"SELECT * FROM {wrap_identifier(table1)} "
#                         f"JOIN {wrap_identifier(table2)} ON {wrap_identifier(table1)}.{wrap_identifier(column)} = {wrap_identifier(table2)}.{wrap_identifier(column)} "
#                         "LIMIT 1;"
#                     )

#                     try:
#                         cursor.execute(join_query)
#                         if cursor.fetchone():  # If at least one result exists
#                             valid_common_columns.append(column)
#                     except pymysql.Error as e:
#                         print(f"Error executing join query: {e}")
#                         continue

#                 if valid_common_columns:
#                     related_tables[(table1, table2)] = valid_common_columns

#     return related_tables

# Detect GROUP BY context
def detect_group_by(tokens, schema):
    group_by_keywords = ["group", "by", 'grouped']
    all_columns = [col for cols in schema.values() for col in cols]
    
    for i, token in enumerate(tokens):
        if token in group_by_keywords and i + 1 < len(tokens):
            column = fuzzy_match_difflib(tokens[i + 1], all_columns)
            if column:
                return column
    return None

# Detect aggregation functions
def detect_aggregation(tokens, schema):
    for i, token in enumerate(tokens):
        if token in NL_TO_SQL_AGGREGATION:
            agg_func = NL_TO_SQL_AGGREGATION[token]
            # Look for the column following the aggregation term
            all_columns = [col for cols in schema.values() for col in cols]
                
            agg_column = fuzzy_match_difflib(tokens[i + 1], all_columns) if i + 1 < len(tokens) else None
            if agg_column:
                return agg_func, agg_column
    return None, None

# Detect limit and offset
def detect_limit_and_offset(tokens):
    limit = None
    offset = 0  # Default offset is 0

    for i, token in enumerate(tokens):
        if token.isdigit():
            num = int(token)
            if i > 0 and tokens[i - 1] in ["top", "first", "last"]:
                limit = num
            elif i > 0 and tokens[i - 1] in ["skip", "offset", "after"]:
                offset = num
    return limit, offset

def detect_limit_and_sort(query, tokens, schema):
    sort_column = None
    sort_order = None

    all_columns = [col for cols in schema.values() for col in cols]

    for i in range(len(tokens) - 1):
        # Check for 'order by' pattern
        if tokens[i] in ["order", "ordered", "sort", "sorted"]:
            # Look for a column name after 'order by'
            sort_column = fuzzy_match_difflib(tokens[i + 1], all_columns)
            # Check for sorting direction (asc/desc)
            sort_order = "ASC"
            if i + 2 < len(tokens): 
                if tokens[i + 2] in ["ascending", "asc", "descending", "desc"]:
                    sort_order = "ASC" if tokens[i + 2] in ["ascending", "asc"] else "DESC"
            else:
                # Default to ascending if no direction is specified
                sort_order = "ASC"
            break  # Exit loop after detecting 'order by'

    return sort_column, sort_order

def generate_sql_query(
    connection,
    user_query,
    mapped,
    keywords,
    limit=None,
    offset=0,
    sort_order=None,
    join=False,
    related_tables=None,
    conditions=None,
    group_by=None,
    aggregation=None,
    schema=None,
    sort_column=None
):

    if not mapped["tables"]:
        return "Error: Could not identify any table in the query."

    table = mapped["tables"][0]
    select_columns = set(mapped["columns"].get(table, []))  # Start with mapped columns for the main table

    # Initialize query parts
    join_clause = ""
    where_clause = ""
    group_by_clause = ""
    order_by_clause = ""
    limit_clause = ""
    select_clause = ""

    # Handle JOIN logic
    if join and len(mapped["tables"]) > 1:
        join_table = mapped["tables"][1]
        if (table, join_table) in related_tables:
            common_columns = related_tables[(table, join_table)]
            common_column = next((col for col in common_columns if 'id' in col), common_columns[0] )

            join_clause = (
                f"JOIN {wrap_identifier(join_table)} "
                f"ON {wrap_identifier(table)}.{wrap_identifier(common_column)} = {wrap_identifier(join_table)}.{wrap_identifier(common_column)}"
            )

            # Add columns from both tables to SELECT
            select_columns.clear()
            select_columns.update(
                [
                    f"{wrap_identifier(table)}.{wrap_identifier(col)} AS {wrap_identifier(clean_identifier(table) + '_' + clean_identifier(col))}"
                    for col in mapped["columns"].get(table, schema[table]) if col != common_column
                ]
            )
            select_columns.update(
                [
                    f"{wrap_identifier(join_table)}.{wrap_identifier(col)} AS {wrap_identifier(clean_identifier(join_table) + '_' + clean_identifier(col))}"
                    for col in mapped["columns"].get(join_table, schema[join_table]) if col != common_column
                ]
            )
            if sort_column:
                # Check if sort_column belongs to one of the tables
                if sort_column in schema[table]:
                    sort_column = f"{wrap_identifier(table)}.{wrap_identifier(sort_column)}"
                elif sort_column in schema[join_table]:
                    sort_column = f"{wrap_identifier(join_table)}.{wrap_identifier(sort_column)}"
        else:
            return "Error: Could not find a valid join condition."
    else:
        join_clause = ""  # No JOIN needed

    # Handle aggregation and group-by
    if aggregation:
        agg_func, agg_column = aggregation
        if agg_column:
            agg_expr = f"{agg_func}({wrap_identifier(agg_column)}) AS {agg_func.lower()}_{clean_identifier(agg_column)}"
            if group_by:
                group_by_col = wrap_identifier(group_by)
                select_columns = {group_by_col, agg_expr}
                group_by_clause = f"GROUP BY {group_by_col}"
            else:
                select_columns = {agg_expr}
            if sort_column == agg_column:
                    sort_column = f"{agg_func.lower()}_{clean_identifier(agg_column)}"

    # Add WHERE clause
    if conditions:
        where_clauses = []
        for column, operator, value in conditions:
            if join:
                # Add table aliases to column names for JOINs
                for tbl in mapped["tables"]:
                    if column in schema.get(tbl, []):
                        column = f"{wrap_identifier(tbl)}.{wrap_identifier(column)}"
                        break
            else:
                column = wrap_identifier(column)
            where_clauses.append(f"{column} {operator} {value}")
        where_clause = f"WHERE {' AND '.join(where_clauses)}"

    # Add ORDER BY clause
    if sort_column and sort_order:
        order_column = wrap_identifier(sort_column) if not join else sort_column
        order_by_clause = f"ORDER BY {order_column} {sort_order}"

    # Add LIMIT and OFFSET clauses
    if limit:
        limit_clause = f"LIMIT {limit}"
    if offset:
        limit_clause += f" OFFSET {offset}" if limit_clause else f"OFFSET {offset}"

    # Finalize SELECT clause
    if not select_columns:
        # Default to SELECT * if no columns are explicitly mentioned
        select_columns = {"*"}
    select_columns_list = ", ".join(select_columns)
    select_clause = f"SELECT {select_columns_list}"

    # Construct the final query with proper clause ordering
    query = f"""
    {select_clause}
    FROM {wrap_identifier(table)}
    {join_clause}
    {where_clause}
    {group_by_clause}
    {order_by_clause}
    {limit_clause}
    """.strip()
    query = re.sub(r'\s+', ' ', query).strip()


    # Print the generated query for debugging
    # print(f"Generated SQL Query: {query}")
    return query

# Execute the generated SQL query
def execute_sql_query(query, connection):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return True, results
    except pymysql.Error as e:
        return False, f"Error executing query: {e}"

# Interpret user query
def interpret_user_query(query, connection):
    schema = get_database_schema(connection)
    related_tables = find_related_tables(connection, schema)
    query, tokens = preprocess_query(query, schema)
    # print(tokens)
    # print(related_tables)
    mapped = map_tokens_to_schema(tokens, schema)
    # print("mapped")
    # print(mapped)
    join = detect_join(query, tokens)
    limit, offset = detect_limit_and_offset(tokens)
    sort_column, sort_order = detect_limit_and_sort(query, tokens, schema)  # Updated to get sort_column and sort_order
    group_by = detect_group_by(tokens, schema)
    conditions = detect_where_conditions(tokens, schema)
    aggregation = detect_aggregation(tokens, schema)

    # print('join', join, ' limit', limit, ' offset', offset, ' sort_column', sort_column, ' sort_order', sort_order, ' group by', group_by, ' conditions', conditions, ' aggregation', aggregation)

    sql_query = generate_sql_query(connection, query, mapped, [], limit, offset, sort_order, join, related_tables, conditions, group_by, aggregation, schema, sort_column)
    # print(sql_query)
    if "Error" in sql_query:
        return sql_query, False, "Could not generate a valid query. Please rephrase your request."

    success, results_or_error = execute_sql_query(sql_query, connection)

    # explanation = generate_query_explanation(mapped, conditions, group_by, aggregation, join)

    if success:
        return sql_query, True, results_or_error
    else:
        return sql_query, False, results_or_error

