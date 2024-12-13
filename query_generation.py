import random
import re

# Wrap identifiers with backticks to handle spaces or special characters in SQL queries
def wrap_identifier(identifier):
    return f"`{identifier}`"

# Remove backticks from column or table names for descriptions
def clean_identifier(identifier):
    return identifier.replace("`", "")

# Extract columns by type (categorical and numeric)
def extract_columns_by_type(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"DESCRIBE {wrap_identifier(table_name)};")
    columns = cursor.fetchall()
    categorized = {"numeric": [], "categorical": []}
    for col_name, col_type, *_ in columns:
        if "int" in col_type or "float" in col_type:
            categorized["numeric"].append(wrap_identifier(col_name))
        else:
            categorized["categorical"].append(wrap_identifier(col_name))
    return categorized

# Check for evenly distributed group-by candidates
def is_evenly_distributed(connection, table_name, column_name):
    cursor = connection.cursor()
    query = f"""
        SELECT {column_name}, COUNT(*) AS value_count
        FROM {wrap_identifier(table_name)}
        GROUP BY {column_name}
    """
    cursor.execute(query)
    results = cursor.fetchall()

    total_rows = sum(row[1] for row in results)
    distribution_ratios = [row[1] / total_rows for row in results]

    evenly_distributed_threshold = 0.25
    evenly_distributed = [ratio for ratio in distribution_ratios if ratio >= evenly_distributed_threshold]
    return len(evenly_distributed) > 1

def find_aggregation_column(columns):
    for col in columns["numeric"]:
        if re.search(r"(price|quantity|budget|amount|total)", col.lower()):
            return col
    return None

def add_where_clause(query, connection, table_name, columns, description):
    operator_mapping = {
        '>': ' greater than',
        '<': ' less than',
        '>=': ' at least',
        '<=': ' at most',
        '=': '',
        '!=': ' not equal to'
    }
    if columns["numeric"]:
        col = random.choice(columns["numeric"])
        cursor = connection.cursor()
        cursor.execute(f"SELECT MIN({col}), MAX({col}) FROM {wrap_identifier(table_name)}")
        min_val, max_val = cursor.fetchone()
        if min_val is not None and max_val is not None:
            if isinstance(min_val, int) and isinstance(max_val, int):
                value = random.randint(min_val, max_val)  # Use randint for integers
            else:
                value = round(random.uniform(min_val, max_val), 2)  # Use uniform for floats, round to 2 decimals
            
            operator = random.choice([">", "<", ">=", "<=", "=", "!="])

            query_column = col
            if 'JOIN' in query:
                query_column = f"{wrap_identifier(table_name)}.{col}"
            query = f"{query} WHERE {query_column} {operator} {value}"
            description += f" where {clean_identifier(col)} is{operator_mapping[operator]} {value}"
    elif columns["categorical"]:
        col = random.choice(columns["categorical"])
        cursor = connection.cursor()
        cursor.execute(f"SELECT DISTINCT {col} FROM {wrap_identifier(table_name)} LIMIT 10")
        distinct_values = [row[0] for row in cursor.fetchall()]
        if distinct_values:
            value = random.choice(distinct_values)
            operator = random.choice(["=", "!="])
            query_column = col
            if 'JOIN' in query:
                query_column = f"{wrap_identifier(table_name)}.{col}"
            query = f"{query} WHERE {query_column} {operator} '{value}'"
            description += f" where {clean_identifier(col)} is {operator_mapping[operator]} '{value}'"
    return query, description

# Add a GROUP BY clause
def add_group_by_clause(query, connection, table_name, columns, select_columns, description, desc_column):
    excluded_columns = [
        col for col in columns["categorical"] + columns["numeric"]
        if re.search(r"(price|quantity|budget|amount|total)", col.lower())
    ]
    group_by_candidates = [col for col in columns["categorical"] + columns["numeric"]  if col not in excluded_columns]

    if group_by_candidates:
        for group_by_col in group_by_candidates:
            if is_evenly_distributed(connection, table_name, group_by_col):
                group_by_clause = f"GROUP BY {group_by_col}"

                # Choose a random aggregation function (SUM, AVG, MAX, MIN, COUNT)
                aggregation_functions = {
                    "SUM": "total",
                    "MAX": "maximum",
                    "MIN": "minimum",
                    "AVG": "average",
                    "COUNT": "count",
                }
                agg_function = random.choice(list(aggregation_functions.keys()))

                # Find a numeric column for aggregation
                aggregation_column = find_aggregation_column(columns)
                if aggregation_column:
                    updated_select = {group_by_col, f"{agg_function}({aggregation_column}) AS {wrap_identifier(aggregation_functions[agg_function]+ '_'+ clean_identifier(aggregation_column))}"}
                    description += f" by {clean_identifier(group_by_col)}"
                    desc_column = f"{aggregation_functions[agg_function]} {clean_identifier(aggregation_column)}"
                else:
                    # Default to COUNT(*) if no numeric column is found
                    updated_select = {group_by_col, "COUNT(*)"}
                    description += f" by {clean_identifier(group_by_col)}"
                    desc_column = f"count of records"

                select_columns.clear()
                select_columns.update(updated_select)
                query = f"{query} {group_by_clause}"
                return query, description, desc_column
    return query, description, desc_column

def add_order_by_clause(query, table_name, columns, select_columns, description):
    if "GROUP BY" in query:
        col = random.choice(list(select_columns))
        if 'JOIN' in query:
            col = f"{wrap_identifier(table_name)}.{col}"
    else:
        col = random.choice(columns["numeric"] + columns["categorical"])
        if 'JOIN' in query:
            col = f"{wrap_identifier(table_name)}.{col}"
        select_columns.add(col)
    
    direction = random.choice(["ASC", "DESC"])

    direction_mapping = {
        "ASC": "chronological",
        "DESC": "reverse chronological"
    }
    query = f"{query} ORDER BY {col} {direction}"
    description += f" sorted by {clean_identifier(col)} in {direction_mapping[direction]} order"
    return query, description

# Add a JOIN clause
# Add a JOIN clause
def add_join_clause(connection, query, table1, related_tables, select_columns, description, desc_column):
    if table1 in related_tables and related_tables[table1]:
        # Choose a related table and join columns
        related_table, common_columns = random.choice(list(related_tables[table1].items()))
        col1, col2 = random.choice(common_columns)

        # Fetch columns for table1
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE {wrap_identifier(table1)};")
        table1_columns = [col[0] for col in cursor.fetchall()]

        # Fetch columns for related_table
        cursor.execute(f"DESCRIBE {wrap_identifier(related_table)};")
        related_table_columns = [col[0] for col in cursor.fetchall()]

        # Alias columns from table1, excluding the join column
        table1_columns_with_alias = [
            f"{wrap_identifier(table1)}.{wrap_identifier(col)} AS {wrap_identifier(clean_identifier(table1) + '_' + clean_identifier(col))}"
            for col in table1_columns if col != col1
        ]

        # Alias columns from related_table, excluding the join column
        related_table_columns_with_alias = [
            f"{wrap_identifier(related_table)}.{wrap_identifier(col)} AS {wrap_identifier(clean_identifier(related_table) + '_' + clean_identifier(col))}"
            for col in related_table_columns if col != col2
        ]

        # Add the join column explicitly
        joined_column = f"{wrap_identifier(table1)}.{wrap_identifier(col1)} AS {wrap_identifier(clean_identifier(table1) + '_' + clean_identifier(col1))}"

        # Clear previous columns to avoid ambiguity and add the fully qualified columns
        select_columns.clear()
        select_columns.update([joined_column] + table1_columns_with_alias + related_table_columns_with_alias)
        # print(joined_column)
        # print(table1_columns_with_alias)
        # print(related_table_columns_with_alias)
        # print()

        # Construct the JOIN clause
        query = f"{query} JOIN {wrap_identifier(related_table)} ON {wrap_identifier(table1)}.{wrap_identifier(col1)} = {wrap_identifier(related_table)}.{wrap_identifier(col2)}"
        description += f" joining {clean_identifier(table1)} and {clean_identifier(related_table)} on {clean_identifier(col1)}"

        return query, description, desc_column

    # If no valid join is possible, return the original query
    return query, description, desc_column


def construct_dynamic_query(connection, table_name, columns, related_tables, max_rows_threshold=20):
    select_columns = set(random.sample(
        columns['numeric'] + columns['categorical'],
        min(3, len(columns['numeric'] + columns['categorical']))
    ))
    query = f"FROM {wrap_identifier(table_name)}"
    start = "Get "
    desc_column = ", ".join([clean_identifier(col) for col in select_columns])
    join = False
    sentence = ''

    if random.choice([True, False]):
        join = True
        query, sentence, desc_column = add_join_clause(connection, query, table_name, related_tables, select_columns, sentence, desc_column)

    if random.choice([True, False]):
        query, sentence = add_where_clause(query, connection, table_name, columns, sentence)

    if random.choice([True, False]) and not join:
        query, sentence, desc_column = add_group_by_clause(query, connection, table_name, columns, select_columns, sentence, desc_column)

    if random.choice([True, False]):
        query, sentence = add_order_by_clause(query, table_name, columns, select_columns, sentence)

    if join:
        desc_column = 'records'

    select_columns_list = ', '.join(select_columns)
    query = f"SELECT {select_columns_list} {query}"

    if not join:
        description = f"{start}{desc_column}{sentence} from the {clean_identifier(table_name)} table"
    else:
        description = f"{start}{desc_column}{sentence}"

    # Execute the query to validate and check row count
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        row_count = len(results)

        # Check for aggregate functions
        has_aggregate = any(agg in query.lower() for agg in ['min(', 'max(', 'avg(', 'sum(', 'count(', ])

        # If too many rows and no aggregate functions, add LIMIT and OFFSET
        if row_count > max_rows_threshold:
            limit = random.randint(1, 20)
            offset = random.randint(0, max(0, min(row_count - limit, 20)))
            query += f" LIMIT {limit}"
            description += f" limiting results to {limit}"
            if not has_aggregate:
                query += f" OFFSET {offset}"
                description += f" with offset {offset}"

    except Exception as e:
        print(f"Error executing query: {query}")
        print(f"Error: {e}")

    return description.strip().capitalize()+'.', query.rstrip(";") + ";"

def generate_sample_queries(connection):
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    related_tables = find_related_tables_with_common_columns(connection, tables)
    row_counts = get_table_row_counts(connection)  # Fetch row counts for all tables

    queries = set()
    for _ in range(10):  # Try generating 3 queries per table
        # Select a table with weighted probability based on row counts
        table_name = weighted_table_selection(row_counts, tables)
        # print(table_name)
        columns = extract_columns_by_type(connection, table_name)

        description, query = construct_dynamic_query(connection, table_name, columns, related_tables)
        if description and query:
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                if results:  # Only add if results are returned
                    queries.add((description.strip(), query))
            except Exception as e:
                print(f"Error executing query: {query}")
                print(f"Error: {e}")

    unique_queries = list(queries)
    random.shuffle(unique_queries)

    # Return up to 5 unique queries

    # Keywords to ensure inclusion
    keywords = ["group by", "where", "order by", "join"]
    selected_queries = []

    # Add one query for each keyword if available
    for keyword in keywords:
        for description, query in unique_queries:
            if keyword in query.lower():
                selected_queries.append((description, query))
                unique_queries.remove((description, query))  # Avoid duplicate inclusion
                break

    # Fill up to 5 queries with remaining unique queries
    while len(selected_queries) < 5 and unique_queries:
        selected_queries.append(unique_queries.pop(0))

    return selected_queries


def find_related_tables_with_common_columns(connection, tables):
    related_tables = {}
    cursor = connection.cursor()

    for table1 in tables:
        # print(table1)
        related_tables[table1] = {}
        cursor.execute(f"DESCRIBE {wrap_identifier(table1)};")
        columns1 = {col[0]: col[1] for col in cursor.fetchall()}

        for table2 in tables:
            if table1 == table2:
                continue

            cursor.execute(f"DESCRIBE {wrap_identifier(table2)};")
            columns2 = {col[0]: col[1] for col in cursor.fetchall()}

            common_columns = [
                (col1, col2)
                for col1, type1 in columns1.items()
                for col2, type2 in columns2.items()
                if col1 == col2 and type1.split("(")[0] == type2.split("(")[0]
            ]

            if common_columns:
                related_tables[table1][table2] = common_columns

    return related_tables


def construct_dynamic_query_with_keyword(connection, table_name, columns, related_tables, keyword, max_attempts=10, max_rows_threshold=20):
    """
    Construct a dynamic SQL query ensuring the specified keyword is included, following correct SQL order.
    Retries if the keyword is not included in the query.

    Args:
        connection: Database connection object.
        table_name (str): Name of the main table.
        columns (dict): Categorized columns of the main table.
        related_tables (dict): Mapping of related tables and common columns.
        keyword (str): The SQL keyword to enforce in the query.
        max_attempts (int): Maximum attempts to ensure the keyword is included.

    Returns:
        tuple or None: Description of the query and the query itself, or None if unsuccessful.
    """
    attempt = 0
    while attempt < max_attempts:
        select_columns = set(random.sample(
            columns['numeric'] + columns['categorical'],
            min(3, len(columns['numeric'] + columns['categorical']))
        ))
        query = f"FROM {wrap_identifier(table_name)}"
        start = "Get "
        desc_column = ", ".join([clean_identifier(col) for col in select_columns])
        sentence = ''
        join = False

        # Add JOIN clause if applicable or if random choice allows
        if keyword.lower() == 'join' or (random.choice([True, False]) and keyword.lower() not in ['group by', 'groupby'] ):
            query, sentence, desc_column = add_join_clause(connection, query, table_name, related_tables, select_columns, sentence, desc_column)
            join = True

        # Add WHERE clause if applicable or if random choice allows
        if keyword.lower() == 'where' or random.choice([True, False]):
            query, sentence = add_where_clause(query, connection, table_name, columns, sentence)

        # Add GROUP BY clause if applicable or if random choice allows
        if (keyword.lower() in ['group by', 'groupby'] or random.choice([True, False])) and not join:
            query, sentence, desc_column = add_group_by_clause(query, connection, table_name, columns, select_columns, sentence, desc_column)

        # Add ORDER BY clause if applicable or if random choice allows
        if keyword.lower() in ['order by', 'orderby'] or random.choice([True, False]):
            query, sentence = add_order_by_clause(query, table_name, columns, select_columns, sentence)

        # Adjust SELECT columns if a join is present
        if join:
            desc_column = 'records'

        # Construct SELECT statement
        select_columns_list = ', '.join(select_columns)
        query = f"SELECT {select_columns_list} {query}"

        if not join:
            description = f"{start}{desc_column}{sentence} from the {clean_identifier(table_name)} table"
        else:
            description = f"{start}{desc_column}{sentence}"

        try:
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            row_count = len(results)

            # Check for aggregate functions
            has_aggregate = any(agg in query.lower() for agg in ['min(', 'max(', 'avg(', 'sum(', 'count(', 'order by'])

            # If too many rows and no aggregate functions, add LIMIT and OFFSET
            if row_count > max_rows_threshold:
                limit = random.randint(1, 20)
                offset = random.randint(0, max(0, min(row_count - limit, 20)))
                query += f" LIMIT {limit}"
                description += f" limiting results to {limit}"
                if not has_aggregate:
                    query += f" OFFSET {offset}"
                    description += f" with offset {offset}"

            # Check if the keyword is included in the query
            if keyword.lower() in query.lower():
                return description.strip().capitalize()+'.', query.rstrip(";") + ";"

        except Exception as e:
            print(f"Error executing query: {query}")
            print(f"Error: {e}")

        attempt += 1

    # Return None if no valid query is generated after max_attempts
    return None


def get_table_row_counts(connection):
    """
    Fetch the row counts for all tables in the database.
    """
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    
    row_counts = {}
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {wrap_identifier(table)};")
            results = cursor.fetchall()
            row_counts[table] = results[0][0]
        except Exception as e:
            print(f"Error fetching row count for table {table}: {e}")
            row_counts[table] = float('inf')  # Assign a very high count if an error occurs

    return row_counts


def weighted_table_selection(row_counts, tables, threshold=1000):
    """
    Select tables with higher probability for those with row counts under the threshold.
    """
    weights = []
    for table in tables:
        row_count = row_counts.get(table, float('inf'))
        if row_count <= threshold:
            weights.append(10)  # Assign a higher weight for tables with fewer rows
        else:
            weights.append(1)  # Assign a lower weight for tables with more rows

    selected_table = random.choices(tables, weights=weights, k=1)[0]
    return selected_table


def generate_sample_queries_with_keyword(connection, keyword, max_attempts=10, threshold=1000):
    """
    Generate random queries that include the specified keyword, maintaining proper SQL keyword order.
    Prioritize tables with fewer rows (<threshold).
    """
    # Fetch all tables and their row counts
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    row_counts = get_table_row_counts(connection)  # Fetch row counts for all tables
    related_tables = find_related_tables_with_common_columns(connection, tables)

    queries = set()
    for _ in range(10):  # Attempt generating multiple queries per table
        # Select a table with weighted probability based on row counts
        table_name = weighted_table_selection(row_counts, tables, threshold)
        # print(table_name)
        columns = extract_columns_by_type(connection, table_name)

        # Construct query ensuring the keyword is included
        resp = construct_dynamic_query_with_keyword(connection, table_name, columns, related_tables, keyword, max_attempts)
        if resp:
            description, query = resp
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                if results:  # Only add if results are returned
                    queries.add((description.strip(), query))
            except Exception as e:
                print(f"Error executing query: {query}")
                print(f"Error: {e}")

    # Shuffle and return unique queries
    unique_queries = list(queries)
    random.shuffle(unique_queries)

    return unique_queries

