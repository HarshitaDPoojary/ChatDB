from mysql_functions import connect_to_mysql, create_database, reset_database, upload_csv_to_mysql, execute_query, process_csv_folder
from query_generation import generate_sample_queries, generate_sample_queries_with_keyword
from query_interpreter import interpret_user_query
import os


def main():
    db_name = "dsci551"
    create_database(db_name)
    connection = connect_to_mysql(db_name=db_name)
    
    while True:
        print("\nMenu:")
        print("1. Reset and Upload CSV Files")
        print("2. Generate Example Queries")
        print("3. Generate Queries with Keyword")
        print("4. Enter a Natural Language Query")
        print("5. Exit")
        
        choice = input("Enter your choice: ")
        if choice == "1":
            directory = input("Enter the directory containing CSV files: ")
            reset_database(connection)
            process_csv_folder(directory, connection)

            # for file in os.listdir(directory):
            #     if file.endswith(".csv"):
            #         print(file)
            #         upload_csv_to_mysql(os.path.join(directory, file), connection)
            print("CSV files uploaded successfully.")
        
        elif choice == "2":
            queries = generate_sample_queries(connection)
            for i, (description, query) in enumerate(queries, start=1):
                print(f"{i}. Description: {description}")
                print(f" Query: {query}\n")
            
            while True:
                print("\nOptions:")
                print("1. Execute a query")
                print("2. View list of queries")
                print("3. Back to main menu")
                
                sub_choice = input("Enter your choice: ")
                if sub_choice == "1":
                    try:
                        query_number = int(input(f"Enter the query number to execute (1-{len(queries)}): "))
                        if 1 <= query_number <= len(queries):
                            _, query_to_execute = queries[query_number - 1]
                            execute_query(connection, query_to_execute)
                        else:
                            print("Invalid query number. Try again.")
                    except ValueError:
                        print("Invalid input. Please enter a valid query number.")
                elif sub_choice == "2":
                    for i, (description, query) in enumerate(queries, start=1):
                        print(f"{i}. Description: {description}")
                        print(f"   Query: {query}\n")
                elif sub_choice == "3":
                    break
                else:
                    print("Invalid choice. Try again.")
        
        elif choice == "3":
            keyword = input("Enter a keyword (group by, where, order by, join): ").strip().lower()
            queries = generate_sample_queries_with_keyword(connection, keyword)
            for i, (description, query) in enumerate(queries, start=1):
                print(f"{i}. Description: {description}")
                print(f"   Query: {query}\n")
            
            while True:
                print("\nOptions:")
                print("1. Execute a query")
                print("2. View list of queries")
                print("3. Back to main menu")
                
                sub_choice = input("Enter your choice: ")
                if sub_choice == "1":
                    try:
                        query_number = int(input(f"Enter the query number to execute (1-{len(queries)}): "))
                        if 1 <= query_number <= len(queries):
                            _, query_to_execute = queries[query_number - 1]
                            execute_query(connection, query_to_execute)
                        else:
                            print("Invalid query number. Try again.")
                    except ValueError:
                        print("Invalid input. Please enter a valid query number.")
                elif sub_choice == "2":
                    for i, (description, query) in enumerate(queries, start=1):
                        print(f"{i}. Description: {description}")
                        print(f"   Query: {query}\n")
                elif sub_choice == "3":
                    break
                else:
                    print("Invalid choice. Try again.")
        
        elif choice == "4":
            user_query = input("Enter your natural language query: ").strip()
            sql_query, success, results_or_error = interpret_user_query(user_query, connection)
            
            if success:
                print("\nGenerated SQL Query:")
                print(sql_query)
                print("\nOptions:")
                print("1. Execute the query")
                print("2. Back to main menu")
                
                while True:
                    sub_choice = input("Enter your choice: ")
                    if sub_choice == "1":
                        execute_query(connection, sql_query)
                        break
                    elif sub_choice == "2":
                        break
                    else:
                        print("Invalid choice. Try again.")
            else:
                print("\nError in generating query:")
                print(results_or_error)
        
        elif choice == "5":
            print("Exiting...")
            break
        
        else:
            print("Invalid choice. Try again.")

if __name__ == "__main__":
    main()