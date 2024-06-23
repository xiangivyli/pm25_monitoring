import duckdb
import pandas as pd

# Path to your DuckDB database file
duck_db_instance_path = "pm25_ducks.db"

def list_tables(db_path):
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(db_path)
        
        # Query to list all tables in the DuckDB database
        query = "SHOW TABLES"
        
        # Execute the query
        result = conn.execute(query).fetchall()
        
        # Close the connection
        conn.close()

        # Print the number of tables and their names
        if result:
            print(f"There are {len(result)} tables in the database:")
            for table in result:
                print(f"- {table[0]}")
        else:
            print("No tables found in the database.")
    except Exception as e:
        print(f"Error: {e}")

# Call the function to list tables
list_tables(duck_db_instance_path)

def part_data_read(db=duck_db_instance_path, table_name="danger_time_list"):
    try:
        conn = duckdb.connect(db)
        cursor = conn.cursor()

        # Query to retrieve data from daily table
        query = f"""SELECT *
            FROM {table_name}
            LIMIT 5;"""

        # Execute the query
        part_data = cursor.execute(query).fetchdf()

        conn.close()

        return part_data

    except Exception as e:
        return None

data = part_data_read()

print(data)

