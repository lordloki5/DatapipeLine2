import psycopg2
from psycopg2 import Error

def connect_to_db():
    """Establish connection to the database"""
    try:
        # Using the DEFAULT_DB_URL format
        connection = psycopg2.connect(
            dbname="ohcldata",
            host="localhost",
            port="5432",
            user="dhruvbhandari",
            password=""
        )
        return connection
    except Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None

def get_table_columns(connection, table_name):
    """Get column names for the specified table"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return columns
    except Error as e:
        print(f"Error getting column names: {e}")
        return None

def print_table_records(table_name):
    """Print all records from the specified table"""
    # Establish connection
    connection = connect_to_db()
    if not connection:
        return

    try:
        # Create a cursor object
        cursor = connection.cursor()
        
        # Get column names
        columns = get_table_columns(connection, table_name)
        if not columns:
            print(f"Could not retrieve columns for table {table_name}")
            return

        # Execute query to get all records
        cursor.execute(f"SELECT * FROM {table_name}")
        records = cursor.fetchall()

        # Print header
        print("\n" + "="*50)
        print(f"Records from table: {table_name}")
        print("="*50)
        print(" | ".join(columns))
        print("-"*50)

        # Print records
        if records:
            for record in records:
                # Convert each value to string and handle None values
                formatted_row = [str(value) if value is not None else "NULL" for value in record]
                print(" | ".join(formatted_row))
        else:
            print("No records found in the table")

        print("="*50)

    except Error as e:
        print(f"Error retrieving records: {e}")
    
    finally:
        # Close cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if connection:
            connection.close()

def main():
    # List of valid tables based on your database structure
    valid_tables = [
        'b_ratios', 'daily_ohlc', 'index_date_ranges', 'indices',
        'monthly_ohlc', 'n_ratios', 'stock_index_mapping',
        'top_1_scores', 'top_2_scores', 'top_3_scores','bottom_1_scores','bottom_2_scores','bottom_3_scores'
    ]
    
    # Get table name from user
    print("Available tables:", ", ".join(valid_tables))
    table_name = input("Enter the table name to print records from: ").lower().strip()
    
    # Validate table name
    if table_name not in valid_tables:
        print(f"Invalid table name. Please choose from: {', '.join(valid_tables)}")
        return
    
    # Print records
    print_table_records(table_name)

if __name__ == "__main__":
    main()