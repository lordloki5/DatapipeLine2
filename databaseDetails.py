# source ~/my_python_env/bin/activate
import psycopg2
from psycopg2 import Error

# Database connection parameters
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def get_table_info():
    try:
        # Establish connection
        connection = psycopg2.connect(DEFAULT_DB_URL)
        cursor = connection.cursor()
        
        # Query to get all table names from the current schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        
        print("Database Tables and Their Structure:")
        print("-" * 50)
        
        # For each table, get its structure
        for table in tables:
            table_name = table[0]
            print(f"\nTable: {table_name}")
            print("-" * 30)
            
            # Query to get column details
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))
            
            columns = cursor.fetchall()
            
            # Print column details
            print("Column Name | Data Type | Nullable | Default | Max Length")
            print("-" * 60)
            for column in columns:
                col_name, data_type, nullable, default, max_length = column
                print(f"{col_name:<12} | {data_type:<15} | {nullable:<8} | {str(default):<15} | {str(max_length):<10}")
            
            # Get constraints (primary keys, foreign keys, etc.)
            cursor.execute("""
                SELECT 
                    tc.constraint_type,
                    tc.constraint_name,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = %s
                AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE');
            """, (table_name,))
            
            constraints = cursor.fetchall()
            if constraints:
                print("\nConstraints:")
                print("-" * 30)
                for constraint in constraints:
                    cons_type, cons_name, col_name = constraint
                    print(f"{cons_type:<12} | {cons_name:<20} | Column: {col_name}")

    except Error as e:
        print(f"Error connecting to database: {e}")
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    get_table_info()