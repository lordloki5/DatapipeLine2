import psycopg2
import csv

# Database connection parameters
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def create_connection():
    try:
        conn = psycopg2.connect(DEFAULT_DB_URL)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def create_stock_index_mapping_table():
    conn = create_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        drop_table_query = "DROP TABLE IF EXISTS stock_index_mapping CASCADE;"
        cur.execute(drop_table_query)
        # Create table for stock-index mapping if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_index_mapping (
            mapping_id SERIAL PRIMARY KEY,
            stock_symbol VARCHAR(20) NOT NULL,
            index_id INTEGER NOT NULL,
            FOREIGN KEY (index_id) REFERENCES indices(index_id),
            UNIQUE (stock_symbol, index_id)
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Table stock_index_mapping created successfully or already exists")
        
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_index_id2(index_name, conn):
    """Helper function to get index_id from index_name"""
    try:
        cur = conn.cursor()
        cur.execute("SELECT index_id FROM indices WHERE index_name = %s", (index_name,))
        result = cur.fetchone()
        cur.close()
        return result[0] if result else None
    except psycopg2.Error as e:
        print(f"Error fetching index_id: {e}")
        return None
    
def get_index_id(index_name, conn):
    """Helper function to get index_id where index_name is a substring of names in indices table"""
    try:
        cur = conn.cursor()
        # Using ILIKE with wildcards to match index_name as a substring
        query = "SELECT index_id FROM indices WHERE index_name ILIKE %s"
        param = f"%{index_name}%"
        
        cur.execute(query, (param,))
        result = cur.fetchone()
        cur.close()
        return result[0] if result else None
    except psycopg2.Error as e:
        print(f"Error fetching index_id: {e}")
        return None

def import_csv_to_mapping2(csv_file_path):
    conn = create_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        with open(csv_file_path, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header row if it exists
            
            for row in csv_reader:
                if len(row) < 2:
                    continue
                    
                stock_symbol = row[0].strip()
                index_names = [name.strip() for name in row[1].split(',')]
                
                # Insert mapping for each index
                for index_name in index_names:
                    index_id = get_index_id(index_name, conn)
                    if index_id:
                        try:
                            cur.execute("""
                                INSERT INTO stock_index_mapping (stock_symbol, index_id)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING
                            """, (stock_symbol, index_id))
                        except psycopg2.Error as e:
                            print(f"Error inserting {stock_symbol} - {index_name}: {e}")
                    else:
                        print(f"Index not found: {index_name} for stock {stock_symbol}")
        
        conn.commit()
        print("CSV data imported successfully")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
    except FileNotFoundError:
        print(f"CSV file not found: {csv_file_path}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        cur.close()
        conn.close()

def import_csv_to_mapping(csv_file_path):
    conn = create_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        with open(csv_file_path, 'r') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)  # Skip header row
            print(f"Header: {header}")
            
            for row in csv_reader:
                if len(row) < 2:  # Need at least stock symbol and indices
                    print(f"Skipping invalid row: {row}")
                    continue
                    
                stock_symbol = row[0].strip()
                # Split the indices string (row[1]) by comma and strip whitespace
                index_names = [name.strip() for name in row[1].split(',') if name.strip()]
                print(f"Processing {stock_symbol} with indices: {index_names}")
                
                # Insert mapping for each index
                for index_name in index_names:
                    if not index_name:
                        print(f"Empty index name skipped for {stock_symbol}")
                        continue
                        
                    index_id = get_index_id(index_name, conn)
                    if index_id:
                        try:
                            cur.execute("""
                                INSERT INTO stock_index_mapping (stock_symbol, index_id)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING
                            """, (stock_symbol, index_id))
                            print(f"Inserted {stock_symbol} - {index_name} (ID: {index_id})")
                        except psycopg2.Error as e:
                            print(f"Error inserting {stock_symbol} - {index_name}: {e}")
                    else:
                        print(f"Index not found: {index_name} for stock {stock_symbol}")
        
        conn.commit()
        print("CSV data imported successfully")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
    except FileNotFoundError:
        print(f"CSV file not found: {csv_file_path}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        cur.close()
        conn.close()
# Example usage
if __name__ == "__main__":
    create_stock_index_mapping_table()
    csv_path = "Universe-with_sector_indices.csv"
    import_csv_to_mapping(csv_path)