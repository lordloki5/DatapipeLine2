import psycopg2
import csv

# Database connection parameters
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

bse_dict = {
    'BSE AUTO': 'AUTO',
    'BSE BANKEX': 'BANKEX',
    'BSE CONSUMER DURABLES': 'BSE CD',
    'BSE CAPITAL GOODS': 'BSE CG',
    'BSE Commodities': 'COMDTY',
    'BSE CONSUMER DISCRETIONARY': 'CDGS',
    'BSE ENERGY': 'ENERGY',
    'BSE FINANCIAL SERVICES': 'FIN',
    'BSE INDUSTRIALS': 'INDSTR',
    'BSE Telecommunication': 'TELCOM',
    'BSE Utilities': 'UTILS',
    'BSE FAST MOVING CONSUMER GOODS': 'BSEFMC',
    'BSE HEALTHCARE': 'BSE HC',
    'BSE INFORMATION TECHNOLOGY': 'BSE IT',
    'BSE METAL': 'METAL',
    'BSE OIL & GAS': 'OILGAS',
    'BSE POWER': 'POWER',
    'BSE REALTY': 'REALTY',
    'BSE TECK': 'TECK',
    'BSE Services': 'BSESER'
}

nse_dict = {
    "NIFTY AUTO": "NIFTYAUTO",
    "NIFTY BANK": "NIFTYBANK",
    "NIFTY CHEMICALS": "NIFTYCHEM",
    "NIFTY FINANCIAL SERVICES": "NIFTYFIN",
    "NIFTY FMCG": "NIFTYFMCG",
    "NIFTY HEALTHCARE INDEX": "NIFTYHEALTHCARE",
    "NIFTY IT": "NIFTYIT",
    "NIFTY MEDIA": "NIFTYMEDIA",
    "NIFTY METAL": "NIFTYMETAL",
    "NIFTY PHARMA": "NIFTYPHARMA",
    "NIFTY PRIVATE BANK": "NIFTYPVTBANK",
    "NIFTY PSU BANK": "NIFTYPSUBANK",
    "NIFTY REALTY": "NIFTYREALTY",
    "NIFTY CONSUMER DURABLES": "NIFTYCONSUMDUR",
    "NIFTY OIL & GAS": "NIFTYOILGAS"
}

# Merge mappings
index_code_to_name = {v: k for k, v in {**bse_dict, **nse_dict}.items()}

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

def get_index_id(index_name, conn):
    """Helper function to get index_id from index_name"""
    try:
        cur = conn.cursor()
        query = "SELECT index_id FROM indices WHERE index_name ILIKE %s"
        cur.execute(query, (f"%{index_name}%",))
        result = cur.fetchone()
        cur.close()
        return result[0] if result else None
    except psycopg2.Error as e:
        print(f"Error fetching index_id: {e}")
        return None

def import_csv_to_mapping(csv_file_path):
    conn = create_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        with open(csv_file_path, 'r') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)
            print(f"Header: {header}")
            
            for row in csv_reader:
                if len(row) < 2:
                    print(f"Skipping invalid row: {row}")
                    continue
                
                stock_symbol = row[0].strip()
                index_codes = [code.strip() for code in row[1].split(',') if code.strip()]
                mapped_index_codes = []
                
                for index_code in index_codes:
                    index_name = index_code_to_name.get(index_code)
                    if not index_name:
                        print(f"Index code not found in mapping: {index_code}")
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
                            mapped_index_codes.append(index_code)
                        except psycopg2.Error as e:
                            print(f"Error inserting {stock_symbol} - {index_name}: {e}")
                    else:
                        print(f"Index not found: {index_name} for stock {stock_symbol}")
                
                print(f"Final mapped codes for {stock_symbol}: {', '.join(mapped_index_codes)}")
        
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