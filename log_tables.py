import psycopg2
from datetime import datetime

# Default database URL
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

# Parse the DEFAULT_DB_URL into a dictionary
def parse_db_url(db_url):
    params = {}
    parts = db_url.split()
    for part in parts:
        key, value = part.split('=', 1)
        params[key] = value
    return params

# Database connection parameters
db_params = parse_db_url(DEFAULT_DB_URL)

# Function to create and populate the index date range table
def create_index_date_range_table():
    # Establish database connection
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    try:
        # Create table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS index_date_ranges (
                index_id INTEGER NOT NULL,
                index_name VARCHAR(50) NOT NULL,
                start_date TIMESTAMP WITH TIME ZONE,
                end_date TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (index_id),
                FOREIGN KEY (index_id) REFERENCES indices(index_id)
            );
        """
        cursor.execute(create_table_query)

        # Get all indices from the indices table
        cursor.execute("""
            SELECT index_id, index_name 
            FROM indices 
            ORDER BY index_id
        """)
        indices = cursor.fetchall()

        # For each index, calculate the start and end dates from daily_ohlc
        for index_id, index_name in indices:
            cursor.execute("""
                SELECT 
                    MIN(trade_date) AS start_date,
                    MAX(trade_date) AS end_date
                FROM daily_ohlc
                WHERE index_id = %s
            """, (index_id,))
            result = cursor.fetchone()

            start_date, end_date = result if result else (None, None)

            # Insert or update the record in index_date_ranges
            cursor.execute("""
                INSERT INTO index_date_ranges (index_id, index_name, start_date, end_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (index_id)
                DO UPDATE SET 
                    index_name = EXCLUDED.index_name,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date
            """, (index_id, index_name, start_date, end_date))

        # Commit the transaction
        conn.commit()
        print("Table 'index_date_ranges' created (if not existed) and populated successfully.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        conn.rollback()
    
    finally:
        # Close database connection
        cursor.close()
        conn.close()

# Example usage
if __name__ == "__main__":
    create_index_date_range_table()