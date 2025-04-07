import psycopg2

# Database connection string (same as in your code)
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def drop_tables():
    try:
        # Establish connection to the database
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                # Drop tables with CASCADE to remove any dependent constraints
                cur.execute("DROP TABLE IF EXISTS monthly_ohlc CASCADE;")
                cur.execute("DROP TABLE IF EXISTS n_ratios CASCADE;")
                cur.execute("DROP TABLE IF EXISTS b_ratios CASCADE;")
                
                # Commit the transaction
                conn.commit()
                print("Tables dropped successfully!")
                
    except psycopg2.Error as e:
        print(f"Error dropping tables: {e}")

if __name__ == "__main__":
    drop_tables()
    