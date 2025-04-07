import psycopg2

# Database connection string (adjust as needed)
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def drop_top_scores_tables():
    """Drop the top scores tables if they exist."""
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                # Drop tables in reverse order to avoid foreign key constraint issues if any
                cur.execute("DROP TABLE IF EXISTS top_3_scores;")
                cur.execute("DROP TABLE IF EXISTS top_2_scores;")
                cur.execute("DROP TABLE IF EXISTS top_1_scores;")
                
                conn.commit()
                print("Top scores tables dropped successfully!")
    except psycopg2.Error as e:
        print(f"Error dropping tables: {e}")

if __name__ == "__main__":
    drop_top_scores_tables()