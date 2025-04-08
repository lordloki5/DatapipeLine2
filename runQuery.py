import psycopg2

# Database connection string
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

# Query to execute
query = """
SELECT sectoral_index_id, score_value
FROM top_1_scores
WHERE EXTRACT(MONTH FROM trade_date) = 4
  AND EXTRACT(YEAR FROM trade_date) = 2018
"""

try:
    # Establish connection and cursor
    connection = psycopg2.connect(DEFAULT_DB_URL)
    cursor = connection.cursor()
    print("Connected to database.")

    # Execute the query
    cursor.execute(query)

    # Fetch all rows
    results = cursor.fetchall()

    # Print results
    for row in results:
        print(f"Sectoral Index ID: {row[0]}, Score Value: {row[1]}")

except psycopg2.Error as e:
    print("Database error:", e)

finally:
    # Clean up
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals():
        connection.close()
        print("Connection closed.")
