import pandas as pd
import psycopg2
from datetime import datetime

# Database connection string
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

# Function to select table and score type (hardcoded for debugging)
def select_table_and_score():
    selected_table = 'top_1_scores'
    selected_score = 'n1'  # Hardcode to 'n1' to match your example
    return selected_table, selected_score

# Function to get indices for a stock symbol
def get_stock_indices(conn, stock_symbol):
    query = """
    SELECT index_id 
    FROM stock_index_mapping 
    WHERE stock_symbol = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (stock_symbol,))
            results = cur.fetchall()
            indices = [row[0] for row in results]
            print(f"Indices for stock_symbol '{stock_symbol}': {indices}")
            return indices
    except Exception as e:
        print(f"Error fetching indices for {stock_symbol}: {e}")
        return []

# Function to check if any index exists in selected table for given month and year
# Also fetch the matching rows for debugging
def check_index_in_table(conn, table_name, score_type, indices, year, month):
    # First, check if a matching row exists
    exists_query = f"""
    SELECT EXISTS (
        SELECT 1 
        FROM {table_name} 
        WHERE sectoral_index_id = ANY(%s)
        AND EXTRACT(YEAR FROM trade_date) = %s
        AND EXTRACT(MONTH FROM trade_date) = %s
        AND score_type = %s
    )
    """
    # Then, fetch the matching rows for debugging
    debug_query = f"""
    SELECT trade_date, ratio_type, sectoral_index_id, score_type, score_value
    FROM {table_name}
    WHERE sectoral_index_id = ANY(%s)
    AND EXTRACT(YEAR FROM trade_date) = %s
    AND EXTRACT(MONTH FROM trade_date) = %s
    AND score_type = %s
    """
    try:
        with conn.cursor() as cur:
            # Check existence
            cur.execute(exists_query, (indices, year, month, score_type))
            result = cur.fetchone()[0]
            print(f"Checking {table_name} for indices {indices}, year {year}, month {month}, score_type '{score_type}': EXISTS = {result}")
            
            # Fetch matching rows for debugging
            if result:
                cur.execute(debug_query, (indices, year, month, score_type))
                matching_rows = cur.fetchall()
                print(f"Matching rows in {table_name}:")
                for row in matching_rows:
                    print(f"  {row}")
            else:
                print(f"No matching rows found in {table_name}")
            
            return result
    except Exception as e:
        print(f"Error checking table {table_name}: {e}")
        return False

# Main processing function
def process_excel_file(file_path):
    # Establish database connection
    conn = psycopg2.connect(DEFAULT_DB_URL)
    
    # Select table and score type
    selected_table, selected_score = select_table_and_score()
    print(f"Selected table: {selected_table}, Score type: {selected_score}")
    
    # Read Excel file with date parsing
    df = pd.read_excel(file_path)
    
    # Convert first column to datetime with specific format
    df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], format='%d-%m-%Y')
    
    # Lists to keep track of rows to keep
    rows_to_keep = []
    
    # Process each row
    for index, row in df.iterrows():
        # Extract month and year from date
        date = row[0]
        year = date.year
        month = date.month
        stock_symbol = row[1]
        print(f"\nProcessing row: date={date}, symbol={stock_symbol}")
        
        # Get all indices for this stock
        indices = get_stock_indices(conn, stock_symbol)
        
        if not indices:
            print(f"No indices found for {stock_symbol}, skipping row")
            continue  # Skip if no indices found
            
        # Check if any index exists in selected table
        exists = check_index_in_table(conn, selected_table, selected_score, 
                                    indices, year, month)
        
        if exists:
            print(f"Keeping row for {stock_symbol} (found matching entry in {selected_table})")
            rows_to_keep.append(index)
        else:
            print(f"Deleting row for {stock_symbol} (no matching entry in {selected_table})")
    
    # Filter dataframe to keep only valid rows
    filtered_df = df.loc[rows_to_keep]
    
    # Close connection
    conn.close()
    
    return filtered_df

# Example usage
if __name__ == "__main__":
    file_path = "addColumns.xlsx"  # Replace with your file path
    try:
        result_df = process_excel_file(file_path)
        print(f"\nOriginal rows: {len(pd.read_excel(file_path))}")
        print(f"Filtered rows: {len(result_df)}")
        print("\nFiltered DataFrame:")
        print(result_df)
        
        # Optionally save the filtered result
        result_df.to_excel("filtered_output.xlsx", index=False)
    except Exception as e:
        print(f"Error processing file: {e}")