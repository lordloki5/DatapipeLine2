import psycopg2
import pandas as pd
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

# Function to connect to database and generate CSV
def generate_ratio_csv(ratio_choice='n1'):
    # Validate ratio choice
    valid_ratios = ['n1', 'n2', 'n3', 'b1', 'b2', 'b3']
    if ratio_choice not in valid_ratios:
        raise ValueError(f"Invalid ratio choice. Must be one of {valid_ratios}")

    # Determine which table to use based on ratio choice
    table_name = 'n_ratios' if ratio_choice.startswith('n') else 'b_ratios'

    # Establish database connection
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    try:
        # Get all indices from indices table
        cursor.execute("""
            SELECT index_id, index_name 
            FROM indices 
            ORDER BY index_id
        """)
        indices = cursor.fetchall()
        index_mapping = {idx[0]: idx[1] for idx in indices}

        # Query to get monthly data for the chosen ratio
        query = f"""
            SELECT 
                trade_date,
                sectoral_index_id,
                {ratio_choice}
            FROM {table_name}
            WHERE {ratio_choice} IS NOT NULL
            ORDER BY trade_date, sectoral_index_id
        """
        cursor.execute(query)
        data = cursor.fetchall()

        # Process data into a dictionary
        monthly_data = {}
        for trade_date, index_id, ratio_value in data:
            # Format date as YYYY-MM
            date_str = trade_date.strftime('%Y-%m')
            if date_str not in monthly_data:
                monthly_data[date_str] = {}
            monthly_data[date_str][index_id] = ratio_value

        # Create DataFrame
        dates = sorted(monthly_data.keys())
        df_data = {}
        
        # Add date column
        df_data['trade_date'] = dates
        
        # Add columns for each index
        for index_id, index_name in index_mapping.items():
            values = []
            for date in dates:
                value = monthly_data[date].get(index_id, None)
                values.append(value)
            df_data[index_name] = values

        # Create DataFrame
        df = pd.DataFrame(df_data)

        # Save to CSV
        output_file = f'monthly_{ratio_choice}_data.csv'
        df.to_csv(output_file, index=False)
        print(f"CSV file '{output_file}' has been generated successfully.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        # Close database connection
        cursor.close()
        conn.close()

# Example usage
if __name__ == "__main__":
    # Choose which ratio to generate (n1, n2, n3, b1, b2, or b3)
    chosen_ratio = 'n1'  # Change this to generate for different ratio
    generate_ratio_csv(chosen_ratio)