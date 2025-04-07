import pandas as pd
import psycopg2
from datetime import datetime
import sys

# Database connection string
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def get_db_connection():
    try:
        return psycopg2.connect(DEFAULT_DB_URL)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def process_excel(excel_path, operation='max'):
    # Validate operation
    if operation not in ['max', 'min']:
        raise ValueError("Operation must be either 'max' or 'min'")
    
    # Read Excel file
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        sys.exit(1)
    
    # Ensure minimum required columns exist
    if len(df.columns) < 2:
        raise ValueError("Excel file must have at least 2 columns: date and stock symbol")
    
    # Add new columns for the ratios
    new_columns = ['n1', 'n2', 'n3', 'b1', 'b2', 'b3']
    for col in new_columns:
        df[col] = None
    
    # Database connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Process each row
        for index, row in df.iterrows():
            # Extract date and symbol
            date_str = row.iloc[0]  # First column (date)
            stock_symbol = row.iloc[1]  # Second column (stock symbol)
            
            # Parse date (explicitly handle dd-mm-yyyy format)
            try:
                # Convert the date string to datetime with the correct format
                date_obj = pd.to_datetime(date_str, format='%d-%m-%Y')
                year = date_obj.year
                month = date_obj.month
                print(f"\nProcessing row {index}: Symbol={stock_symbol}, Date={date_str}, Year={year}, Month={month}")
            except Exception as e:
                print(f"Error parsing date {date_str}: {e}")
                continue
            
            # Get all sector index_ids for the stock symbol
            query_indices = """
                SELECT index_id 
                FROM stock_index_mapping 
                WHERE stock_symbol = %s
            """
            cursor.execute(query_indices, (stock_symbol,))
            sector_index_ids = [row[0] for row in cursor.fetchall()]
            print(f"Stock {stock_symbol} belongs to sectors: {sector_index_ids}")
            
            if not sector_index_ids:
                print(f"No sector mapping found for stock symbol: {stock_symbol}")
                continue
            
            # Prepare lists to store ratios
            n1_values, n2_values, n3_values = [], [], []
            b1_values, b2_values, b3_values = [], [], []
            
            # For each sector the stock belongs to
            for sector_index_id in sector_index_ids:
                # Query n_ratios for this sector
                query_n_ratios = """
                    SELECT trade_date, sectoral_index_id, benchmark_index_id, n1, n2, n3
                    FROM n_ratios
                    WHERE sectoral_index_id = %s
                    AND EXTRACT(YEAR FROM trade_date) = %s
                    AND EXTRACT(MONTH FROM trade_date) = %s
                    LIMIT 1
                """
                cursor.execute(query_n_ratios, (sector_index_id, year, month))
                n_result = cursor.fetchone()
                if n_result:
                    print(f"n_ratios for sector {sector_index_id} (benchmark {n_result[2]}) on {n_result[0]}: n1={n_result[3]}, n2={n_result[4]}, n3={n_result[5]}")
                    if all(x is not None for x in n_result[3:]):
                        n1_values.append(n_result[3])
                        n2_values.append(n_result[4])
                        n3_values.append(n_result[5])
                else:
                    print(f"No n_ratios found for sector {sector_index_id} in {year}-{month}")
                
                # Query b_ratios for this sector
                query_b_ratios = """
                    SELECT trade_date, sectoral_index_id, benchmark_index_id, b1, b2, b3
                    FROM b_ratios
                    WHERE sectoral_index_id = %s
                    AND EXTRACT(YEAR FROM trade_date) = %s
                    AND EXTRACT(MONTH FROM trade_date) = %s
                    LIMIT 1
                """
                cursor.execute(query_b_ratios, (sector_index_id, year, month))
                b_result = cursor.fetchone()
                if b_result:
                    print(f"b_ratios for sector {sector_index_id} (benchmark {b_result[2]}) on {b_result[0]}: b1={b_result[3]}, b2={b_result[4]}, b3={b_result[5]}")
                    if all(x is not None for x in b_result[3:]):
                        b1_values.append(b_result[3])
                        b2_values.append(b_result[4])
                        b3_values.append(b_result[5])
                else:
                    print(f"No b_ratios found for sector {sector_index_id} in {year}-{month}")
            
            # Calculate max or min based on operation
            agg_func = max if operation == 'max' else min
            print(f"\nApplying {operation}:")
            print(f"n1_values={n1_values}, n2_values={n2_values}, n3_values={n3_values}")
            print(f"b1_values={b1_values}, b2_values={b2_values}, b3_values={b3_values}")
            
            df.at[index, 'n1'] = agg_func(n1_values) if n1_values else None
            df.at[index, 'n2'] = agg_func(n2_values) if n2_values else None
            df.at[index, 'n3'] = agg_func(n3_values) if n3_values else None
            df.at[index, 'b1'] = agg_func(b1_values) if b1_values else None
            df.at[index, 'b2'] = agg_func(b2_values) if b2_values else None
            df.at[index, 'b3'] = agg_func(b3_values) if b3_values else None
            
            print(f"\nFinal values for row {index}:")
            print(f"n1={df.at[index, 'n1']}, n2={df.at[index, 'n2']}, n3={df.at[index, 'n3']}")
            print(f"b1={df.at[index, 'b1']}, b2={df.at[index, 'b2']}, b3={df.at[index, 'b3']}")
        
        # Save the updated DataFrame
        output_path = excel_path.replace('.xlsx', '_processed.xlsx')
        df.to_excel(output_path, index=False)
        print(f"\nProcessed Excel file saved to: {output_path}")
        
    except Exception as e:
        print(f"Error processing data: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Example usage
    if len(sys.argv) < 2:
        print("Usage: python script.py <excel_path> [max|min]")
        sys.exit(1)
    
    excel_path = sys.argv[1]
    operation = sys.argv[2] if len(sys.argv) > 2 else 'max'
    process_excel(excel_path, operation)