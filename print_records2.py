import psycopg2
from datetime import date

# Database connection string (adjust as needed)
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def display_table_records(table_name: str):
    """Fetch and display all records from the specified table."""
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                # Define the query based on table structure
                if table_name == 'n_ratios':
                    sql = """
                        SELECT trade_date, sectoral_index_id, benchmark_index_id, 
                               open_ratio, high_ratio, low_ratio, close_ratio, 
                               tag, n1, n2, n3 
                        FROM n_ratios 
                        ORDER BY trade_date, sectoral_index_id
                    """
                elif table_name == 'b_ratios':
                    sql = """
                        SELECT trade_date, sectoral_index_id, benchmark_index_id, 
                               open_ratio, high_ratio, low_ratio, close_ratio, 
                               tag, b1, b2, b3 
                        FROM b_ratios 
                        ORDER BY trade_date, sectoral_index_id
                    """
                elif table_name in ('top_1_scores', 'top_2_scores', 'top_3_scores'):
                    sql = f"""
                        SELECT trade_date, ratio_type, sectoral_index_id, 
                               score_type, score_value 
                        FROM {table_name} 
                        ORDER BY trade_date, ratio_type, score_type, sectoral_index_id
                    """
                else:
                    raise ValueError(f"Unknown table: {table_name}")

                # Execute the query
                cur.execute(sql)
                rows = cur.fetchall()

                # Print table header
                print(f"\n{'='*50}")
                print(f"Records from {table_name}:")
                print(f"{'='*50}")
                if not rows:
                    print("No records found.")
                    return

                # Print column headers
                if table_name == 'n_ratios':
                    print("Trade Date | Sectoral ID | Benchmark ID | Open Ratio | High Ratio | Low Ratio | Close Ratio | Tag | n1 | n2 | n3")
                    print("-"*130)
                elif table_name == 'b_ratios':
                    print("Trade Date | Sectoral ID | Benchmark ID | Open Ratio | High Ratio | Low Ratio | Close Ratio | Tag | b1 | b2 | b3")
                    print("-"*130)
                elif table_name in ('top_1_scores', 'top_2_scores', 'top_3_scores'):
                    print("Trade Date | Ratio Type | Sectoral ID | Score Type | Score Value")
                    print("-"*70)

                # Print each row
                for row in rows:
                    if table_name == 'n_ratios':
                        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]:.4f} | {row[4]:.4f} | {row[5]:.4f} | {row[6]:.4f} | {row[7]} | {row[8]} | {row[9]} | {row[10]}")
                    elif table_name == 'b_ratios':
                        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]:.4f} | {row[4]:.4f} | {row[5]:.4f} | {row[6]:.4f} | {row[7]} | {row[8]} | {row[9]} | {row[10]}")
                    elif table_name in ('top_1_scores', 'top_2_scores', 'top_3_scores'):
                        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")

    except psycopg2.Error as e:
        print(f"Error fetching records from {table_name}: {e}")

def main():
    """Display records from all relevant tables."""
    tables = ['top_2_scores', 'top_1_scores', 'top_3_scores','n_ratios', 'b_ratios']
    for table in tables:
        display_table_records(table)

if __name__ == "__main__":
    main()