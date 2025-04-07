import psycopg2
from datetime import date

# Database connection string
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def print_monthly_ohlc(limit: int = None):
    """Print all records from monthly_ohlc table"""
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT trade_date, index_id, open_price, high_price, low_price, close_price
                    FROM monthly_ohlc
                    ORDER BY trade_date, index_id
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)
                
                cur.execute(query, params)
                
                print("\n=== Monthly OHLC Records ===")
                print("Trade Date | Index ID | Open          | High          | Low           | Close")
                print("-" * 85)
                
                for row in cur.fetchall():
                    trade_date, index_id, open_price, high_price, low_price, close_price = row
                    print(f"{trade_date.date()} | {index_id:8d} | "
                          f"{open_price:13.8f} | {high_price:13.8f} | "
                          f"{low_price:13.8f} | {close_price:13.8f}")
                
                print(f"\nTotal records: {cur.rowcount}")
                
    except psycopg2.Error as e:
        print(f"Database error: {e}")

def print_n_ratios(limit: int = None):
    """Print all records from n_ratios table"""
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT trade_date, sectoral_index_id, benchmark_index_id,
                           open_ratio, high_ratio, low_ratio, close_ratio,
                           tag, n1, n2, n3
                    FROM n_ratios
                    ORDER BY trade_date
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)
                
                cur.execute(query, params)
                
                print("\n=== N Ratios Records ===")
                print("Trade Date | Sectoral ID | Benchmark ID | Open Ratio | High Ratio | "
                      "Low Ratio | Close Ratio | Tag           | n1   | n2   | n3")
                print("-" * 115)
                
                for row in cur.fetchall():
                    trade_date, sectoral_id, benchmark_id, open_r, high_r, low_r, close_r, \
                    tag, n1, n2, n3 = row
                    n1_str = str(n1) if n1 is not None else 'null'
                    n2_str = str(n2) if n2 is not None else 'null'
                    n3_str = str(n3) if n3 is not None else 'null'
                    
                    print(f"{trade_date.date()} | {sectoral_id:11d} | {benchmark_id:12d} | "
                          f"{open_r:10.8f} | {high_r:10.8f} | {low_r:10.8f} | "
                          f"{close_r:11.8f} | {tag:13s} | "
                          f"{n1_str:>4s} | {n2_str:>4s} | {n3_str:>4s}")
                
                print(f"\nTotal records: {cur.rowcount}")
                
    except psycopg2.Error as e:
        print(f"Database error: {e}")

def print_b_ratios(limit: int = None):
    """Print all records from b_ratios table"""
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT trade_date, sectoral_index_id, benchmark_index_id,
                           open_ratio, high_ratio, low_ratio, close_ratio,
                           tag, b1, b2, b3
                    FROM b_ratios
                    ORDER BY trade_date
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)
                
                cur.execute(query, params)
                
                print("\n=== B Ratios Records ===")
                print("Trade Date | Sectoral ID | Benchmark ID | Open Ratio | High Ratio | "
                      "Low Ratio | Close Ratio | Tag           | b1   | b2   | b3")
                print("-" * 115)
                
                for row in cur.fetchall():
                    trade_date, sectoral_id, benchmark_id, open_r, high_r, low_r, close_r, \
                    tag, b1, b2, b3 = row
                    b1_str = str(b1) if b1 is not None else 'null'
                    b2_str = str(b2) if b2 is not None else 'null'
                    b3_str = str(b3) if b3 is not None else 'null'
                    
                    print(f"{trade_date.date()} | {sectoral_id:11d} | {benchmark_id:12d} | "
                          f"{open_r:10.8f} | {high_r:10.8f} | {low_r:10.8f} | "
                          f"{close_r:11.8f} | {tag:13s} | "
                          f"{b1_str:>4s} | {b2_str:>4s} | {b3_str:>4s}")
                
                print(f"\nTotal records: {cur.rowcount}")
                
    except psycopg2.Error as e:
        print(f"Database error: {e}")

def main():
    # Print all records from all tables
    print_monthly_ohlc()
    # print_n_ratios()
    # print_b_ratios()
    
    # Optional: Print with a limit to verify formatting (uncomment if desired)
    # print("\n=== Limited Records Example ===")
    # print_monthly_ohlc(limit=5)
    # print_n_ratios(limit=5)
    # print_b_ratios(limit=5)

if __name__ == "__main__":
    main()