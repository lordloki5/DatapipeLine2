import psycopg2
from datetime import datetime
from typing import Dict, List, Optional, Tuple

def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        dbname="ohcldata",
        host="localhost",
        port="5432",
        user="dhruvbhandari",
        password=""
    )

def get_score_for_index_subtype_date(index_id: int, sub_type: str, date: datetime) -> Optional[int]:
    """
    Get score for a specific index_id, sub_type, and date (month/year).
    sub_type can be: 'n1', 'n2', 'n3', 'b1', 'b2', 'b3'
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Determine which table to query based on sub_type
            if sub_type.startswith('n'):
                table_name = 'n_ratios'
                column_name = sub_type
            elif sub_type.startswith('b'):
                table_name = 'b_ratios'
                column_name = sub_type
            else:
                return None

            query = f"""
                SELECT {column_name}
                FROM {table_name}
                WHERE sectoral_index_id = %s
                AND EXTRACT(YEAR FROM trade_date) = %s
                AND EXTRACT(MONTH FROM trade_date) = %s
                LIMIT 1
            """
            
            cur.execute(query, (index_id, date.year, date.month))
            result = cur.fetchone()
            return result[0] if result else None
            
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        conn.close()

def get_all_scores_for_index_date(index_id: int, date: datetime) -> Dict[str, Optional[int]]:
    """
    Get all scores (n1,n2,n3,b1,b2,b3) for a specific index_id and date as a dictionary.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Query n_ratios
            n_query = """
                SELECT n1, n2, n3
                FROM n_ratios
                WHERE sectoral_index_id = %s
                AND EXTRACT(YEAR FROM trade_date) = %s
                AND EXTRACT(MONTH FROM trade_date) = %s
                LIMIT 1
            """
            
            # Query b_ratios
            b_query = """
                SELECT b1, b2, b3
                FROM b_ratios
                WHERE sectoral_index_id = %s
                AND EXTRACT(YEAR FROM trade_date) = %s
                AND EXTRACT(MONTH FROM trade_date) = %s
                LIMIT 1
            """
            
            cur.execute(n_query, (index_id, date.year, date.month))
            n_result = cur.fetchone()
            
            cur.execute(b_query, (index_id, date.year, date.month))
            b_result = cur.fetchone()
            
            scores = {}
            if n_result:
                scores.update({
                    'n1': n_result[0],
                    'n2': n_result[1],
                    'n3': n_result[2]
                })
            if b_result:
                scores.update({
                    'b1': b_result[0],
                    'b2': b_result[1],
                    'b3': b_result[2]
                })
                
            return scores
            
    except Exception as e:
        print(f"Error: {e}")
        return {}
    finally:
        conn.close()

def get_scores_for_subtype_date(sub_type: str, date: datetime) -> List[Tuple[int, str, Optional[int]]]:
    """
    Get scores for all indices for a specific sub_type and date, including index names.
    Returns list of tuples: (index_id, index_name, score)
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if sub_type.startswith('n'):
                table_name = 'n_ratios'
                column_name = sub_type
            elif sub_type.startswith('b'):
                table_name = 'b_ratios'
                column_name = sub_type
            else:
                return []

            query = f"""
                SELECT 
                    r.sectoral_index_id,
                    i.index_name,
                    r.{column_name}
                FROM {table_name} r
                JOIN indices i ON r.sectoral_index_id = i.index_id
                WHERE EXTRACT(YEAR FROM r.trade_date) = %s
                AND EXTRACT(MONTH FROM r.trade_date) = %s
            """
            
            cur.execute(query, (date.year, date.month))
            return cur.fetchall()
            
    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        conn.close()

# Additional utility functions

def get_monthly_ohlc_for_index(index_id: int, date: datetime) -> Dict[str, float]:
    """Get monthly OHLC prices for a specific index and date."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT open_price, high_price, low_price, close_price
                FROM monthly_ohlc
                WHERE index_id = %s
                AND EXTRACT(YEAR FROM trade_date) = %s
                AND EXTRACT(MONTH FROM trade_date) = %s
                LIMIT 1
            """
            
            cur.execute(query, (index_id, date.year, date.month))
            result = cur.fetchone()
            if result:
                return {
                    'open': float(result[0]),
                    'high': float(result[1]),
                    'low': float(result[2]),
                    'close': float(result[3])
                }
            return {}
            
    except Exception as e:
        print(f"Error: {e}")
        return {}
    finally:
        conn.close()

def get_index_name(index_id: int) -> Optional[str]:
    """Get the name of an index given its ID."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = "SELECT index_name FROM indices WHERE index_id = %s LIMIT 1"
            cur.execute(query, (index_id,))
            result = cur.fetchone()
            return result[0] if result else None
            
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        conn.close()

# Example usage
if __name__ == "__main__":
    sample_date = datetime(2020, 4, 1)
    
    # Example 1: Get score for specific index, subtype, and date
    score = get_score_for_index_subtype_date(1, 'n1', sample_date)
    print(f"N1 Score: {score}")
    
    # Example 2: Get all scores for an index and date
    all_scores = get_all_scores_for_index_date(1, sample_date)
    print(f"All Scores: {all_scores}")
    
    # Example 3: Get scores for a subtype across all indices
    subtype_scores = get_scores_for_subtype_date('b1', sample_date)
    print(f"B1 Scores for all indices: {subtype_scores}")
    
    # Example 4: Get monthly OHLC
    ohlc = get_monthly_ohlc_for_index(1, sample_date)
    print(f"Monthly OHLC: {ohlc}")
