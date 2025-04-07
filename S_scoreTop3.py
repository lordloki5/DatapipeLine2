import psycopg2
from datetime import date
from typing import List, Dict, Tuple

# Database connection string (adjust as needed)
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def create_top_scores_tables():
    """Create tables to store top 1, top 2, and top 3 scores for n_ratios and b_ratios."""
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                # Table for top 1 scores (all indices with the highest score)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS top_1_scores (
                        trade_date TIMESTAMPTZ NOT NULL,
                        ratio_type CHAR(1) NOT NULL CHECK (ratio_type IN ('N', 'B')),
                        sectoral_index_id INT NOT NULL,
                        score_type VARCHAR(2) NOT NULL CHECK (score_type IN ('n1', 'n2', 'n3', 'b1', 'b2', 'b3')),
                        score_value INT NOT NULL,
                        FOREIGN KEY (sectoral_index_id) REFERENCES indices(index_id) ON DELETE CASCADE,
                        CONSTRAINT unique_top_1_entry UNIQUE (trade_date, ratio_type, score_type, sectoral_index_id)
                    );
                """)

                # Table for top 2 scores (all indices with the top two distinct scores)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS top_2_scores (
                        trade_date TIMESTAMPTZ NOT NULL,
                        ratio_type CHAR(1) NOT NULL CHECK (ratio_type IN ('N', 'B')),
                        sectoral_index_id INT NOT NULL,
                        score_type VARCHAR(2) NOT NULL CHECK (score_type IN ('n1', 'n2', 'n3', 'b1', 'b2', 'b3')),
                        score_value INT NOT NULL,
                        FOREIGN KEY (sectoral_index_id) REFERENCES indices(index_id) ON DELETE CASCADE,
                        CONSTRAINT unique_top_2_entry UNIQUE (trade_date, ratio_type, score_type, sectoral_index_id)
                    );
                """)

                # Table for top 3 scores (all indices with the top three distinct scores)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS top_3_scores (
                        trade_date TIMESTAMPTZ NOT NULL,
                        ratio_type CHAR(1) NOT NULL CHECK (ratio_type IN ('N', 'B')),
                        sectoral_index_id INT NOT NULL,
                        score_type VARCHAR(2) NOT NULL CHECK (score_type IN ('n1', 'n2', 'n3', 'b1', 'b2', 'b3')),
                        score_value INT NOT NULL,
                        FOREIGN KEY (sectoral_index_id) REFERENCES indices(index_id) ON DELETE CASCADE,
                        CONSTRAINT unique_top_3_entry UNIQUE (trade_date, ratio_type, score_type, sectoral_index_id)
                    );
                """)
                conn.commit()
                print("Top scores tables created successfully!")
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")

def fetch_ratios_data(table_name: str) -> Dict[date, List[Tuple[int, int, int, int]]]:
    """Fetch ratio data from n_ratios or b_ratios table."""
    data = {}
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                if table_name == 'n_ratios':
                    sql = """
                        SELECT trade_date, sectoral_index_id, n1, n2, n3
                        FROM n_ratios
                        WHERE n1 IS NOT NULL
                        ORDER BY trade_date, sectoral_index_id
                    """
                elif table_name == 'b_ratios':
                    sql = """
                        SELECT trade_date, sectoral_index_id, b1, b2, b3
                        FROM b_ratios
                        WHERE b1 IS NOT NULL
                        ORDER BY trade_date, sectoral_index_id
                    """
                else:
                    raise ValueError("Invalid table_name. Use 'n_ratios' or 'b_ratios'.")

                cur.execute(sql)
                for row in cur.fetchall():
                    trade_date = row[0]
                    sectoral_id = row[1]
                    scores = (row[2], row[3], row[4])
                    if trade_date not in data:
                        data[trade_date] = []
                    data[trade_date].append((sectoral_id, *scores))
    except psycopg2.Error as e:
        print(f"Error fetching data from {table_name}: {e}")
    return data

def get_top_scores(scores: List[Tuple[int, int, int, int]], score_idx: int, top_n: int) -> List[Tuple[int, int]]:
    """Get all sectoral indices with the top N distinct scores for the given score type."""
    # Extract score values and sectoral IDs for the given score type (0 = n1/b1, 1 = n2/b2, 2 = n3/b3)
    score_map = [(entry[0], entry[score_idx + 1]) for entry in scores if entry[score_idx + 1] is not None]
    # Sort by score in descending order
    score_map.sort(key=lambda x: x[1], reverse=True)
    
    # Group by distinct scores
    distinct_scores = []
    current_score = None
    current_indices = []
    
    for sectoral_id, score in score_map:
        if score != current_score:
            if current_indices:
                distinct_scores.append((current_score, current_indices))
            current_score = score
            current_indices = [sectoral_id]
        else:
            current_indices.append(sectoral_id)
    
    # Append the last group
    if current_indices:
        distinct_scores.append((current_score, current_indices))
    
    # Take top N distinct scores and flatten into (sectoral_id, score) tuples
    result = []
    for score, indices in distinct_scores[:top_n]:
        for sectoral_id in indices:
            result.append((sectoral_id, score))
    
    return result

def populate_top_scores_tables():
    """Populate top scores tables with data from n_ratios and b_ratios."""
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                # Fetch data from both tables
                n_ratios_data = fetch_ratios_data('n_ratios')
                b_ratios_data = fetch_ratios_data('b_ratios')

                # Process each table
                for ratio_type, ratios_data in [('N', n_ratios_data), ('B', b_ratios_data)]:
                    score_types = ['n1', 'n2', 'n3'] if ratio_type == 'N' else ['b1', 'b2', 'b3']
                    for trade_date, scores in ratios_data.items():
                        for idx, score_type in enumerate(score_types):
                            # Get top 1, 2, and 3 scores with all matching indices
                            top_1 = get_top_scores(scores, idx, 1)
                            top_2 = get_top_scores(scores, idx, 2)
                            top_3 = get_top_scores(scores, idx, 3)

                            # Insert into top_1_scores (all indices with the highest score)
                            for sectoral_id, score in top_1:
                                cur.execute("""
                                    INSERT INTO top_1_scores (trade_date, ratio_type, sectoral_index_id, score_type, score_value)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (trade_date, ratio_type, score_type, sectoral_index_id) DO UPDATE
                                    SET score_value = EXCLUDED.score_value
                                """, (trade_date, ratio_type, sectoral_id, score_type, score))

                            # Insert into top_2_scores (all indices with the top two distinct scores)
                            for sectoral_id, score in top_2:
                                cur.execute("""
                                    INSERT INTO top_2_scores (trade_date, ratio_type, sectoral_index_id, score_type, score_value)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (trade_date, ratio_type, score_type, sectoral_index_id) DO UPDATE
                                    SET score_value = EXCLUDED.score_value
                                """, (trade_date, ratio_type, sectoral_id, score_type, score))

                            # Insert into top_3_scores (all indices with the top three distinct scores)
                            for sectoral_id, score in top_3:
                                cur.execute("""
                                    INSERT INTO top_3_scores (trade_date, ratio_type, sectoral_index_id, score_type, score_value)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (trade_date, ratio_type, score_type, sectoral_index_id) DO UPDATE
                                    SET score_value = EXCLUDED.score_value
                                """, (trade_date, ratio_type, sectoral_id, score_type, score))

                conn.commit()
                print("Top scores tables populated successfully!")
    except psycopg2.Error as e:
        print(f"Error populating tables: {e}")

def main():
    create_top_scores_tables()
    populate_top_scores_tables()

if __name__ == "__main__":
    main()