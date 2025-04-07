import psycopg2
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

# Constants
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"
NIFTY50_INDEX_ID = 1
BSE500_INDEX_ID = 2
SCORE_DATE = date.today()

class Candle:
    def __init__(self, trade_date: date, open_price: float, high: float, low: float, close: float):
        self.trade_date = trade_date
        self.open = open_price
        self.high = high
        self.low = low
        self.close = close
        self.tag: Optional[str] = None
        self.n1: Optional[int] = None
        self.n2: Optional[int] = None
        self.n3: Optional[int] = None
        self.b1: Optional[int] = None
        self.b2: Optional[int] = None
        self.b3: Optional[int] = None

    def __str__(self) -> str:
        return (f"Date: {self.trade_date}, Open: {self.open:.8f}, High: {self.high:.8f}, "
                f"Low: {self.low:.8f}, Close: {self.close:.8f}, Tag: {self.tag}, "
                f"n1: {self.n1 if self.n1 is not None else 'null'}, "
                f"n2: {self.n2 if self.n2 is not None else 'null'}, "
                f"n3: {self.n3 if self.n3 is not None else 'null'}, "
                f"b1: {self.b1 if self.b1 is not None else 'null'}, "
                f"b2: {self.b2 if self.b2 is not None else 'null'}, "
                f"b3: {self.b3 if self.b3 is not None else 'null'}")

    @staticmethod
    def print_n_ratios(candles: List['Candle']) -> None:
        print("\n--------Printing N Ratios------------")
        print("Date,Open,High,Low,Close,Tag,n1,n2,n3")
        for candle in candles:
            print(f"{candle.trade_date},{candle.open:.8f},{candle.high:.8f},"
                  f"{candle.low:.8f},{candle.close:.8f},{candle.tag},"
                  f"{candle.n1 if candle.n1 is not None else 'null'},"
                  f"{candle.n2 if candle.n2 is not None else 'null'},"
                  f"{candle.n3 if candle.n3 is not None else 'null'}")

    @staticmethod
    def print_b_ratios(candles: List['Candle']) -> None:
        print("\n--------Printing B Ratios------------")
        print("Date,Open,High,Low,Close,Tag,b1,b2,b3")
        for candle in candles:
            print(f"{candle.trade_date},{candle.open:.8f},{candle.high:.8f},"
                  f"{candle.low:.8f},{candle.close:.8f},{candle.tag},"
                  f"{candle.b1 if candle.b1 is not None else 'null'},"
                  f"{candle.b2 if candle.b2 is not None else 'null'},"
                  f"{candle.b3 if candle.b3 is not None else 'null'}")

def create_tables():
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS monthly_ohlc (
                        trade_date TIMESTAMPTZ NOT NULL,
                        index_id INT NOT NULL,
                        open_price DECIMAL(15, 8) NOT NULL,
                        high_price DECIMAL(15, 8) NOT NULL,
                        low_price DECIMAL(15, 8) NOT NULL,
                        close_price DECIMAL(15, 8) NOT NULL,
                        FOREIGN KEY (index_id) REFERENCES indices(index_id) ON DELETE CASCADE,
                        CONSTRAINT unique_monthly_index_date UNIQUE (index_id, trade_date)
                    );
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS n_ratios (
                        trade_date TIMESTAMPTZ NOT NULL,
                        sectoral_index_id INT NOT NULL,
                        benchmark_index_id INT NOT NULL,
                        open_ratio DECIMAL(15, 8) NOT NULL,
                        high_ratio DECIMAL(15, 8) NOT NULL,
                        low_ratio DECIMAL(15, 8) NOT NULL,
                        close_ratio DECIMAL(15, 8) NOT NULL,
                        tag VARCHAR(20),
                        n1 INT,
                        n2 INT,
                        n3 INT,
                        FOREIGN KEY (sectoral_index_id) REFERENCES indices(index_id) ON DELETE CASCADE,
                        FOREIGN KEY (benchmark_index_id) REFERENCES indices(index_id) ON DELETE CASCADE,
                        CONSTRAINT unique_n_ratio_date UNIQUE (trade_date, sectoral_index_id, benchmark_index_id)
                    );
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS b_ratios (
                        trade_date TIMESTAMPTZ NOT NULL,
                        sectoral_index_id INT NOT NULL,
                        benchmark_index_id INT NOT NULL,
                        open_ratio DECIMAL(15, 8) NOT NULL,
                        high_ratio DECIMAL(15, 8) NOT NULL,
                        low_ratio DECIMAL(15, 8) NOT NULL,
                        close_ratio DECIMAL(15, 8) NOT NULL,
                        tag VARCHAR(20),
                        b1 INT,
                        b2 INT,
                        b3 INT,
                        FOREIGN KEY (sectoral_index_id) REFERENCES indices(index_id) ON DELETE CASCADE,
                        FOREIGN KEY (benchmark_index_id) REFERENCES indices(index_id) ON DELETE CASCADE,
                        CONSTRAINT unique_b_ratio_date UNIQUE (trade_date, sectoral_index_id, benchmark_index_id)
                    );
                """)
                conn.commit()
                print("Tables created successfully!")
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")

def fetch_and_store_monthly_data(index_id: int, end_date: date) -> Dict[str, 'Candle']:
    candles = {}
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                sql = """
                    WITH monthly_data AS (
                        SELECT EXTRACT(YEAR FROM trade_date) AS year,
                               EXTRACT(MONTH FROM trade_date) AS month,
                               trade_date,
                               FIRST_VALUE(open_price) OVER (PARTITION BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date) ORDER BY trade_date) AS open_price,
                               MAX(high_price) OVER (PARTITION BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date)) AS high_price,
                               MIN(low_price) OVER (PARTITION BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date)) AS low_price,
                               LAST_VALUE(close_price) OVER (PARTITION BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date) ORDER BY trade_date
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price
                        FROM daily_ohlc
                        WHERE index_id = %s AND trade_date <= %s
                    )
                    SELECT year, month, open_price, high_price, low_price, close_price, trade_date AS last_date
                    FROM monthly_data
                    WHERE trade_date = (SELECT MAX(trade_date) FROM monthly_data md2
                                      WHERE md2.year = monthly_data.year AND md2.month = monthly_data.month)
                """
                cur.execute(sql, (index_id, end_date))
                for row in cur.fetchall():
                    year, month, open_price, high_price, low_price, close_price, trade_date = row
                    key = f"{int(year)}-{int(month):02d}"
                    candle = Candle(trade_date, open_price, high_price, low_price, close_price)
                    candles[key] = candle
                    
                    cur.execute("""
                        INSERT INTO monthly_ohlc (trade_date, index_id, open_price, high_price, low_price, close_price)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (index_id, trade_date) DO UPDATE
                        SET open_price = EXCLUDED.open_price,
                            high_price = EXCLUDED.high_price,
                            low_price = EXCLUDED.low_price,
                            close_price = EXCLUDED.close_price
                    """, (trade_date, index_id, open_price, high_price, low_price, close_price))
                
                conn.commit()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    return candles

def get_and_store_ratio_data(sectoral: Dict[str, 'Candle'], benchmark: Dict[str, 'Candle'], 
                           sectoral_id: int, benchmark_id: int, table_name: str) -> List['Candle']:
    ratio_candles = []
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                # Generate all ratio candles
                for month in sectoral:
                    if month in benchmark:
                        s = sectoral[month]
                        b = benchmark[month]
                        candle = Candle(
                            s.trade_date,
                            s.open / b.open,
                            s.high / b.high,
                            s.low / b.low,
                            s.close / b.close
                        )
                        ratio_candles.append(candle)
                
                # Calculate tags and scores for all candles
                tag_candles(ratio_candles)
                temp_list = ratio_candles.copy()
                calculate_scores(ratio_candles, temp_list) if table_name == 'n_ratios' else calculate_scores(temp_list, ratio_candles)
                
                # Insert ALL candles into the appropriate table
                for candle in ratio_candles:
                    if table_name == 'n_ratios':
                        cur.execute("""
                            INSERT INTO n_ratios (trade_date, sectoral_index_id, benchmark_index_id, 
                                               open_ratio, high_ratio, low_ratio, close_ratio,
                                               tag, n1, n2, n3)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (trade_date, sectoral_index_id, benchmark_index_id) DO UPDATE
                            SET open_ratio = EXCLUDED.open_ratio,
                                high_ratio = EXCLUDED.high_ratio,
                                low_ratio = EXCLUDED.low_ratio,
                                close_ratio = EXCLUDED.close_ratio,
                                tag = EXCLUDED.tag,
                                n1 = EXCLUDED.n1,
                                n2 = EXCLUDED.n2,
                                n3 = EXCLUDED.n3
                        """, (candle.trade_date, sectoral_id, benchmark_id,
                              candle.open, candle.high, candle.low, candle.close,
                              candle.tag, candle.n1, candle.n2, candle.n3))
                    else:  # b_ratios
                        cur.execute("""
                            INSERT INTO b_ratios (trade_date, sectoral_index_id, benchmark_index_id, 
                                               open_ratio, high_ratio, low_ratio, close_ratio,
                                               tag, b1, b2, b3)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (trade_date, sectoral_index_id, benchmark_index_id) DO UPDATE
                            SET open_ratio = EXCLUDED.open_ratio,
                                high_ratio = EXCLUDED.high_ratio,
                                low_ratio = EXCLUDED.low_ratio,
                                close_ratio = EXCLUDED.close_ratio,
                                tag = EXCLUDED.tag,
                                b1 = EXCLUDED.b1,
                                b2 = EXCLUDED.b2,
                                b3 = EXCLUDED.b3
                        """, (candle.trade_date, sectoral_id, benchmark_id,
                              candle.open, candle.high, candle.low, candle.close,
                              candle.tag, candle.b1, candle.b2, candle.b3))
                conn.commit()
                print(f"Inserted {len(ratio_candles)} rows into {table_name}")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    return ratio_candles

def tag_candles(candles: List['Candle']) -> None:
    prev_tag = "Bullish"
    for i, curr in enumerate(candles):
        if i == 0:
            curr.tag = "Bullish"
        else:
            prev = candles[i - 1]
            prev_close = prev.close
            prev_open = prev.open
            curr_close = curr.close

            if curr_close > prev_close:
                if prev_tag in ["Highly Bullish", "Bullish"]:
                    curr.tag = "Highly Bullish" if prev_close >= prev_open else \
                              "Highly Bullish" if curr_close > prev_open else "Bullish"
                else:
                    curr.tag = "Bullish" if prev_close > prev_open else \
                              "Bullish" if curr_close > prev_open else "Bearish"
            elif curr_close < prev_close:
                if prev_tag in ["Highly Bullish", "Bullish"]:
                    curr.tag = "Bullish" if prev_close >= prev_open and curr_close > prev_open else "Bearish"
                else:
                    curr.tag = "Highly Bearish" if prev_close <= prev_open else \
                              "Bearish" if curr_close > prev_open else "Highly Bearish"
            else:
                if prev_tag in ["Highly Bullish", "Bullish"]:
                    curr.tag = prev_tag if prev_close >= prev_open else "Bullish"
                else:
                    curr.tag = "Bearish" if prev_close < prev_open else prev_tag
        prev_tag = curr.tag

def calculate_scores(n_candles: List['Candle'], b_candles: List['Candle']) -> None:
    def score_for_tag(tag: str) -> int:
        return {"Highly Bullish": 2, "Bullish": 1, "Bearish": -1, "Highly Bearish": -2}.get(tag, 0)

    n_scores = [score_for_tag(candle.tag) for candle in n_candles]
    b_scores = [score_for_tag(candle.tag) for candle in b_candles]

    for i in range(len(n_candles)):
        if i >= 1:
            n_candles[i].n1 = n_scores[i - 1]
            b_candles[i].b1 = b_scores[i - 1]
        if i >= 2:
            n_candles[i].n2 = n_scores[i - 2] + n_scores[i - 1]
            b_candles[i].b2 = b_scores[i - 2] + b_scores[i - 1]
        if i >= 3:
            n_candles[i].n3 = n_scores[i - 3] + n_scores[i - 2] + n_scores[i - 1]
            b_candles[i].b3 = b_scores[i - 3] + b_scores[i - 2] + b_scores[i - 1]



def fetch_indices():
    try:
        conn = psycopg2.connect(DEFAULT_DB_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT index_id FROM indices;")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        print("Error:", e)
        return []

def process_sectoral_data(sectoral_id):
    try:
        
        cutoff = SCORE_DATE.replace(day=1) - timedelta(days=1)
        
        index_ids = fetch_indices()
        print(index_ids)
        sectoral_data = fetch_and_store_monthly_data(sectoral_id, cutoff)
        nifty_data = fetch_and_store_monthly_data(NIFTY50_INDEX_ID, cutoff)
        bse500_data = fetch_and_store_monthly_data(BSE500_INDEX_ID, cutoff)

        # print(f"{sectoral_id}")
        # for key, value in sectoral_data.items():
        #     print(f"{key} -> {value}")
        # print("NIFTY50_INDEX_ID")
        # for key, value in nifty_data.items():
        #     print(f"{key} -> {value}")
        # print("BSE500_INDEX_ID")
        # for key, value in bse500_data.items():
        #     print(f"{key} -> {value}")

        if not all([sectoral_data, nifty_data, bse500_data]):
            print("Insufficient data for calculation.")
            return

        n_ratio = get_and_store_ratio_data(sectoral_data, nifty_data, 
                                         sectoral_id, NIFTY50_INDEX_ID, 'n_ratios')
        b_ratio = get_and_store_ratio_data(sectoral_data, bse500_data, 
                                         sectoral_id, BSE500_INDEX_ID, 'b_ratios')

        if not n_ratio or not b_ratio:
            print("No matching months for ratio calculation.")
            return

        print(f"Scores for {SCORE_DATE} (based on data up to {cutoff}):")
        last_n = n_ratio[-1]
        last_b = b_ratio[-1]
        print(f"N Scores: n1={last_n.n1}, n2={last_n.n2}, n3={last_n.n3}")
        print(f"B Scores: b1={last_b.b1}, b2={last_b.b2}, b3={last_b.b3}")

        Candle.print_n_ratios(n_ratio)
        Candle.print_b_ratios(b_ratio)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

# Call process_sectoral_data for all indices
def main():

    # create_tables()
    index_ids = fetch_indices()
    if not index_ids:
        print("No indices found to process.")
        return
    
    for sectoral_id in index_ids:
        print(f"\nProcessing sectoral ID: {sectoral_id}")
        print("-" * 50)
        process_sectoral_data(sectoral_id)

if __name__ == "__main__":
    main()