import psycopg2
from datetime import datetime, date
from typing import Dict, List, Optional

# Constants
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"
SECTORAL_INDEX_ID = 4
NIFTY50_INDEX_ID = 1
BSE500_INDEX_ID = 2
SCORE_DATE = date(2025, 3, 1)

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
                  f"{candle.n1},{candle.n2},{candle.n3}")

    @staticmethod
    def print_b_ratios(candles: List['Candle']) -> None:
        print("\n--------Printing B Ratios------------")
        print("Date,Open,High,Low,Close,Tag,b1,b2,b3")
        for candle in candles:
            print(f"{candle.trade_date},{candle.open:.8f},{candle.high:.8f},"
                  f"{candle.low:.8f},{candle.close:.8f},{candle.tag},"
                  f"{candle.b1},{candle.b2},{candle.b3}")

def fetch_monthly_data(index_id: int, end_date: date) -> Dict[str, 'Candle']:
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
                    candles[key] = Candle(
                        trade_date,
                        open_price,
                        high_price,
                        low_price,
                        close_price
                    )
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    return candles

def fetch_monthly_data2(index_id: int, end_date: date) -> Dict[str, 'Candle']:
    candles = {}
    try:
        with psycopg2.connect(DEFAULT_DB_URL) as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT d.trade_date, d.open_price, d.high_price, d.low_price, d.close_price
                    FROM daily_ohlc d
                    INNER JOIN (
                        SELECT EXTRACT(YEAR FROM trade_date) AS year,
                               EXTRACT(MONTH FROM trade_date) AS month,
                               MAX(trade_date) AS max_date
                        FROM daily_ohlc
                        WHERE index_id = %s AND trade_date <= %s
                        GROUP BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date)
                    ) m ON d.trade_date = m.max_date AND d.index_id = %s
                """
                cur.execute(sql, (index_id, end_date, index_id))
                for row in cur.fetchall():
                    trade_date, open_price, high_price, low_price, close_price = row
                    key = f"{trade_date.year}-{trade_date.month:02d}"
                    candles[key] = Candle(
                        trade_date,
                        open_price,
                        high_price,
                        low_price,
                        close_price
                    )
                    print(f"Fetched for index {index_id}: {key} -> {trade_date}")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    return candles

def get_ratio_data(sectoral: Dict[str, 'Candle'], benchmark: Dict[str, 'Candle']) -> List['Candle']:
    ratio_candles = []
    for month in sectoral:
        if month in benchmark:
            s = sectoral[month]
            b = benchmark[month]
            ratio_candles.append(Candle(
                s.trade_date,
                s.open / b.open,
                s.high / b.high,
                s.low / b.low,
                s.close / b.close
            ))
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

            if curr_close > prev_close:  # Green candle
                if prev_tag in ["Highly Bullish", "Bullish"]:
                    curr.tag = "Highly Bullish" if prev_close >= prev_open else \
                              "Highly Bullish" if curr_close > prev_open else "Bullish"
                else:
                    curr.tag = "Bullish" if prev_close > prev_open else \
                              "Bullish" if curr_close > prev_open else "Bearish"
            elif curr_close < prev_close:  # Red candle
                if prev_tag in ["Highly Bullish", "Bullish"]:
                    curr.tag = "Bullish" if prev_close >= prev_open and curr_close > prev_open else "Bearish"
                else:
                    curr.tag = "Highly Bearish" if prev_close <= prev_open else \
                              "Bearish" if curr_close > prev_open else "Highly Bearish"
            else:  # Neutral candle
                if prev_tag in ["Highly Bullish", "Bullish"]:
                    curr.tag = prev_tag if prev_close >= prev_open else "Bullish"
                else:
                    curr.tag = "Bearish" if prev_close < prev_open else prev_tag
        prev_tag = curr.tag

def calculate_scores(n_candles: List['Candle'], b_candles: List['Candle']) -> None:
    def score_for_tag(tag: str) -> int:
        return {
            "Highly Bullish": 2,
            "Bullish": 1,
            "Bearish": -1,
            "Highly Bearish": -2
        }.get(tag, 0)

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

def main():
    try:
        cutoff = SCORE_DATE.replace(day=1) - timedelta(days=1)  # Last day of Feb
        
        sectoral_data = fetch_monthly_data(SECTORAL_INDEX_ID, cutoff)
        nifty_data = fetch_monthly_data(NIFTY50_INDEX_ID, cutoff)
        bse500_data = fetch_monthly_data(BSE500_INDEX_ID, cutoff)

        print("SECTORAL_INDEX_ID")
        for key, value in sectoral_data.items():
            print(f"{key} -> {value}")
        print("NIFTY50_INDEX_ID")
        for key, value in nifty_data.items():
            print(f"{key} -> {value}")
        print("BSE500_INDEX_ID")
        for key, value in bse500_data.items():
            print(f"{key} -> {value}")

        if not all([sectoral_data, nifty_data, bse500_data]):
            print("Insufficient data for calculation.")
            return

        n_ratio = get_ratio_data(sectoral_data, nifty_data)
        b_ratio = get_ratio_data(sectoral_data, bse500_data)

        if not n_ratio or not b_ratio:
            print("No matching months for ratio calculation.")
            return

        tag_candles(n_ratio)
        tag_candles(b_ratio)
        calculate_scores(n_ratio, b_ratio)

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

if __name__ == "__main__":
    from datetime import timedelta
    main()