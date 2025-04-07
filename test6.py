import yfinance as yf

# Define the NIFTY AUTO index ticker (for NSE indices, we use '^CNXAUTO')
index_ticker = "^CNXAUTO"

# Fetch data for the NIFTY AUTO index
try:
    nifty_auto = yf.Ticker(index_ticker)
    # Get historical data or basic info (yfinance doesn't directly list constituents)
    info = nifty_auto.info
    print("NIFTY AUTO Index Info:")
    print(info)

    # If you want historical data instead
    historical_data = nifty_auto.history(period="1mo")  # Last 1 month of data
    if not historical_data.empty:
        print("Historical Data for NIFTY AUTO:")
        print(historical_data)
    else:
        print("No historical data received for NIFTY AUTO")
except Exception as e:
    print(f"Error fetching NIFTY AUTO data: {e}")

# Note: To get individual stocks in NIFTY AUTO, you need a predefined list of tickers
# Example list of some NIFTY AUTO constituents (you can expand this)
nifty_auto_stocks = ["TATAMOTORS.NS", "MARUTI.NS", "M&M.NS", "BAJAJ-AUTO.NS"]

# Fetch data for individual stocks
print("\nFetching data for individual NIFTY AUTO stocks:")
for stock in nifty_auto_stocks:
    try:
        stock_data = yf.Ticker(stock)
        info = stock_data.info
        print(f"{stock} Info:")
        print(info)
    except Exception as e:
        print(f"Error fetching data for {stock}: {e}")