import yfinance as yf

def get_index_constituents(index_name):
    index_ticker_mapping = {
        "NIFTY 50": "^NSEI",
        "NIFTY AUTO": "^CNXAUTO"
    }

    index_ticker = index_ticker_mapping.get(index_name)
    if not index_ticker:
        print(f"Index '{index_name}' not supported.")
        return pd.DataFrame()  # Return empty DataFrame instead of list

    try:
        index_data = yf.Ticker(index_ticker)
        return index_data.history(period="1d")  # Returns DataFrame
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    index_name = "NIFTY 50"
    constituents = get_index_constituents(index_name)
    
    if not constituents.empty:
        print(f"Data found for {index_name}:")
        print(constituents)
    else:
        print(f"No data found for {index_name}")
