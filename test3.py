from nsetools import Nse

# Initialize NSE object
nse = Nse()

# Fetch stocks in NIFTY 50
try:
    index_name="NIFTY AUTO"
    stocks_in_nifty50 = nse.get_stocks_in_index(index="NIFTY AUTO")
    if stocks_in_nifty50:
        print("Stocks in NIFTY 50:")
        print(stocks_in_nifty50)
    else:
        print("No data received for {index_name}")
except Exception as e:
    print(f"Error fetching {index_name} data: {e}")
