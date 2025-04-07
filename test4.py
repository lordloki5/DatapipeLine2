from nsetools import Nse
import time  # Import time for delays

# Initialize NSE object
nse = Nse()

# List of NSE indices
nse_indices = [
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY CHEMICALS",
    "NIFTY FIN SERVICE",
    "NIFTY FINSRV25 50",
    "NIFTY FINSEREXBNK",
    "NIFTY FMCG",
    "NIFTY HEALTHCARE",
    "NIFTY IT",
    "NIFTY MEDIA",
    "NIFTY METAL",
    "NIFTY PHARMA",
    "NIFTY PVT BANK",
    "NIFTY PSU BANK",
    "NIFTY REALTY",
    "NIFTY CONSR DURBL",
    "NIFTY OIL AND GAS"
]

def find_sector(stock_symbol):
    stock_symbol = stock_symbol.upper()  # Ensure uppercase
    found_indices = []
    
    for index in nse_indices:
        try:
            stocks_in_index = nse.get_stocks_in_index(index)
            if not stocks_in_index:  # Handle empty responses
                print(f"Skipping {index}: No data received.")
                continue
            if stock_symbol in stocks_in_index:
                found_indices.append(index)
        except Exception as e:
            print(f"Error fetching data for {index}: {e}")
        
        time.sleep(5)  # Prevent rate limiting by adding a delay

    if found_indices:
        return f"{stock_symbol} belongs to: {', '.join(found_indices)}"
    else:
        return f"{stock_symbol} does not belong to any of the specified NSE indices."

# Get user input
stock_symbol = input("Enter an Indian stock symbol (e.g., HDFCBANK, RELIANCE): ")

# Find and print the sector
result = find_sector(stock_symbol)
print(result)
