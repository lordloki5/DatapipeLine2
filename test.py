from nsetools import Nse

# Initialize NSE object
nse = Nse()

# List of NSE indices to check
nse_indices = [
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY Chemicals",
    "NIFTY FIN SERVICE",
    "NIFTY FINANCIAL SERVICES 25/50",
    "NIFTY FINANCIAL SERVICES EX BANK",
    "NIFTY FMCG",
    "NIFTY HEALTHCARE",
    "NIFTY IT",
    "NIFTY MEDIA",
    "NIFTY METAL",
    "NIFTY PHARMA",
    "NIFTY PRIVATE BANK",
    "NIFTY PSU BANK",
    "NIFTY REALTY",
    "NIFTY CONSR DURBL",
    "NIFTY OIL AND GAS"
]
nse_indices2 = [
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY CHEMICALS",  # Note: Not in your full list, may need verification
    "NIFTY FIN SERVICE",
    "NIFTY FINSRV25 50", # Note: Not in your full list, may need verification
    "NIFTY FINSEREXBNK",# Note: Not in your full list, may need verification
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

def find_index_for_symbol(stock_symbol):
    stock_symbol = stock_symbol.upper()  # Ensure symbol is uppercase
    found_indices = []
    
    # Iterate through each index
    for index in nse_indices:
        try:
            # Get list of stocks in the current index
            stocks_in_index = nse.get_stocks_in_index(index)
            # Check if the stock symbol is in this index
            if stock_symbol in stocks_in_index:
                found_indices.append(index)
        except Exception as e:
            print(f"Error fetching data for {index}: {e}")
    
    # Return results
    if found_indices:
        return f"{stock_symbol} belongs to: {', '.join(found_indices)}"
    else:
        return f"{stock_symbol} does not belong to any of the specified NSE indices."

# Get user input
stock_symbol = input("Enter an Indian stock symbol (e.g., HDFCBANK, RELIANCE): ")

# Find and print the index
result = find_index_for_symbol(stock_symbol)
print(result)