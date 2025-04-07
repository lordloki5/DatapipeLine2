from nsetools import Nse
import time  # Import time for delays

# Initialize NSE object
nse = Nse()

# List of NSE indices (excluding NIFTY CHEMICALS, NIFTY FINSRV25 50, and NIFTY FINSEREXBNK, which are checked manually)
nse_indices = [
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY FIN SERVICE",
    # "NIFTY CHEMICALS",    # Removed from API query since we manually check it
    # "NIFTY FINSRV25 50",  # Removed from API query since we manually check it
    # "NIFTY FINSEREXBNK",  # Removed from API query since we manually check it
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

# Predefined stock symbols for "NIFTY CHEMICALS"
nifty_chemicals_stocks = {
    "AARTIIND", "ATUL", "BAYERCROP", "CHAMBLFERT", "COROMANDEL",
    "DEEPAKNTR", "EIDPARRY", "FLUOROCHEM", "GNFC", "HSCL",
    "LINDEINDIA", "NAVINFLUOR", "PCBL", "PIIND", "PIDILITIND",
    "SRF", "SOLARINDS", "SUMICHEM", "TATACHEM", "UPL"
}

# Predefined stock symbols for "NIFTY FINSRV25 50"
nifty_finsrv25_50_stocks = {
    "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "CHOLAFIN", "HDFCAMC",
    "HDFCBANK", "HDFCLIFE", "ICICIBANK", "ICICIGI", "ICICIPRULI",
    "JIOFIN", "KOTAKBANK", "LICHSGFIN", "MUTHOOTFIN", "PFC",
    "RECLTD", "SBICARD", "SBILIFE", "SHRIRAMFIN", "SBIN"
}

# Predefined stock symbols for "NIFTY FINSEREXBNK"
nifty_finserexbnk_stocks = {
    "ABCAPITAL", "ANGELONE", "BSE", "BAJFINANCE", "BAJAJFINSV",
    "CDSL", "CHOLAFIN", "CAMS", "HDFCAMC", "HDFCLIFE",
    "ICICIGI", "ICICIPRULI", "IEX", "IRFC", "JIOFIN",
    "LTF", "LICHSGFIN", "LICI", "M&MFIN", "MFSL",
    "MCX", "MUTHOOTFIN", "PAYTM", "POLICYBZR", "PEL",
    "PFC", "RECLTD", "SBICARD", "SBILIFE", "SHRIRAMFIN"
}

def find_sector(stock_symbol):
    stock_symbol = stock_symbol.upper()  # Ensure uppercase
    found_indices = []

    # Manually check for NIFTY CHEMICALS
    if stock_symbol in nifty_chemicals_stocks:
        found_indices.append("NIFTY CHEMICALS")

    # Manually check for NIFTY FINSRV25 50
    if stock_symbol in nifty_finsrv25_50_stocks:
        found_indices.append("NIFTY FINSRV25 50")

    # Manually check for NIFTY FINSEREXBNK
    if stock_symbol in nifty_finserexbnk_stocks:
        found_indices.append("NIFTY FINSEREXBNK")

    # Check other indices using API
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
        
        time.sleep(1)  # Prevent rate limiting by adding a delay

    if found_indices:
        return f"{stock_symbol} belongs to: {', '.join(found_indices)}"
    else:
        return f"{stock_symbol} does not belong to any of the specified NSE indices."

# Get user input
stock_symbol = input("Enter an Indian stock symbol (e.g., HDFCBANK, RELIANCE, TATACHEM, AXISBANK, PAYTM): ")

# Find and print the sector
result = find_sector(stock_symbol)
print(result)
