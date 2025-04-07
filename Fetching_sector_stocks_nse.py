from nsetools import Nse
import time
import pandas as pd
from pandas import ExcelWriter

# Initialize NSE object
nse = Nse()

# List of NSE indices
nse_indices = [
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY FIN SERVICE",
    "NIFTY CHEMICALS",
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

# Predefined stock symbols for special indices
nifty_chemicals_stocks = {
    "AARTIIND", "ATUL", "BAYERCROP", "CHAMBLFERT", "COROMANDEL",
    "DEEPAKNTR", "EIDPARRY", "FLUOROCHEM", "GNFC", "HSCL",
    "LINDEINDIA", "NAVINFLUOR", "PCBL", "PIIND", "PIDILITnD",
    "SRF", "SOLARINDS", "SUMICHEM", "TATACHEM", "UPL"
}

nifty_finsrv25_50_stocks = {
    "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "CHOLAFIN", "HDFCAMC",
    "HDFCBANK", "HDFCLIFE", "ICICIBANK", "ICICIGI", "ICICIPRULI",
    "JIOFIN", "KOTAKBANK", "LICHSGFIN", "MUTHOOTFIN", "PFC",
    "RECLTD", "SBICARD", "SBILIFE", "SHRIRAMFIN", "SBIN"
}

nifty_finserexbnk_stocks = {
    "ABCAPITAL", "ANGELONE", "BSE", "BAJFINANCE", "BAJAJFINSV",
    "CDSL", "CHOLAFIN", "CAMS", "HDFCAMC", "HDFCLIFE",
    "ICICIGI", "ICICIPRULI", "IEX", "IRFC", "JIOFIN",
    "LTF", "LICHSGFIN", "LICI", "M&MFIN", "MFSL",
    "MCX", "MUTHOOTFIN", "PAYTM", "POLICYBZR", "PEL",
    "PFC", "RECLTD", "SBICARD", "SBILIFE", "SHRIRAMFIN"
}

def get_all_index_data():
    # Dictionary to store stocks for each index
    index_stocks = {}
    
    # Dictionary to store stock-to-index mapping
    stock_indices = {}

    # Process predefined indices
    index_stocks["NIFTY CHEMICALS"] = list(nifty_chemicals_stocks)
    index_stocks["NIFTY FINSRV25 50"] = list(nifty_finsrv25_50_stocks)
    index_stocks["NIFTY FINSEREXBNK"] = list(nifty_finserexbnk_stocks)

    # Update stock_indices for predefined indices
    for stock in nifty_chemicals_stocks:
        stock_indices[stock] = stock_indices.get(stock, []) + ["NIFTY CHEMICALS"]
    for stock in nifty_finsrv25_50_stocks:
        stock_indices[stock] = stock_indices.get(stock, []) + ["NIFTY FINSRV25 50"]
    for stock in nifty_finserexbnk_stocks:
        stock_indices[stock] = stock_indices.get(stock, []) + ["NIFTY FINSEREXBNK"]

    # Fetch data for other indices
    for index in nse_indices:
        if index in ["NIFTY CHEMICALS", "NIFTY FINSRV25 50", "NIFTY FINSEREXBNK"]:
            continue
        try:
            time.sleep(10)  # Prevent rate limiting
            stocks = nse.get_stocks_in_index(index)
            time.sleep(10)  # Prevent rate limiting
            if not stocks:
                print(f"Skipping {index}: No data received.")
                continue
            index_stocks[index] = list(stocks)
            
            # Update stock_indices mapping
            for stock in stocks:
                stock_indices[stock] = stock_indices.get(stock, []) + [index]
            
            
        except Exception as e:
            print(f"Error fetching data for {index}: {e}")

    return index_stocks, stock_indices

def save_to_csv(index_stocks, stock_indices):
    # Save indices and their stocks to Excel with multiple sheets
    with ExcelWriter('indices_stocks.xlsx') as writer:
        for index, stocks in index_stocks.items():
            df = pd.DataFrame(stocks, columns=['Stock Symbol'])
            df.to_excel(writer, sheet_name=index, index=False)

    # Save stock-to-indices mapping to CSV
    # Create a DataFrame where columns are indices and values are 1/0
    all_stocks = sorted(stock_indices.keys())
    df_mapping = pd.DataFrame(index=all_stocks, columns=nse_indices)
    
    # Fill the DataFrame
    for stock in all_stocks:
        for index in nse_indices:
            df_mapping.loc[stock, index] = 1 if index in stock_indices[stock] else 0
    
    # Rename index column to 'Stock Symbol'
    df_mapping.index.name = 'Stock Symbol'
    df_mapping.to_csv('stock_indices_mapping.csv')

def main():
    print("Fetching data for all indices...")
    index_stocks, stock_indices = get_all_index_data()
    
    print("Saving data to CSV files...")
    save_to_csv(index_stocks, stock_indices)
    
    print("Data saved successfully!")
    print("1. indices_stocks.xlsx - Contains one sheet per index with its stocks")
    print("2. stock_indices_mapping.csv - Contains stocks and their index memberships (1/0)")

if __name__ == "__main__":
    main()