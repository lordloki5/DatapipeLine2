import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd

# Step 1: Scrape BSE Sectoral Indices and Their Constituent Stocks Using Selenium

# Set up Selenium with ChromeDriver
# Replace the path below with the path to your chromedriver executable if it's not in your PATH
driver = webdriver.Chrome()

# URL of the BSE Indices page
bse_url = "https://m.bseindia.com/IndicesView.aspx"

# Load the main page
driver.get(bse_url)
time.sleep(3)  # Wait for the page to load (adjust as needed)

# Parse the page with BeautifulSoup
soup = BeautifulSoup(driver.page_source, "html.parser")

# Find the "Sectoral" header to locate the sectoral indices section
sectoral_header = soup.find("td", class_="indexsubheader", string="Sectoral")

if not sectoral_header:
    print("Could not find the 'Sectoral' header on the page.")
    driver.quit()
    exit()

# Get the parent table of the sectoral header
table = sectoral_header.find_parent("table")

if not table:
    print("Could not find the parent table of the 'Sectoral' header.")
    driver.quit()
    exit()

# Find all <tr> elements after the sectoral header
rows = table.find_all("tr")
sectoral_header_row = sectoral_header.find_parent("tr")
sectoral_header_index = rows.index(sectoral_header_row)

# Extract sectoral indices and their corresponding <a> elements
index_elements = []
for row in rows[sectoral_header_index + 1:]:  # Start from the row after the sectoral header
    left_cell = row.find("td", class_="TTRow_left")
    if left_cell and "BSE" in left_cell.text:
        index_name = left_cell.text.strip()
        link_element = left_cell.find("a")
        if link_element:
            index_elements.append((index_name, link_element))

print("BSE Sectoral Indices Found:", [index_name for index_name, _ in index_elements])

# Scrape constituent stock symbols for each index
stock_data = {}

for index_name, link_element in index_elements:
    try:
        # Find the link element in the current page's DOM and click it
        link = driver.find_element(By.XPATH, f"//a[text()='{index_name}']")
        link.click()
        time.sleep(3)  # Wait for the detailed page to load (adjust as needed)

        # Parse the detailed page with BeautifulSoup
        detailed_soup = BeautifulSoup(driver.page_source, "html.parser")

        # Find the table containing the constituent stocks
        # Look for a table that has a header row with "Security" as a column
        tables = detailed_soup.find_all("table")
        stock_table = None
        for table in tables:
            header_row = table.find("tr")
            if header_row:
                headers = [th.text.strip() for th in header_row.find_all("th")]
                if "Security" in headers:
                    stock_table = table
                    break

        if not stock_table:
            print(f"No stock table found for {index_name}")
            driver.back()  # Go back to the main page
            time.sleep(2)  # Wait for the main page to reload
            continue

        stock_symbols = []
        # Find the header row to locate the "Security" column
        header_row = stock_table.find("tr")
        headers = [th.text.strip() for th in header_row.find_all("th")]
        security_col_index = headers.index("Security") if "Security" in headers else None

        if security_col_index is None:
            print(f"No 'Security' column found in the table for {index_name}")
            driver.back()
            time.sleep(2)
            continue

        # Extract stock symbols from rows after the header
        for row in stock_table.find_all("tr")[1:]:  # Skip header row
            cells = row.find_all("td")
            if len(cells) > security_col_index:
                symbol_cell = cells[security_col_index]  # The "Security" column
                if symbol_cell and symbol_cell.text.strip():
                    stock_symbols.append(symbol_cell.text.strip())

        stock_data[index_name] = stock_symbols
        print(f"Symbols for {index_name}: {stock_symbols[:5]}...")  # Print first 5 for brevity

        # Go back to the main page to process the next index
        driver.back()
        time.sleep(2)  # Wait for the main page to reload

    except Exception as e:
        print(f"Error processing {index_name}: {e}")
        driver.back()
        time.sleep(2)
        continue

# Close the browser
driver.quit()

# Step 2: Map BSE Sectoral Indices to NIFTY Sectoral Indices
# Manual mapping based on sector names
bse_to_nifty_mapping = {
    "BSE AUTO": "NIFTY AUTO",
    "BSE BANKEX": "NIFTY BANK",
    "BSE CONSUMER DURABLES": "NIFTY CONSUMER DURABLES",
    "BSE CAPITAL GOODS": "NIFTY INFRASTRUCTURE",
    "BSE COMMODITIES": "NIFTY COMMODITIES",
    "BSE CONSUMER DISCRETIONARY": "NIFTY CONSUMPTION",
    "BSE ENERGY": "NIFTY ENERGY",
    "BSE FINANCIALS": "NIFTY FINANCIAL SERVICES",
    "BSE INDUSTRIALS": "NIFTY INDUSTRIAL",
    "BSE TELECOMMUNICATION": "NIFTY TELECOM",
    "BSE UTILITIES": "NIFTY INFRASTRUCTURE",
    "BSE FAST MOVING CONSUMER GOODS": "NIFTY FMCG",
    "BSE HEALTHCARE": "NIFTY PHARMA",  # Could also be NIFTY HEALTHCARE
    "BSE INFORMATION TECHNOLOGY": "NIFTY IT",
    "BSE METAL": "NIFTY METAL",
    "BSE OIL & GAS": "NIFTY ENERGY",  # NIFTY doesn't have a direct Oil & Gas index; mapping to Energy
    "BSE POWER": "NIFTY INFRASTRUCTURE"  # Mapping to Infrastructure as a close match
}

# Print the mapping
print("\nBSE to NIFTY Mapping:")
for bse_index, nifty_index in bse_to_nifty_mapping.items():
    print(f"{bse_index} -> {nifty_index}")

# Step 3: Find the Market Leader for Each BSE Sectoral Index

# Load the market capitalization file
try:
    market_cap_df = pd.read_csv("marketcap.csv")
    # Drop any empty columns
    market_cap_df = market_cap_df.loc[:, ~market_cap_df.columns.str.contains('^Unnamed')]
    # Rename 'symbol' column to 'Symbol' for consistency
    market_cap_df = market_cap_df.rename(columns={"symbol": "Symbol"})
    print("\nMarket Capitalization Data Loaded:")
    print(market_cap_df.head())
except FileNotFoundError:
    print("Error: marketcap.csv file not found.")
    exit()
except pd.errors.EmptyDataError:
    print("Error: marketcap.csv file is empty.")
    exit()

# Find the market leader for each index
market_leaders = {}

for index_name, symbols in stock_data.items():
    # Filter market cap data for symbols in this index
    index_stocks_df = market_cap_df[market_cap_df["Symbol"].isin(symbols)]
    
    # Drop rows with NaN market capitalization values
    index_stocks_df = index_stocks_df.dropna(subset=["Market Capitalization (in Crores)"])
    
    if not index_stocks_df.empty:
        # Find the stock with the highest market cap
        leader = index_stocks_df.loc[index_stocks_df["Market Capitalization (in Crores)"].idxmax()]
        market_leaders[index_name] = {
            "Symbol": leader["Symbol"],
            "Market Cap (Crores)": leader["Market Capitalization (in Crores)"]
        }
    else:
        market_leaders[index_name] = {
            "Symbol": "N/A",
            "Market Cap (Crores)": "N/A"
        }
        print(f"No market cap data found for symbols in {index_name}")

# Print the market leaders
print("\nMarket Leaders for Each BSE Sectoral Index:")
for index_name, leader_info in market_leaders.items():
    print(f"{index_name}: {leader_info['Symbol']} (Market Cap: {leader_info['Market Cap (Crores)']})")

# Step 4: Save Results to a File
# Save the mapping and market leaders to a CSV file
results = []
for bse_index in stock_data.keys():
    result = {
        "BSE Index": bse_index,
        "Market Leader Symbol": market_leaders.get(bse_index, {}).get("Symbol", "N/A"),
        "Market Cap (Crores)": market_leaders.get(bse_index, {}).get("Market Cap (Crores)", "N/A")
    }
    results.append(result)

results_df = pd.DataFrame(results)
results_df.to_csv("bse_sectoral_analysis.csv", index=False)
print("\nResults saved to bse_sectoral_analysis.csv")