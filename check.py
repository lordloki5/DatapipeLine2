import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin

# Step 1: Scrape BSE Sectoral Indices and Their Constituent Stocks

# Headers to mimic a browser request
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# URL of the BSE Indices page
bse_url = "https://m.bseindia.com/IndicesView.aspx"

# Fetch the page content
try:
    response = requests.get(bse_url, headers=headers)
    response.raise_for_status()  # Check for HTTP errors
except requests.exceptions.RequestException as e:
    print(f"Error fetching the BSE page: {e}")
    exit()

# Parse the page with BeautifulSoup
soup = BeautifulSoup(response.content, "html.parser")

# Find the "Sectoral" header to locate the sectoral indices section
sectoral_header = soup.find("td", class_="indexsubheader", string="Sectoral")

if not sectoral_header:
    print("Could not find the 'Sectoral' header on the page.")
    exit()

# Get the parent table of the sectoral header
table = sectoral_header.find_parent("table")

if not table:
    print("Could not find the parent table of the 'Sectoral' header.")
    exit()

# Find all <tr> elements after the sectoral header
# We need to locate the <tr> that contains the sectoral header and then get the subsequent <tr> elements
rows = table.find_all("tr")
sectoral_header_row = sectoral_header.find_parent("tr")
sectoral_header_index = rows.index(sectoral_header_row)

# Extract sectoral indices from the rows after the sectoral header
index_links = {}
for row in rows[sectoral_header_index + 1:]:  # Start from the row after the sectoral header
    left_cell = row.find("td", class_="TTRow_left")
    if left_cell and "BSE" in left_cell.text:
        index_name = left_cell.text.strip()
        link = left_cell.find("a")
        if link and "href" in link.attrs:
            index_links[index_name] = urljoin(bse_url, link["href"])  # Construct full URL

print("BSE Sectoral Indices Found:", list(index_links.keys()))

# Scrape constituent stocks for each index
stock_data = {}

for index_name, link in index_links.items():
    try:
        response = requests.get(link, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page for {index_name}: {e}")
        continue

    soup = BeautifulSoup(response.content, "html.parser")
    # Find the table containing the constituent stocks
    # Note: The exact table structure may vary; inspect the page to confirm
    stock_table = soup.find("table")  # Adjust based on actual HTML structure

    if not stock_table:
        print(f"No stock table found for {index_name}")
        continue

    stocks = []
    for row in stock_table.find_all("tr")[1:]:  # Skip header row
        cells = row.find_all("td")
        if len(cells) > 0:
            stock_name = cells[0].text.strip()  # Adjust column index as needed
            if stock_name:
                stocks.append(stock_name)

    stock_data[index_name] = stocks
    print(f"Stocks for {index_name}: {stocks[:5]}...")  # Print first 5 for brevity




# Step 2: Find the Market Leader for Each BSE Sectoral Index

# Load the market capitalization file
try:
    market_cap_df = pd.read_csv("marketcap.csv")
    print("\nMarket Capitalization Data Loaded:")
    print(market_cap_df.head())
except FileNotFoundError:
    print("Error: market_capitalization.csv file not found.")
    exit()
except pd.errors.EmptyDataError:
    print("Error: market_capitalization.csv file is empty.")
    exit()

# Find the market leader for each index
market_leaders = {}

for index_name, stocks in stock_data.items():
    # Filter market cap data for stocks in this index
    index_stocks_df = market_cap_df[market_cap_df["symbol"].isin(stocks)]
    
    if not index_stocks_df.empty:
        # Find the stock with the highest market cap
        leader = index_stocks_df.loc[index_stocks_df["Market Capitalization (in Crores)"].idxmax()]
        market_leaders[index_name] = {
            "Stock": leader["symbol"],
            "Market Cap (Crores)": leader["Market Capitalization (in Crores)"]
        }
    else:
        market_leaders[index_name] = {
            "Stock": "N/A",
            "Market Cap (Crores)": "N/A"
        }
        print(f"No market cap data found for stocks in {index_name}")

# Print the market leaders
print("\nMarket Leaders for Each BSE Sectoral Index:")
for index_name, leader_info in market_leaders.items():
    print(f"{index_name}: {leader_info['Stock']} (Market Cap: {leader_info['Market Cap (Crores)']})")

# Step 4: Save Results to a File
# Save the mapping and market leaders to a CSV file
results = []
for bse_index in stock_data.keys():
    result = {
        "BSE Index": bse_index,
        # "NIFTY Index": bse_to_nifty_mapping.get(bse_index, "N/A"),
        "Market Leader": market_leaders.get(bse_index, {}).get("Stock", "N/A"),
        # "Market Cap (Crores)": market_leaders.get(bse_index, {}).get("Market Cap (Crores)", "N/A")
    }
    results.append(result)

results_df = pd.DataFrame(results)
results_df.to_csv("bse_sectoral_analysis.csv", index=False)
print("\nResults saved to bse_sectoral_analysis.csv")