from nsetools import Nse
import pandas as pd

# Function to fetch and display all NSE indices using nsetools
def get_nse_indices():
    # Initialize the NSE object
    nse = Nse()
    
    # Get the list of indices
    indices = nse.get_index_list()
    
    # Print the list of indices
    print("List of Indices Traded at NSE:")
    for i, index in enumerate(indices, 1):
        print(f"{i}. {index}")
    
    # Optionally, return the list as a pandas DataFrame for further use
    df = pd.DataFrame(indices, columns=["Index Name"])
    return df

# Execute the function
if __name__ == "__main__":
    indices_df = get_nse_indices()
    # Optionally save to a CSV file
    indices_df.to_csv("nse_indices_nsetools.csv", index=False)
    print("\nIndices have been saved to 'nse_indices_nsetools.csv'")