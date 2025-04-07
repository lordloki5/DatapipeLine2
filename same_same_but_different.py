import pandas as pd

# Read the Excel files (replace 'file1.xlsx' and 'file2.xlsx' with your actual file paths)
file1_path = 'stock_indices_mapping.xlsx'
file2_path = 'Universe-with_sector_indices.xlsx'

# Read first column from both files
# Assuming the first column is named 'Column1' - adjust if your column has a different name
df1 = pd.read_excel(file1_path, usecols=[0])
df2 = pd.read_excel(file2_path, usecols=[0])

# Get the column names (in case they're different)
col1_name = df1.columns[0]
col2_name = df2.columns[0]

# Convert to sets for comparison
set1 = set(df1[col1_name].dropna())
set2 = set(df2[col2_name].dropna())

# Find common and different values
common_values = set1.intersection(set2)
diff_values_file1 = set1 - set2  # Values only in File1
diff_values_file2 = set2 - set1  # Values only in File2

# Create DataFrame for common values
common_df = pd.DataFrame(list(common_values), columns=['Common_Values'])

# Create DataFrame for different values with source information
diff_data = []
for value in diff_values_file1:
    diff_data.append([value, file1_path])
for value in diff_values_file2:
    diff_data.append([value, file2_path])

diff_df = pd.DataFrame(diff_data, columns=['Value', 'Source_File'])

# Sort the DataFrames for better readability
common_df = common_df.sort_values(by='Common_Values')
diff_df = diff_df.sort_values(by='Value')

# Save to Excel files
common_df.to_excel('common_values.xlsx', index=False)
diff_df.to_excel('different_values.xlsx', index=False)

print("Files generated successfully:")
print("- 'common_values.xlsx' contains values present in both files")
print("- 'different_values.xlsx' contains unique values with their source file")
print(f"Number of common values: {len(common_values)}")
print(f"Number of values unique to File1: {len(diff_values_file1)}")
print(f"Number of values unique to File2: {len(diff_values_file2)}")