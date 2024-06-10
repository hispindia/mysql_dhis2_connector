import pandas as pd

# Load the Excel file
file_path = 'read_sample.xlsx'
data = pd.read_excel(file_path)

# Fetch data from specified columns and store in variables
column1 = data['Column1']
column2 = data['Column2']
column3 = data['Column3']
column4 = data['Column4']

# Print fetched data
print("Column1:")
print(column1)
print("\nColumn2:")
print(column2)
print("\nColumn3:")
print(column3)
print("\nColumn4:")
print(column4)
