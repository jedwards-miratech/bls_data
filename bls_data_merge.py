# optimized_join_from_bls_with_logging_export.py
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting BLS data processing script.")

# URLs or local paths for the BLS data files
url_series     = "data/oe.series"
url_current    = "data/oe.data.0.Current"
url_datatype   = "data/oe.datatype"
url_occupation = "data/oe.occupation"

# Load the datasets directly from the files/URLs
logging.info("Loading Series data from %s", url_series)
series_df = pd.read_csv(url_series, delimiter='\t', dtype=str)
logging.info("Series data loaded. Columns: %s", series_df.columns.tolist())

logging.info("Loading Current data from %s", url_current)
current_df = pd.read_csv(url_current, delimiter='\t', dtype=str)
logging.info("Current data loaded. Columns: %s", current_df.columns.tolist())

logging.info("Loading DataType data from %s", url_datatype)
datatype_df = pd.read_csv(url_datatype, delimiter='\t', dtype=str)
logging.info("DataType data loaded. Columns: %s", datatype_df.columns.tolist())

logging.info("Loading Occupation data from %s", url_occupation)
occupation_df = pd.read_csv(url_occupation, delimiter='\t', dtype=str)
logging.info("Occupation data loaded. Columns: %s", occupation_df.columns.tolist())

# Standardize column names: strip whitespace and convert to lowercase
logging.info("Standardizing column names: stripping whitespace and converting to lowercase.")
series_df.columns = series_df.columns.str.strip().str.lower()
current_df.columns = current_df.columns.str.strip().str.lower()
datatype_df.columns = datatype_df.columns.str.strip().str.lower()
occupation_df.columns = occupation_df.columns.str.strip().str.lower()

logging.info("Series columns after renaming: %s", series_df.columns.tolist())
logging.info("Current columns after renaming: %s", current_df.columns.tolist())
logging.info("DataType columns after renaming: %s", datatype_df.columns.tolist())
logging.info("Occupation columns after renaming: %s", occupation_df.columns.tolist())

# Debug: check for the expected 'value' column in current_df
if 'value' not in current_df.columns:
    logging.error("Expected column 'value' not found in Current data. Found columns: %s", current_df.columns.tolist())
    raise KeyError("The expected column 'value' was not found in current_df.")

# Convert the 'value' column to numeric
logging.info("Converting 'value' column to numeric.")
current_df["value"] = pd.to_numeric(current_df["value"], errors="coerce")

# 1. Merge Series with CurrentData on 'series_id'
logging.info("Merging Series and Current data on 'series_id'.")
merged_df = pd.merge(series_df, current_df, on='series_id', how='left')
logging.info("Merge complete. DataFrame shape: %s", merged_df.shape)

# 2. Merge with DataType on 'datatype_code'
logging.info("Merging with DataType on 'datatype_code'.")
merged_df = pd.merge(merged_df, datatype_df, on='datatype_code', how='left')
logging.info("Merge complete. DataFrame shape: %s", merged_df.shape)

# 3. Replace nulls in 'datatype_name' with "MISSING"
logging.info("Replacing nulls in 'datatype_name' with 'MISSING'.")
merged_df['datatype_name'] = merged_df['datatype_name'].fillna("MISSING")

# 4. Drop unneeded columns; keep occupation_code, area_code, and value
logging.info("Dropping unneeded columns: 'series_id' and 'datatype_code'.")
merged_df = merged_df.drop(columns=['series_id', 'datatype_code'])
logging.info("Remaining columns: %s", merged_df.columns.tolist())

# 5. Pivot so that each distinct datatype becomes its own column.
logging.info("Pivoting table to transform 'datatype_name' values into columns.")
pivot_df = merged_df.pivot_table(
    index=['occupation_code', 'area_code'],
    columns='datatype_name',
    values='value',
    aggfunc='first'
).reset_index()

# Flatten the pivoted columns (remove the pivot level name)
pivot_df.columns.name = None
logging.info("Pivot complete. DataFrame shape: %s", pivot_df.shape)

# 6. Merge with Occupation to add occupation_name and occupation_description
logging.info("Merging with Occupation data on 'occupation_code'.")
final_df = pd.merge(
    pivot_df,
    occupation_df[['occupation_code', 'occupation_name', 'occupation_description']],
    on='occupation_code',
    how='left'
)
logging.info("Final merge complete. Final DataFrame shape: %s", final_df.shape)

# 7. Export the final dataset to a CSV file
export_file = "final_dataset.csv"
logging.info("Exporting final dataset to file: %s", export_file)
final_df.to_csv(export_file, index=False)
logging.info("Export completed successfully.")

print(final_df.head())
