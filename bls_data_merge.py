# bls_data_merge_filtered_local.py
import pandas as pd
import logging

# Set up logging to file and console
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("debug.log", mode="w"),
        logging.StreamHandler()
    ]
)

logging.info("Starting BLS data processing with filtering from local files.")

##############################################
# 1. Load and filter the OE.series file
##############################################
series_path = "data/oe.series"
logging.info("Loading series data from %s", series_path)
series_df = pd.read_csv(series_path, delimiter="\t", encoding="cp1252", dtype=str)
series_df.columns = series_df.columns.str.strip()  # clean column names
logging.info("Series data loaded. Columns: %s", series_df.columns.tolist())

# Filter rows: keep only those with areatype_code == "M"
series_df = series_df[series_df["areatype_code"] == "M"]
# Filter rows: keep only those with datatype_code in the desired list
series_df = series_df[series_df["datatype_code"].isin(["11", "12", "13", "14", "15"])]
# Drop unnecessary columns
drop_cols = ["footnote_codes", "begin_year", "begin_period", "end_period",
             "seasonal", "areatype_code", "industry_code", "state_code",
             "sector_code", "series_title"]
series_df = series_df.drop(columns=drop_cols, errors="ignore")
logging.info("Series data filtered. Shape: %s", series_df.shape)

##############################################
# 2. Load and filter the OE.datatype file
##############################################
datatype_path = "data/oe.datatype"
logging.info("Loading datatype data from %s", datatype_path)
datatype_df = pd.read_csv(datatype_path, delimiter="\t", encoding="cp1252", dtype=str)
datatype_df.columns = datatype_df.columns.str.strip()
# Keep only the specific wage measures
desired_datatypes = [
    "Annual 10th percentile wage",
    "Annual 25th percentile wage",
    "Annual 75th percentile wage",
    "Annual 90th percentile wage",
    "Annual median wage"
]
datatype_df = datatype_df[datatype_df["datatype_name"].isin(desired_datatypes)]
logging.info("Datatype data filtered. Shape: %s", datatype_df.shape)

##############################################
# 3. Load and process the OE.occupation file
##############################################
occupation_path = "data/oe.occupation"
logging.info("Loading occupation data from %s", occupation_path)
occupation_df = pd.read_csv(occupation_path, delimiter="\t", encoding="cp1252", dtype=str)
occupation_df.columns = occupation_df.columns.str.strip()
# Remove unwanted columns: "selectable", "display_level"
occupation_df = occupation_df.drop(columns=["selectable", "display_level"], errors="ignore")
logging.info("Occupation data processed. Shape: %s", occupation_df.shape)

##############################################
# 4. Load and process the OE.data.0.Current file
##############################################
current_path = "data/oe.data.0.Current"
logging.info("Loading current data from %s", current_path)
current_df = pd.read_csv(current_path, delimiter="\t", encoding="cp1252", dtype=str)
current_df.columns = current_df.columns.str.strip()
logging.info("Current data loaded. Columns: %s", current_df.columns.tolist())

# Replace "           -" with NA (using a regex to catch dashes with surrounding whitespace)
current_df.replace(r"^\s*-\s*$", pd.NA, regex=True, inplace=True)
# Drop columns that are not needed: "footnote_codes", "year", "period"
current_df = current_df.drop(columns=["footnote_codes", "year", "period"], errors="ignore")
# Convert the value column to numeric
current_df["value"] = pd.to_numeric(current_df["value"], errors="coerce")
logging.info("Current data processed. Shape: %s", current_df.shape)

##############################################
# 5. Merge and Pivot the Data
##############################################
# Merge series and current data on "series_id"
logging.info("Merging series and current data on 'series_id'.")
merged_df = pd.merge(series_df, current_df, on="series_id", how="left")
logging.info("After merging series and current: %s", merged_df.shape)

# Merge the above result with datatype data on "datatype_code"
logging.info("Merging with datatype data on 'datatype_code'.")
merged_df = pd.merge(merged_df, datatype_df, on="datatype_code", how="left")
logging.info("After merging with datatype: %s", merged_df.shape)

# Replace any missing datatype names with "MISSING"
merged_df["datatype_name"] = merged_df["datatype_name"].fillna("MISSING")

# Drop unneeded columns: remove "series_id" and "datatype_code"
merged_df = merged_df.drop(columns=["series_id", "datatype_code"], errors="ignore")
logging.info("Columns after dropping unneeded fields: %s", merged_df.columns.tolist())

# Pivot the table so that each distinct datatype becomes its own column
logging.info("Pivoting table so that each datatype becomes its own column.")
pivot_df = merged_df.pivot_table(
    index=["occupation_code", "area_code"],
    columns="datatype_name",
    values="value",
    aggfunc="first"
).reset_index()
pivot_df.columns.name = None  # remove pivot index name
logging.info("Pivot complete. Shape: %s", pivot_df.shape)

# Merge with occupation data to add occupation_name and occupation_description
logging.info("Merging pivoted data with occupation data on 'occupation_code'.")
final_df = pd.merge(
    pivot_df,
    occupation_df[["occupation_code", "occupation_name", "occupation_description"]],
    on="occupation_code",
    how="left"
)
logging.info("Final merge complete. Final DataFrame shape: %s", final_df.shape)

##############################################
# 6. Export the Final Dataset
##############################################
export_file = "final_dataset.csv"
logging.info("Exporting final dataset to %s", export_file)
final_df.to_csv(export_file, index=False)
logging.info("Export completed successfully.")

print(final_df.head())
