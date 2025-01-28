import pandas as pd
import pyarrow.parquet as pq
import os

def clean_parquet(raw_data_parquet,
                  remove_nulls=True,
                  remove_invalids=True,
                  null_parts = 10,
                  path_to_cleaned_parquet = "/ny_taxi_postgres_data/cleaned_green_taxi_data.parquet"
):
    """This function cleans a raw data parquet file of invalid and null data and returns a cleaned dataframe
    Usage: clean_parquet(raw_data, neg_fare_to_positive=True, remove_negative_values=True)
    Args:
        raw_data: The raw data parquet to be cleaned
        remove_nulls: A boolean value to remove null values from the data
        null_parts: This is estimated using raw_data_df.isnull().sum(). This value is used to divide the number of rows
        in the dataframe to determine the threshold for removing rows with null values
    Returns:
        A cleaned dataframe written to parquet file"""
    
    raw_data_df = pq.read_table(raw_data_parquet).to_pandas()
    
    print("dropping columns with more than 50% nulls)")
    if remove_nulls:
        try:
       
            #drop columns with >50% nulls
            fifty_pct_threshold = len(raw_data_df)/2
            columns_to_drop = raw_data_df.columns[raw_data_df.isnull().sum() > fifty_pct_threshold]
            raw_data_df = raw_data_df.drop(columns=columns_to_drop)
            initial_rows = len(raw_data_df)
            print(f"dropped {columns_to_drop} column")
            print(f"cleaning {initial_rows} rows of data")

            #drop rows with more nulls than threshold
            null_rows_threshold = len(raw_data_df)/null_parts
            columns_above_threshold = raw_data_df.columns[raw_data_df.isnull().sum() > null_rows_threshold]
            raw_data_df = raw_data_df.dropna(subset=columns_above_threshold)
            rows_dropped = initial_rows - len(raw_data_df)
            print(f"dropped {rows_dropped} null rows than threshold")
        except Exception as e:
            print(f"An expected error occurred during null removal: {e}")
            raise
     
    if remove_invalids:
        # drop rows with invalid values in RatecodeID column
        not_99_df = raw_data_df[raw_data_df['RatecodeID'] != 99]
        invalids_removed = len(raw_data_df) - len(not_99_df)
        print(f"dropped {invalids_removed} rows with invalid values in RatecodeID column")
    raw_data_df = not_99_df
    
    os.makedirs(os.path.dirname(path_to_cleaned_parquet), exist_ok=True)
    raw_data_df.to_parquet(path_to_cleaned_parquet)
    print(f"Raw Parquet data successfully cleaned giving {len(raw_data_df)} clean rows")

    return path_to_cleaned_parquet

