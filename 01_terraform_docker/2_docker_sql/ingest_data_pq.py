#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pyarrow.parquet as pq
from time import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, DateTime, Float 
from clean_parquet import clean_parquet

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = '/app/downloaded_output.parquet'

    #check if parquet file already exists
    if os.path.exists(parquet_name):
        os.remove(parquet_name)
        print(f"{parquet_name} exists, deleted existing file")

    #download the parquet file
    os.system(f"wget {url} -O {parquet_name}")
    t_start = time()

    # We need to tell Pandas that we need to put this into postgres but we need to create a connection to postgres first
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #load the parquet file for cleaning
    #raw_data_parquet = pq.ParquetFile(parquet_name) 
    cleaned_data_parquet = clean_parquet(parquet_name) 

    #Read in the cleaned_data_parquet
    ny_data_parquet = pq.ParquetFile(cleaned_data_parquet)
    column_types = {'VendorID':Integer,
    'lpep_pickup_datetime':DateTime,
    'lpep_dropoff_datetime':DateTime,
    'store_and_fwd_flag':Text,
    'RatecodeID':Float,
    'PULocationID':Integer,
    'DOLocationID':Integer,
    'passenger_count':Float,
    'trip_distance':Float,
    'fare_amount':Float,
    'extra':Float,
    'mta_tax':Float,
    'tip_amount':Float,
    'tolls_amount':Float,
    'ehail_fee':Text,
    'improvement_surcharge':Float,
    'total_amount':Float,
    'payment_type':Float,
    'trip_type':Float,
    'congestion_surcharge':Float}

    # Create the table schema (only once, outside the loop)
    ny_data = ny_data_parquet.read().to_pandas()
    ny_data.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace',dtype=column_types, index=False)

    for batch in ny_data_parquet.iter_batches(batch_size=80000):
        ny_data = batch.to_pandas()
        ny_data.to_sql(name=table_name, con=engine, if_exists='append',index=False,dtype=column_types)
        print(f"batch{batch} loaded , rows:{len(ny_data)} added to record_batches")
    t_end = time()
    total_time = (t_end - t_start)/60
    print(f"total time taken: {total_time} minutes")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user',help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db',help='database name for postgres')
    parser.add_argument('--table_name',help='name of the table where we will write results')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()
    main(args)

# So in this exercise we have ingested the parquet data into the database, 
# - Key takeaways:
#     - we ran postgres using docker
#     - We connected to this database
#     - We then ingested the data into the postgres-docker database
# 
