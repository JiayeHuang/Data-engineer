#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os, argparse
from sqlalchemy import create_engine
from time import time
import logging

def main(params):
    url = params.url
    postgres_user = params.user
    postgres_pass = params.password
    postgres_host = params.host
    postgres_port = params.port
    postgres_db = params.db

    # Download and unzip files
    filename = url.rstrip("/").split("/")[-1].strip()
    os.system(f"wget {url} -O {filename}")

    if ".gz" in filename:
        os.system(f"gzip -d -f {filename}")
    elif ".csv" in filename:
        pass
    else:
        logging.error(f"Uncognized file type {filename}")

    # Create SQL engine
    engine = create_engine(f'postgresql://{postgres_user}:{postgres_pass}@{postgres_host}:{postgres_port}/{postgres_db}')

    # Create table in PostgreSQL
    df = pd.read_csv(filename.replace(".gz", ""), nrows=10)
    df.head(0).to_sql(name=filename.replace(".gz", "").replace(".csv", ""), con=engine, if_exists="replace")

    # Insert values
    df_iter = pd.read_csv(filename.replace(".gz", ""), iterator=True, chunksize=100000)
    t_start = time()
    count = 0
    for batch_df in df_iter:
        count += 1

        b_start = time()
        if "lpep_pickup_datetime" in batch_df.columns:
            batch_df.lpep_pickup_datetime = pd.to_datetime(batch_df.lpep_pickup_datetime)
        if "lpep_dropoff_datetime" in batch_df.columns:
            batch_df.lpep_dropoff_datetime = pd.to_datetime(batch_df.lpep_dropoff_datetime)
        batch_df.to_sql(name=filename.replace(".gz", "").replace(".csv", ""), con=engine, if_exists="append")
        b_end = time()

        print(f'inserted {count} batch! time taken {b_end-b_start:10.3f} seconds.\n')
        
    t_end = time()   
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')

    # Clean-up files
    os.remove(filename.replace(".gz", ""))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download gz/csv files from provided url, load the data to pstgresql database')
    parser.add_argument('--url', help='url to download files')
    parser.add_argument('--user', help='postgres username to connect')
    parser.add_argument('--password', help='postgres password to connect')
    parser.add_argument('--host', help='host to connect postgres')    
    parser.add_argument('--port', help='port to connect postgres')
    parser.add_argument('--db', help='postgres databse to load the data in')            

    args = parser.parse_args()

    main(args)
