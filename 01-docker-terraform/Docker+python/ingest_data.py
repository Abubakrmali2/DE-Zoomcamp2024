#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url=params.url
    csv_name='output.csv.gz'
    csv_name1="output1.csv"

    os.system(f"wget {url} -O {csv_name}")
    os.system(f"gunzip -c {csv_name} > {csv_name1}")


    engine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter=pd.read_csv(csv_name1, iterator=True, chunksize=100000)
    df=next(df_iter)

    df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)


    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')



    while True:
        tstart=time()
        df=next(df_iter)
        df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        tend=time()
        print('Import another chunk......, it took %.3f seconds'%(tend-tstart))





if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='ingest CSV data to Postgres')
    # user, password, host, port, dbname, tablename
    # URL of Csv
    parser.add_argument('--user',help='postgres user name')
    parser.add_argument('--password',help='postgres password')
    parser.add_argument('--host',help='DB host name')
    parser.add_argument('--port',help='DB port')
    parser.add_argument('--db',help='Database name')
    parser.add_argument('--table_name',help='table name')
    parser.add_argument('--url',help='url of the CSV')

    args = parser.parse_args()

    main(args)






