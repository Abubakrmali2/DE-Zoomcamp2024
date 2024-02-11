import requests
import json
import dlt
import duckdb
import pandas as pd

def download_parquet():
  i=1
  for i in range(1, 13) :
    if i < 10:
      url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-0{i}.parquet"
      print(url)
    else:
      url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{i}.parquet"
      print(url)
    df = pd.read_parquet(url)
    
    yield df # response.content

  
  
#generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')
# we can load any generator to a table at the pipeline destnation as follows:
#info = generators_pipeline.run(download_parquet(),
#										table_name="Green_taxi_data1",
#                    write_disposition="replace",          
#										loader_file_format="parquet")


pipeline = dlt.pipeline(
    pipeline_name='pipeline_1',
    destination='filesystem',
    dataset_name='your_dataset_name_test'
)

info1 = pipeline.run(download_parquet(), loader_file_format="parquet")

# the outcome metadata is returned by the load and we can inspect it by printing it.
#print(info)

#conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
#conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
#print('Loaded tables: ')
#display(conn.sql("show tables"))
#display(conn.sql("select * from Green_taxi_data1 limit 20"))
#display(conn.sql("select count(*) from Green_taxi_data1 "))
#display(conn.sql("DESCRIBE Green_taxi_data "))
