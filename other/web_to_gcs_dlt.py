import requests
import dlt
import duckdb
import pandas as pd

init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

def download_parquet(year,service):
  i=1
  for i in range(12) :
    month = '0'+str(i+1)
    month = month[-2:]

    file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
    request_url = f"{init_url}{service}/{file_name}"

    #if i < 10:
    #  url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-0{i}.parquet"
    #  print(url)
    #else:
    #  url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{i}.parquet"
    #  print(url)
    df = pd.read_csv(request_url,compression='gzip')
    file_name = file_name.replace('.csv.gz', '.parquet')
    df.to_parquet(file_name, engine='pyarrow')
    print(f"Parquet: {file_name}")
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
    dataset_name='your_dataset_name_dbt'
)

info1 = pipeline.run(download_parquet('2019','green'), loader_file_format="parquet")



#web_to_gcs('2019', 'green')
#web_to_gcs('2020', 'green')
#web_to_gcs('2019', 'yellow')
#web_to_gcs('2020', 'yellow')


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
