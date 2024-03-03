import requests
import dlt
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

def download_parquet(year,service):
  
  for i in range(12) :
    
    month = '0'+str(i+1)
    month = month[-2:]

    file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
    request_url = f"{init_url}{service}/{file_name}"

    taxi_dtypes = {
     'dispatching_base_num' : str,
     'PUlocationID' : 'Int64',
     'DOlocationID' : 'Int64',
     'Affiliated_base_number' : str,
     'SR_Flag' : str
    }
    #parse_date = ['lpep_pickup_datetime','lpep_dropoff_datetime']
    #dtype=taxi_dtypes, parse_dates=parse_date
    print(i)
    print(request_url)
    df = pd.read_csv(request_url,compression='gzip', dtype=taxi_dtypes)
    
    print(df.columns)
    print(df.dtypes)
    #float_rows = df[df['PUlocationID'].astype(str).str.contains('\.')]
    #print("Rows with Float Values in PUlocationID:")
    #print(float_rows)



    
    yield df # response.content


pipeline_dbt_fhv = dlt.pipeline(
    pipeline_name='pipeline_dbt_fhv',
    destination='filesystem',
    dataset_name='your_dataset_fhv_dbt'
)

info2 = pipeline_dbt_fhv.run(download_parquet('2019','fhv'), loader_file_format="parquet")


#web_to_gcs('2019', 'green')
#web_to_gcs('2020', 'green')
#web_to_gcs('2019', 'yellow')
#web_to_gcs('2020', 'yellow')

