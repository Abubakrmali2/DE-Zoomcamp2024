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

    
    taxi_dtypes = {
    'VendorID' : pd.Int64Dtype(),		
    'store_and_fwd_flag' :str,	
    'RatecodeID' : pd.Int64Dtype(), 	
    'PULocationID' : pd.Int64Dtype(),	
    'DOLocationID' : pd.Int64Dtype(),	
    'passenger_count' : pd.Int64Dtype(),	
    'trip_distance' : float,	
    'fare_amount' : float,	
    'extra' : float,	
    'mta_tax' : float,	
    'tip_amount' : float,
    'tolls_amount' : float,		
    'improvement_surcharge' : float,	
    'total_amount' : float,	
    'payment_type' : pd.Int64Dtype(),	
    'trip_type' : pd.Int64Dtype(),	
    'congestion_surcharge' : float
    } 
    #parse_date = ['lpep_pickup_datetime','lpep_dropoff_datetime'] 
    #dtype=taxi_dtypes, parse_dates=parse_date

    df = pd.read_csv(request_url,compression='gzip',dtype=taxi_dtypes)
    #file_name = file_name.replace('.csv.gz', '.parquet')
    #df.to_parquet(file_name, engine='pyarrow')
    #print(f"Parquet: {file_name}")
    yield df # response.content

  
  

pipeline_dbt = dlt.pipeline(
    pipeline_name='pipeline_dbt',
    destination='filesystem',
    dataset_name='your_dataset_yellow_dbt'
)

info1 = pipeline_dbt.run(download_parquet('2019','yellow'), loader_file_format="parquet")




#web_to_gcs('2019', 'green')
#web_to_gcs('2020', 'green')
#web_to_gcs('2019', 'yellow')
#web_to_gcs('2020', 'yellow')

