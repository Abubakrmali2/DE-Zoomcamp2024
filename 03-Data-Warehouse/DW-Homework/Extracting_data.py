import io
import pandas as pd
import pyarrow.parquet as pq
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader

def load_data_from_api(*args, **kwargs):
    i=1
    combined_df=pd.DataFrame()
     
    for i in range(1, 13) :
        
        
        if i < 10:
            url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-0{i}.parquet"
            local_path = f"green_tripdata_2022-0{i}.parquet"

      
        else:
            url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{i}.parquet"
            local_path = f"green_tripdata_2022-{i}.parquet"
      
    
    
        #response = requests.get(url)
        #response= pq.read_table(url)
        #local_path = f"green_tripdata_2022-{i}.parquet"
        response = requests.get(url)
        with open(local_path, 'wb') as file:
            file.write(response.content)

        # Read the local Parquet file using pyarrow
        table = pq.read_table(local_path)

        # Convert the table to a pandas DataFrame
        df = table.to_pandas()

        combined_df = combined_df.append(df, ignore_index=True)


    #return pd.read_csv(io.StringIO(response.text), sep=',')
    return combined_df #response.to_pandas()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
