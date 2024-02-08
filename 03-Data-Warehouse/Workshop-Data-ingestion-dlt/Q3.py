
#Install the dependencies
#%%capture
#!pip install dlt[duckdb]

import dlt
import duckdb
from IPython.display import display



def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}




def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}





generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')

info= generators_pipeline.run (people_1(),
										table_name="people",
										write_disposition="replace")

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")



info= generators_pipeline.run (people_2(),
										table_name="people",
										write_disposition="append")

print("\n\n\n people table after appending below:\n\n\n")
rides = conn.sql("SELECT * FROM people").df()
display(rides)

ages = conn.sql("SELECT sum(Age) FROM people")
print(f'\n\n\nsum Ages after append:\n\n\n {ages}')
