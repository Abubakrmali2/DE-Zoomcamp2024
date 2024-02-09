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
										table_name="people1",
										write_disposition="replace")

info= generators_pipeline.run (people_2(),
										table_name="people2",
										write_disposition="replace")

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")


conn.sql(" CREATE or replace TABLE people_merge (id INTEGER PRIMARY KEY, Name VARCHAR, age INTEGER, city VARCHAR) ")
display(conn.sql("show tables"))


conn.sql('INSERT INTO people_merge (id, name, age, city) SELECT id, name, age, city FROM people1')
conn.sql("INSERT INTO people_merge (id, name, age, city) SELECT id, name, age, city FROM people2 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name,age = EXCLUDED.age,city = EXCLUDED.city")


rides= conn.sql("select * from people_merge").df()
display(rides)

rides= conn.sql("select sum(age) from people_merge").df()
display(rides)