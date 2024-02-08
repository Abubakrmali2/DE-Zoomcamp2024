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

display(conn.sql("show tables"))


rides1 = conn.sql("Select sum(l.age) From people2 l LEFT JOIN people1 r on l.id=r.id ").df()
display(rides1)
