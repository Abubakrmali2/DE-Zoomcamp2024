CREATE OR REPLACE EXTERNAL TABLE `mydezoomcamp-project.ny_taxi.greent_taxi_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mydezoomcamp-project-green-bucket/nyc_green_taxi_data/*.parquet']
);


CREATE OR REPLACE TABLE `mydezoomcamp-project.ny_taxi.greent_taxi`
AS SELECT * FROM `mydezoomcamp-project.ny_taxi.greent_taxi_external`;





SELECT count(*) FROM `mydezoomcamp-project.ny_taxi.greent_taxi_external`;


SELECT COUNT(DISTINCT(PULocationID)) FROM `mydezoomcamp-project.ny_taxi.greent_taxi_external`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `mydezoomcamp-project.ny_taxi.greent_taxi`;


select count (fare_amount) FROM `mydezoomcamp-project.ny_taxi.greent_taxi_external` where fare_amount=0 ;



CREATE OR REPLACE TABLE `mydezoomcamp-project.ny_taxi.greent_taxi_partitioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `mydezoomcamp-project.ny_taxi.greent_taxi_external`
);




SELECT distinct(PULocationID) FROM  `mydezoomcamp-project.ny_taxi.greent_taxi_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '06/01/2022' AND '06/30/2022';

SELECT distinct(PULocationID) FROM  `mydezoomcamp-project.ny_taxi.greent_taxi`
WHERE DATE(lpep_pickup_datetime) BETWEEN '06/01/2022' AND '06/30/2022';





SELECT count(*)  FROM  `mydezoomcamp-project.ny_taxi.greent_taxi`;
