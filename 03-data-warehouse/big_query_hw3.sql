--Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-411.d_engr_zoomcamp_hw3.external_yellow_trip2024`
OPTIONS (
  format = 'parquet',
  uris = ['gs://d_engr_hw3_bucket/yellow_tripdata_2024-*.parquet']
);

-- Create regular/materialized table from 
CREATE OR REPLACE TABLE `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_2024trips`
AS
SELECT *
FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.external_yellow_trip2024`;

--Count Distinct PULocationID from external table
SELECT COUNT(DISTINCT(PULocationID)) FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.external_yellow_trip2024`;

--Select PULocationID from materialized table
SELECT PULocationID FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_2024trips`;

--Select PULocationID and DOLocationID from materialized table
SELECT PULocationID, DOLocationID FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_2024trips`;

--Count number of records with fare amount of zero
SELECT COUNT(fare_amount) FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_2024trips`
WHERE fare_amount = 0;

--Create an optimized table 
CREATE OR REPLACE TABLE kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_2024optimized
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM kestra-sandbox-411.d_engr_zoomcamp_hw3.external_yellow_trip2024; 

--Select distinct VedorIDs from materialized table
SELECT COUNT(DISTINCT(VendorID)) 
FROM kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_2024trips
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

--Select distinct VendorIDs from optimized table
SELECT COUNT(DISTINCT(VendorID)) 
FROM kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_2024optimized
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
