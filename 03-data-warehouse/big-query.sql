--Query public available table
SELECT station_id, name FROM
bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

--Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-411.dataengr_zoomcamp_892.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-de-zcamp-bucket/yellow_tripdata_2019-*.csv','gs://kestra-de-zcamp-bucket/yellow_tripdata_2020-*.csv','gs://kestra-de-zcamp-bucket/yellow_tripdata_2021-*.csv']
);

--Now lets check the external yellow trip-data table created
SELECT * FROM kestra-sandbox-411.dataengr_zoomcamp_892.external_yellow_tripdata limit 10;

--Create a NON-partitioned table from external table
CREATE OR REPLACE TABLE kestra-sandbox-411.dataengr_zoomcamp_892.yellow_tripdata_non_partitioned AS
SELECT * FROM kestra-sandbox-411.dataengr_zoomcamp_892.external_yellow_tripdata;

--Create a partitioned table from external table
CREATE OR REPLACE TABLE kestra-sandbox-411.dataengr_zoomcamp_892.yellow_tripdata_partitioned
PARTITION BY DATE(tpep_pickup_datetime) AS (
SELECT *
FROM kestra-sandbox-411.dataengr_zoomcamp_892.external_yellow_tripdata
WHERE tpep_pickup_datetime IS NOT NULL);

-- Impact of partitions
-- Processed 1.83GB Data
SELECT DISTINCT(VendorID)
FROM kestra-sandbox-411.dataengr_zoomcamp_892.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Processed 105.91MB data
SELECT DISTINCT(VendorID)
FROM kestra-sandbox-411.dataengr_zoomcamp_892.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

--Look into the Partitions
SELECT table_name, partition_id, total_rows
FROM dataengr_zoomcamp_892.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

--Creating a Partition and Cluster Table
CREATE OR REPLACE TABLE kestra-sandbox-411.dataengr_zoomcamp_892.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
cluster by VendorID AS
SELECT * FROM kestra-sandbox-411.dataengr_zoomcamp_892.external_yellow_tripdata;

--Assessing performance between partitione & - processed 1.06GB
select count(*) as trips
from kestra-sandbox-411.dataengr_zoomcamp_892.yellow_tripdata_partitioned
where DATE(tpep_pickup_datetime) between '2019-06-01' and '2020-12-31'
and VendorID=1;

--partitioned & clustered table - processed 825MB
select count(*) as trips
from kestra-sandbox-411.dataengr_zoomcamp_892.yellow_tripdata_partitioned_clustered
where DATE(tpep_pickup_datetime) between '2019-06-01' and '2020-12-31'
and VendorID=1;