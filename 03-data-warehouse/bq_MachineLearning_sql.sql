--Select the columns you are interested in
SELECT passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tip_amount, tolls_amount
FROM kestra-sandbox-411.dataengr_zoomcamp_892.yellow_tripdata_partitioned
WHERE fare_amount != 0;

--Create an ML table with appropriate type
CREATE OR REPLACE TABLE kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_tripdataML(
  `passenger_count` INTEGER,
  `trip_distance` FLOAT64,
  `PULocationID` STRING,
  `DOLocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64
  ) AS (
    SELECT passenger_count, trip_distance, cast(PULocationID as STRING), cast(DOLocationID as STRING), cast(payment_type as STRING), fare_amount, tolls_amount, tip_amount
    FROM kestra-sandbox-411.dataengr_zoomcamp_892.yellow_tripdata_partitioned 
    WHERE fare_amount != 0
  ); 


  --Create Machine Learning Model with Default Setting
  CREATE OR REPLACE MODEL `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_tipmodel`
  OPTIONS (
    model_type = 'linear_reg',
    input_label_cols = ['tip_amount'],
    DATA_SPLIT_METHOD = 'AUTO_SPLIT') 
    AS(
      SELECT * FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_tripdataML`
      WHERE tip_amount IS NOT NULL
  );

--Check features of the Model
SELECT * FROM ML.FEATURE_INFO(MODEL `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_tipmodel`);

--Evaluate the Model
SELECT * FROM ML.EVALUATE(MODEL `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_tipmodel`,
(
  SELECT * FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_tripdataML`
  WHERE tip_amount IS NOT NULL
));

-- Predict the Model
SELECT * FROM ML.PREDICT(MODEL `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_tipmodel`,
(
  SELECT * FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_tripdataML`
  WHERE tip_amount IS NOT NULL
));

-- PREDICT & EXPLAIN
SELECT * FROM ML.EXPLAIN_PREDICT(MODEL `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_tipmodel`,
(
  SELECT * FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_tripdataML`
  WHERE tip_amount IS NOT NULL
),STRUCT(3 as top_k_features));

-- HYPER PARAM TUNING
CREATE OR REPLACE MODEL `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_taxi_tip_hyperparam_model`
OPTIONS(
  model_type='linear_reg',
  input_label_cols = ['tip_amount'],
  DATA_SPLIT_METHOD = 'AUTO_SPLIT',
  num_trials = 5,
  max_parallel_trials = 2,
  l1_reg=hparam_range(0,20),
  l2_reg=hparam_candidates([0,0.1,1,10]))
  AS
  SELECT * FROM `kestra-sandbox-411.d_engr_zoomcamp_hw3.yellow_tripdataML`
  WHERE tip_amount IS NOT NULL;
