/* @bruin
name: staging.trips
type: duckdb.sql
connection: duckdb-default

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "The date and time when the meter was engaged"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: double
    checks:
      # This check will now PASS because we filter negative values in the query below
      - name: non_negative 

custom_checks:
  - name: check_positive_passengers
    description: "Ensure we aren't processing trips with 0 passengers"
    query: |
      SELECT COUNT(*) FROM staging.trips WHERE passenger_count = 0
    value: 0

@bruin */

SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.payment_type,
    pl.payment_type_name, -- Joined from the seed asset
    t.fare_amount,
    t.taxi_type,
    t.extracted_at
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup pl 
    ON t.payment_type = pl.payment_type_id
WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'
  -- DATA CLEANING: Filter out the ~37k rows with negative fares found by Bruin
  AND t.fare_amount >= 0
  -- DATA CLEANING: Ensure we have at least one passenger
  AND t.passenger_count > 0
  -- DATA CLEANING: Remove any rows with invalid distances
  AND t.trip_distance >= 0
