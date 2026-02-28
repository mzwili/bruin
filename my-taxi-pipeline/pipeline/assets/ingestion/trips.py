"""@bruin
name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

# Typical NYC taxi trip schema
columns:
  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    checks:
      - name: non_negative
        blocking: false
  - name: taxi_type
    type: string
  - name: extracted_at
    type: timestamp

@bruin"""

import os
import json
from datetime import datetime, timezone
import pandas as pd
import requests
from io import BytesIO

def materialize():
    # 1. Parse date window
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")
    
    if not start_date or not end_date:
        raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set")

    # 2. Load variables
    vars_json = os.environ.get("BRUIN_VARS", "{}")
    vars_dict = json.loads(vars_json)
    taxi_types = vars_dict.get("taxi_types", ["yellow"])

    # Helper for month iteration
    def month_range(start: datetime, end: datetime):
        current = start.replace(day=1)
        while current <= end:
            yield current.year, current.month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

    start_dt = datetime.fromisoformat(start_date.replace("Z", ""))
    end_dt = datetime.fromisoformat(end_date.replace("Z", ""))

    dfs = []
    headers = {'User-Agent': 'Mozilla/5.0'}

    for taxi_type in taxi_types:
        for year, month in month_range(start_dt, end_dt):
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            
            try:
                print(f"Fetching: {url}")
                r = requests.get(url, headers=headers, timeout=60)
                r.raise_for_status()
                
                # Use BytesIO to avoid 'seek' errors
                df = pd.read_parquet(BytesIO(r.content))
                
                # 3. Standardize Columns (remap TLC names to our schema)
                df = df.rename(columns={
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                    'passenger_count': 'passenger_count',
                    'trip_distance': 'trip_distance',
                    'payment_type': 'payment_type',
                    'fare_amount': 'fare_amount'
                }, errors='ignore')

                df["taxi_type"] = taxi_type
                dfs.append(df)
                print(f"Success: {len(df)} rows loaded for {taxi_type} {year}-{month}")
                
            except Exception as e:
                print(f"Warning: could not fetch {url}: {e}")

    # 4. Handle Empty Results
    if not dfs:
        print("No data found for this period. Returning None to skip load.")
        return None

    result = pd.concat(dfs, ignore_index=True)
    result = result[result['fare_amount'] >= 0] 
    result["extracted_at"] = datetime.now(timezone.utc)

    # 5. Final Schema Alignment
    # Ensuring ONLY these columns are returned to match the DuckDB table
    final_cols = [
        "pickup_datetime", "dropoff_datetime", "passenger_count", 
        "trip_distance", "payment_type", "fare_amount", 
        "taxi_type", "extracted_at"
    ]
    
    # Filter to only the columns that exist in the dataframe
    available_cols = [c for c in final_cols if c in result.columns]
    return result[available_cols]
