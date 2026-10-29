import requests
import pandas as pd
from datetime import datetime, date
import time
from pyspark.sql import SparkSession
from pyspark import pipelines as dp

# Initial configurations
# Replace the following variables with your own Databricks storage locations
CSV_FILE_PATH = "/Workspace/Users/YOUR_EMAIL@gmail.com/aqi-by-continent-pipeline/source_files/capital_city_locations.csv"
BRONZE_TABLE_NAME = "workspace.default.bronze_global_aqi"

def fetch_global_aqi(csv_path):
    # 1. Load raw geographical data
    cities = pd.read_csv(csv_path)

    # 2. Drop duplicates and null values for each country (ensuring only the largest remains)
    cities_not_null = cities[~cities['population'].isna()]
    roster = cities_not_null.sort_values(by='population', ascending=False).drop_duplicates(subset=['country'])

    # URL for OpenMeteo API
    aqi_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    raw_data_list = []

    # 3. Iterate over cities list to get data
    for idx, row in roster.iterrows():
        # Indicate parameters to be pulled by API
        params = {
            "latitude": row['lat'],
            "longitude": row['lng'],
            "current": "us_aqi,pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone"
            }
        
        try:
            # Initiate 10-second timeout to prevent hanging
            response = requests.get(aqi_url, params=params, timeout=10)
            response.raise_for_status()

            raw_data = response.json()
            current_metrics = raw_data.get("current", {})
            latitude = raw_data.get("latitude", {})

            # 4. Merge static data with live pulled data
            record = {
                "city": row['city_ascii'],
                "country": row['country'],
                "continent": row['Continent'],
                "population": row['population'],
                "latitude": row['lat'],
                "longitude": row['lng'],
                # Obtain live metrics from pulled data
                "sensor_timestamp": current_metrics.get("time"),
                "overall_aqi": current_metrics.get("us_aqi"),
                "pm10": current_metrics.get("pm10"),
                "pm2_5": current_metrics.get("pm2_5"),
                "carbon_monoxide": current_metrics.get("carbon_monoxide"),
                "nitrogen_dioxide": current_metrics.get("nitrogen_dioxide"),
                "sulphur_dioxide": current_metrics.get("sulphur_dioxide"),
                "ozone": current_metrics.get("ozone"),
                # Add ingestion timestamp to track when pipeline ran
                "current_timestamp": datetime.utcnow().isoformat(),
                "current_date": date.today()
            }
            raw_data_list.append(record)

            # 5. Add rate-limiting protection (delay) to prevent OpenMeteo API from temporarily 
            # banning our pipeline for spamming requests for querying 100+ countries
            time.sleep(0.25)

        except requests.exceptions.RequestException as e:
            # Network error exception
            print(f"Network error fetching data for {row['city_ascii'], row['country']}: {e}")
            continue
        except KeyError as e:
            print(f"Data missing in API response for {row['city']}: {e}")
            continue
    
    return pd.DataFrame(raw_data_list)

# Send raw CSV file into our function
final_df = fetch_global_aqi(CSV_FILE_PATH)

# Write dataframe into bronze layer table
spark = (
    SparkSession.builder
        .appName("Aggregate AQI Table Builder")
        .getOrCreate()
)
spark_df = spark.createDataFrame(final_df)

spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(BRONZE_TABLE_NAME)


            
            
        

    