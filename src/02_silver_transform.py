from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, FloatType

# Initial configurations
# Replace the following with your own Databricks storage locations
BRONZE_TABLE = "workspace.default.bronze_global_aqi"
SILVER_TABLE = "workspace.default.silver_cleaned_aqi"

# 1. Read from registered Delta table
bronze_df = spark.table(BRONZE_TABLE)

# 2. Clean and cast data types
cast_df = bronze_df.withColumn("overall_aqi", col("overall_aqi").cast(IntegerType())) \
        .withColumn("pm2_5", col("pm2_5").cast(FloatType())) \
        .withColumn("population", col("population").cast(IntegerType()))
pre_silver_df = cast_df[["city", "country", "continent", "population", "overall_aqi", "pm2_5", "current_timestamp", "current_date"]]

# 3. Add categorical classification of AQI
silver_df = pre_silver_df.withColumn("aqi_category", 
                                     when(col("overall_aqi").isNull(), "Unavailable")
                                     .when(col("overall_aqi") <= 50, "Good")
                                     .when(col("overall_aqi") <= 100, "Moderate")
                                     .when(col("overall_aqi") <= 150, "Unhealthy for Sensitive Groups")
                                     .when(col("overall_aqi") <= 200, "Unhealthy")
                                     .when(col("overall_aqi") <= 300, "Very Unhealthy")
                                     .otherwise("Hazardous"))

# 4. Write out as new silver table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("continent") \
    .saveAsTable(SILVER_TABLE)
