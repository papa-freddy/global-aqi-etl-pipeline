from pyspark.sql.functions import col, count, sum as _sum, round
from pyspark.sql.window import Window

# Initial configurations
# Replace the following with your own Databricks storage locations
SILVER_TABLE = "workspace.default.silver_cleaned_aqi"
GOLD_TABLE = "workspace.default.aqi_by_continent_gold"

# 1. Read cleaned data from silver layer.
silver_df = spark.table(SILVER_TABLE)

# 2. Group table by continent and AQI category and aggregate by country count
category_count_df = silver_df.groupBy("continent", "aqi_category").agg(count("country").alias("country_count"))

# 3. Calculate total countries per continent and derive the percentage
gold_df = category_count_df.withColumn("total_countries", _sum("country_count").over(window)) \
        .withColumn("category_percentage", round(col("country_count") / col("total_countries") * 100, 3) ) \
        .orderBy("continent", col("category_percentage").desc())

# 4. Save gold table as a Delta table
gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(GOLD_TABLE)