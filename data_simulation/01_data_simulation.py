# Databricks notebook source
# 1. Import libraries
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd

# 2. Define simulation functions
cities = ['London', 'Manchester', 'Leeds', 'Birmingham', 'Glasgow']
trip_statuses = ['completed', 'cancelled', 'in_progress']
driver_statuses = ['active', 'inactive']

def simulate_event():
    return {
        'event_id': str(uuid.uuid4()),
        'timestamp': datetime.utcnow().isoformat(),
        'city': random.choice(cities),
        'driver_id': f"DRV-{random.randint(1000,9999)}",
        'rider_id': f"RID-{random.randint(1000,9999)}",
        'trip_status': random.choice(trip_statuses),
        'driver_status': random.choice(driver_statuses),
        'pickup_lat': round(random.uniform(51.3, 53.6), 6),
        'pickup_lon': round(random.uniform(-0.5, -3.2), 6),
        'trip_duration_mins': random.randint(2, 60),
        'fare_amount': round(random.uniform(3.0, 40.0), 2)
    }

# 3. Generate 1000 events
events = [simulate_event() for _ in range(1000)]
df = pd.DataFrame(events)

# 4. Convert to Spark DataFrame
spark_df = spark.createDataFrame(df)

# 5. Save as CSV in `/data/raw`
raw_path = "dbfs:/data/raw/ride_events/"
spark_df.write.mode("overwrite").option("header", True).csv(raw_path)

display(spark_df)


# COMMAND ----------

from pyspark.sql.functions import col

# 1. Read raw CSV from DBFS
raw_path = "dbfs:/data/raw/ride_events/"
df_raw = spark.read.option("header", True).csv(raw_path)

# 2. Write to Bronze Delta Table
bronze_path = "dbfs:/data/lakehouse/bronze/ride_events"
df_raw.write.format("delta").mode("overwrite").save(bronze_path)

# 3. Create Bronze table (for querying)
spark.sql("DROP TABLE IF EXISTS bronze_ride_events")
spark.sql(f"""
    CREATE TABLE bronze_ride_events
    USING DELTA
    LOCATION '{bronze_path}'
""")

display(spark.sql("SELECT * FROM bronze_ride_events LIMIT 5"))


# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# 1. Read from Bronze
bronze_df = spark.read.format("delta").load(bronze_path)

# 2. Clean + cast columns
silver_df = bronze_df.withColumn("timestamp", to_timestamp("timestamp")) \
    .withColumn("trip_duration_mins", col("trip_duration_mins").cast("int")) \
    .withColumn("fare_amount", col("fare_amount").cast("float")) \
    .dropna()

# 3. Save as Silver Delta Table
silver_path = "dbfs:/data/lakehouse/silver/ride_events"
silver_df.write.format("delta").mode("overwrite").save(silver_path)

# 4. Create Silver Table
spark.sql("DROP TABLE IF EXISTS silver_ride_events")
spark.sql(f"""
    CREATE TABLE silver_ride_events
    USING DELTA
    LOCATION '{silver_path}'
""")

display(spark.sql("SELECT * FROM silver_ride_events LIMIT 5"))


# COMMAND ----------

from pyspark.sql.functions import hour, date_format, count, avg, sum

# 1. Load Silver data
silver_df = spark.read.format("delta").load(silver_path)

# 2. Create hourly aggregates
gold_df = silver_df.withColumn("hour", hour("timestamp")) \
    .withColumn("date", date_format("timestamp", "yyyy-MM-dd")) \
    .groupBy("city", "date", "hour") \
    .agg(
        count("*").alias("total_trips"),
        avg("fare_amount").alias("avg_fare"),
        sum("fare_amount").alias("total_revenue")
    )

# 3. Save as Gold Delta Table
gold_path = "dbfs:/data/lakehouse/gold/hourly_city_metrics"
gold_df.write.format("delta").mode("overwrite").save(gold_path)

# 4. Create Gold Table
spark.sql("DROP TABLE IF EXISTS gold_hourly_metrics")
spark.sql(f"""
    CREATE TABLE gold_hourly_metrics
    USING DELTA
    LOCATION '{gold_path}'
""")

display(spark.sql("SELECT * FROM gold_hourly_metrics LIMIT 5"))


# COMMAND ----------

