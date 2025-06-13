#Imports & Spark Session Setup

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, sum, when

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYCTaxiAnalysis") \
    .getOrCreate()

    
# Load CSV file
file_path = "sample_dataset.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
# Show first few rows
df.show(5)


# Check for missing values
print( 'check for missing values only on numeric columns')
from pyspark.sql.functions import isnan, when, count

missing_all = df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df.columns
])

missing_all.show()

# Drop rows with nulls
df_clean = df.dropna()


#Revenue by Hour
df_hourly = df_clean.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                    .groupBy("pickup_hour") \
                    .agg(sum("total_amount").alias("total_revenue")) \
                    .orderBy("pickup_hour")
df_hourly.show()

#Top Payment Types
top_payment_types = df_clean.groupBy("payment_type") \
                            .agg(count("*").alias("transaction_count")) \
                            .orderBy(col("transaction_count").desc())
top_payment_types.show()

#Tip Percentage by Distance
df_with_tip_percent = df_clean.withColumn("tip_percentage", (col("tip_amount") / col("fare_amount")) * 100)

avg_tip_by_distance = df_with_tip_percent.filter(col("fare_amount") > 0) \
    .groupBy(
        when(col("trip_distance") <= 2, "0-2 miles")
         .when((col("trip_distance") > 2) & (col("trip_distance") <= 5), "2-5 miles")
         .otherwise("5+ miles").alias("distance_range")
    ) \
    .agg({"tip_percentage": "mean"}) \
    .withColumnRenamed("avg(tip_percentage)", "avg_tip_percentage") \
    .orderBy("distance_range")

avg_tip_by_distance.show()

#Most Popular Pickup Locations
top_pickups = df_clean.groupBy("PULocationID") \
                      .agg(count("*").alias("pickup_count")) \
                      .orderBy(col("pickup_count").desc()) \
                      .limit(10)
top_pickups.show()


import os

# output folder 
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# 1. Revenue by Hour
df_hourly_pandas = df_hourly.toPandas()
df_hourly_pandas.to_csv(os.path.join(output_dir, "revenue_by_hour.csv"), index=False)

# 2. Top Payment Types
top_payment_types_pandas = top_payment_types.toPandas()
top_payment_types_pandas.to_csv(os.path.join(output_dir, "top_payment_types.csv"), index=False)

# 3. Tip Percentage by Distance
avg_tip_by_distance_pandas = avg_tip_by_distance.toPandas()
avg_tip_by_distance_pandas.to_csv(os.path.join(output_dir, "tip_percentage_by_distance.csv"), index=False)

# 4. Top Pickup Locations
top_pickups_pandas = top_pickups.toPandas()
top_pickups_pandas.to_csv(os.path.join(output_dir, "top_pickups.csv"), index=False)

print("All files saved successfully in the 'output' folder!")