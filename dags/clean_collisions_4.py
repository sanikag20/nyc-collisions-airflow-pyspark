from pyspark.sql import SparkSession
from pyspark.sql.functions import try_to_date
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    when,
    trim
)

spark = (
    SparkSession.builder
    .appName("NYCCollisionCleaning")
    .getOrCreate()
)

input_path = "/home/sanikagumaste10/nyc_collision_project/data/raw/collisions.csv"
output_path = "/home/sanikagumaste10/nyc_collision_project/data/processed/cleaned"

# Read raw CSV
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("====================================")
print("INITIAL DATA")
print("====================================")

print("Initial row count:", df.count())

df.printSchema()
df.show(5, truncate=False)


# Rename columns so they are easier to work with
df = df.withColumnRenamed("CRASH DATE", "crash_date")
df = df.withColumnRenamed("CRASH TIME", "crash_time")
df = df.withColumnRenamed("BOROUGH", "borough")
df = df.withColumnRenamed("ZIP CODE", "zip_code")
df = df.withColumnRenamed("LATITUDE", "latitude")
df = df.withColumnRenamed("LONGITUDE", "longitude")
df = df.withColumnRenamed("LOCATION", "location")
df = df.withColumnRenamed("ON STREET NAME", "on_street_name")
df = df.withColumnRenamed("CROSS STREET NAME", "cross_street_name")
df = df.withColumnRenamed("OFF STREET NAME", "off_street_name")

df = df.withColumnRenamed(
    "NUMBER OF PERSONS INJURED",
    "number_of_persons_injured"
)

df = df.withColumnRenamed(
    "NUMBER OF PERSONS KILLED",
    "number_of_persons_killed"
)

df = df.withColumnRenamed(
    "NUMBER OF PEDESTRIANS INJURED",
    "number_of_pedestrians_injured"
)

df = df.withColumnRenamed(
    "NUMBER OF PEDESTRIANS KILLED",
    "number_of_pedestrians_killed"
)

df = df.withColumnRenamed(
    "NUMBER OF CYCLIST INJURED",
    "number_of_cyclist_injured"
)

df = df.withColumnRenamed(
    "NUMBER OF CYCLIST KILLED",
    "number_of_cyclist_killed"
)

df = df.withColumnRenamed(
    "NUMBER OF MOTORIST INJURED",
    "number_of_motorist_injured"
)

df = df.withColumnRenamed(
    "NUMBER OF MOTORIST KILLED",
    "number_of_motorist_killed"
)

df = df.withColumnRenamed(
    "CONTRIBUTING FACTOR VEHICLE 1",
    "contributing_factor_vehicle_1"
)

df = df.withColumnRenamed(
    "CONTRIBUTING FACTOR VEHICLE 2",
    "contributing_factor_vehicle_2"
)

df = df.withColumnRenamed(
    "CONTRIBUTING FACTOR VEHICLE 3",
    "contributing_factor_vehicle_3"
)

df = df.withColumnRenamed(
    "CONTRIBUTING FACTOR VEHICLE 4",
    "contributing_factor_vehicle_4"
)

df = df.withColumnRenamed(
    "CONTRIBUTING FACTOR VEHICLE 5",
    "contributing_factor_vehicle_5"
)

df = df.withColumnRenamed(
    "COLLISION_ID",
    "collision_id"
)

df = df.withColumnRenamed(
    "VEHICLE TYPE CODE 1",
    "vehicle_type_code1"
)

df = df.withColumnRenamed(
    "VEHICLE TYPE CODE 2",
    "vehicle_type_code2"
)

df = df.withColumnRenamed(
    "VEHICLE TYPE CODE 3",
    "vehicle_type_code3"
)

df = df.withColumnRenamed(
    "VEHICLE TYPE CODE 4",
    "vehicle_type_code4"
)

df = df.withColumnRenamed(
    "VEHICLE TYPE CODE 5",
    "vehicle_type_code5"
)


# Convert crash date
df = df.withColumn(
    "crash_date",
    try_to_date(col("crash_date"), "MM/dd/yyyy")
)


# Convert numerical columns
numeric_columns = [
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed"
]

from pyspark.sql.functions import expr

for c in numeric_columns:
    df = df.withColumn(
        c,
        expr(f"try_cast({c} as double)")
    )

# Replace null numerical values with zero
df = df.fillna(
    0,
    subset=numeric_columns
)


# Clean borough
df = df.withColumn(
    "borough",
    trim(col("borough"))
)


# Remove records without crash date
df = df.filter(
    col("crash_date").isNotNull()
)


# Remove records without borough
df = df.filter(
    col("borough").isNotNull()
)


# Create year
df = df.withColumn(
    "crash_year",
    year(col("crash_date"))
)


# Create month
df = df.withColumn(
    "crash_month",
    month(col("crash_date"))
)


# Create total injured column
df = df.withColumn(
    "total_injured",
    col("number_of_persons_injured")
)


# Create severity classification
df = df.withColumn(
    "severity",
    when(
        col("number_of_persons_killed") > 0,
        "FATAL"
    )
    .when(
        col("number_of_persons_injured") >= 3,
        "SEVERE"
    )
    .when(
        col("number_of_persons_injured") >= 1,
        "MODERATE"
    )
    .otherwise("MINOR")
)


print("====================================")
print("CLEANED DATA")
print("====================================")

print("Cleaned row count:", df.count())

df.printSchema()

df.show(
    10,
    truncate=False
)


print("ABOUT TO WRITE TO:", output_path)

df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("WRITE COMPLETED SUCCESSFULLY")

spark.stop()
