# Real-Time-PySpark-Streaming-Data-Pipeline-with-AutoLoader

## Overview

This notebook demonstrates how to set up a data pipeline using PySpark's Auto Loader feature to continuously ingest CSV files into a Delta table in Databricks. The pipeline will automatically infer the schema or use a predefined schema, process the incoming data, and write it to the `bronze_table` in the `rashid` database.

## Table of Contents

1. **Schema Definition**
2. **Data Ingestion Setup**
3. **Auto Loader Configuration**
4. **Streaming Write to Delta Table**
5. **Clean-Up Operations**

## 1. Schema Definition

We define the schema for the incoming data using the `StructType` and `StructField` classes from `pyspark.sql.types`. The schema includes fields such as:

- `VendorID`: Integer
- `tpep_pickup_datetime`: Timestamp
- `tpep_dropoff_datetime`: Timestamp
- `passenger_count`: Integer
- `trip_distance`: Double
- ... (additional fields as specified)

```python
from pyspark.sql.types import StructField, StructType, IntegerType, LongType, DoubleType, TimestampType, StringType

schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", LongType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),  
    StructField("DOLocationID", IntegerType()),  
    StructField("payment_type", IntegerType()),  
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("Airport_fee", DoubleType())
])
```

## 2. Data Ingestion Setup

We begin by cleaning up the existing data by truncating the `bronze_table` and removing any old checkpoint and schema location directories to ensure a fresh start.

```sql
%sql
truncate table rashid.bronze_table
```

```python
%fs rm -r /FileStore/rashid/autoloader/sl
%fs rm -r /FileStore/rashid/autoloader/cp
%fs mkdirs /FileStore/rashid/autoloader/cp
%fs mkdirs /FileStore/rashid/autoloader/sl
```

## 3. Auto Loader Configuration

We configure the Auto Loader to read CSV files from a specified directory in DBFS. The following options are set:

- **Format**: `csv`
- **Schema Location**: Directory to store inferred schema.
- **Header**: Set to `true` to read the header from the CSV files.
- **Schema**: Predefined schema to enforce data structure.

```python
dbfs_path = "dbfs:/FileStore/rashid/poc_folder"
sl= "dbfs:/FileStore/rashid/autoloader/sl/"

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv") 
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaLocation", sl)   
      .option('header', 'true') 
      .schema(schema)  
      .load(dbfs_path))
```

## 4. Streaming Write to Delta Table

The incoming data is then written to the Delta table `bronze_table` with the following configurations:

- **Merge Schema**: Set to `true` to accommodate schema evolution.
- **Checkpoint Location**: Directory for storing checkpoint information.
- **Output Mode**: Set to `append` to add new records.
- **Trigger**: Configured to run once for this operation.

```python
cp= "dbfs:/FileStore/rashid/autoloader/cp/"
query = (df.writeStream.format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", cp)
    .outputMode("append")
    .trigger(once=True)
    .table("rashid.bronze_table"))
```

## 5. Clean-Up Operations

After the stream processing, it’s advisable to clean up any temporary files and directories if necessary. Ensure that you manage your storage effectively by monitoring the size of your checkpoint and schema locations.

---
# README for Data Processing Pipeline: From Bronze to Silver Table

## Overview

This notebook outlines the steps to process streaming data from a Delta table (`bronze_table`) into a refined Delta table (`silver_table`) using PySpark. The transformation includes handling missing values, duplicates, incorrect data types, and computing additional metrics.

## Table of Contents

1. **Schema Cleanup and Setup**
2. **Data Ingestion**
3. **Data Transformation**
4. **Statistical Analysis**
5. **Data Validation**
6. **Final Write to Silver Table**

## 1. Schema Cleanup and Setup

We begin by truncating the `silver_table` and cleaning up any previous checkpoint data to ensure a fresh processing environment.

```sql
%sql
truncate TABLE rashid.silver_table;
```

```python
%fs rm -r FileStore/rashid/autoloader/sil_cp
%fs mkdirs FileStore/rashid/autoloader/sil_cp
```

## 2. Data Ingestion

We read streaming data from the `bronze_table` into a DataFrame. The schema is inferred automatically.

```python
silver_df = spark.readStream.format("delta").option("inferSchema", "true").table("rashid.bronze_table")
```

## 3. Data Transformation

In this section, we apply various transformations:

- **Handle Missing Values**: Set `fare_amount` to `None` if it exceeds a certain threshold.
- **Introduce Duplicates**: For testing, we add 10 duplicate rows.
- **Incorrect Data Types**: Change `total_amount` to string type for demonstration.

```python
from pyspark.sql.functions import when, col, lit 

df_with_missing = silver_df.withColumn("fare_amount", when(col("fare_amount") > 45, lit(None)).otherwise(col("fare_amount")))
df_with_duplicates = df_with_missing.union(df_with_missing.limit(10))  # Adding 10 duplicate rows
df_with_incorrect_types = df_with_duplicates.withColumn("total_amount", col("total_amount").cast("string"))
```

## 4. Statistical Analysis

We calculate the count of missing values across all columns and compute basic statistics for numeric columns (`trip_distance`, `fare_amount`, and `total_amount`).

```python
from pyspark.sql.functions import count, isnan, mean, stddev

numeric_columns = [c for c, dtype in silver_df.dtypes if dtype in ('double', 'float', 'int')]

# Check for missing values
missing_values_count = silver_df.select([
    count(when(isnan(c) | col(c).isNull(), c)).alias(c) if c in numeric_columns 
    else count(when(col(c).isNull(), c)).alias(c) for c in silver_df.columns
])

# Display missing values count
# display(missing_values_count)

# Compute mean and standard deviation
stats = silver_df.select([mean(col(c)).alias(f"mean_{c}") for c in numeric_columns] +
                         [stddev(col(c)).alias(f"stddev_{c}") for c in numeric_columns])
# display(stats)
```

## 5. Data Validation

We validate the data by filtering out unrealistic values and checking for type mismatches based on an expected schema.

```python
# Filter out unrealistic values
silver_df = silver_df.filter((col("trip_distance") < 100) & (col("fare_amount") < 500))

# Define the expected schema
expected_schema = {
    "VendorID": IntegerType(), 
    "tpep_pickup_datetime": TimestampType(),
    "tpep_dropoff_datetime": TimestampType(),
    "passenger_count": IntegerType(),
    "trip_distance": DoubleType(),
    "RatecodeID": LongType(),
    "store_and_fwd_flag": StringType(),
    "PULocationID": IntegerType(),  
    "DOLocationID": IntegerType(),  
    "payment_type": IntegerType(),  
    "fare_amount": DoubleType(),
    "extra": DoubleType(),
    "mta_tax": DoubleType(),
    "tip_amount": DoubleType(),
    "tolls_amount": DoubleType(),
    "improvement_surcharge": DoubleType(),
    "total_amount": DoubleType(),
    "congestion_surcharge": DoubleType(),
    "Airport_fee": DoubleType()
}

# Check for mismatched data types
for col_name, expected_type in expected_schema.items():
    actual_type = [f.dataType for f in silver_df.schema.fields if f.name == col_name][0]
    if type(actual_type) != type(expected_type):
        print(f"Column '{col_name}' has incorrect data type. Expected: {expected_type.simpleString()}, Actual: {actual_type.simpleString()}")
```

## 6. Final Write to Silver Table

After corrections and validations, we write the refined DataFrame to the `silver_table`. This includes additional transformations to compute time duration and tax percentage.

```python
from pyspark.sql import functions as F

silver_df = df_corrected.withColumn(
    'time_duration_seconds',
    F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')
).withColumn(
    'time_format',
    F.concat(
        F.lpad(F.floor(F.col('time_duration_seconds') / 3600), 2, '0'), F.lit(':'),
        F.lpad(F.floor((F.col('time_duration_seconds') % 3600) / 60), 2, '0'), F.lit(':'),
        F.lpad(F.col('time_duration_seconds') % 60, 2, '0')
    )
).withColumn(
    'tax_percentage',
    F.round(((F.col('total_amount') - F.col('fare_amount')) / F.col('total_amount')) * 100, 2)
)

# Write the streaming DataFrame to a Delta table
sil_cp = "FileStore/rashid/autoloader/sil_cp"

query = (silver_df.writeStream
         .format("delta")
         .outputMode("append")  # Output mode is set to 'append'
         .option("mergeSchema", "true")
         .option("checkpointLocation", sil_cp)  # Checkpointing
         .trigger(availableNow=True) 
         .table("rashid.silver_table"))  # Write to the silver table
```

## Conclusion

This notebook provides a comprehensive framework for processing streaming data from a bronze to a silver Delta table. Each transformation step is crucial for ensuring data quality and preparing the dataset for further analysis. Adjust the code and schema as needed based on specific data requirements.

---

#Gold Layer Insights, Visualizations, and Code Documentation
---
##Overview

This project analyzes taxi trip data to extract insights related to revenue generation, trip characteristics, payment methods, and seasonal trends. The analysis is conducted using Apache Spark, leveraging DataFrames and SQL for efficient data manipulation and aggregation.

## Table of Contents

1. [Data Preparation](#data-preparation)
2. [Key Analyses](#key-analyses)
   - [1. Highest Revenue Locations](#1-highest-revenue-locations)
   - [2. Impact of Congestion Surcharge](#2-impact-of-congestion-surcharge)
   - [3. Vendor Comparison](#3-vendor-comparison)
   - [4. Payment Method Analysis](#4-payment-method-analysis)
   - [5. Peak Taxi Demand Hours](#5-peak-taxi-demand-hours)
   - [6. Trip Distance and Profitability](#6-trip-distance-and-profitability)
   - [7. Tolls and Fees Impact](#7-tolls-and-fees-impact)
   - [8. Average Passenger Count and Revenue](#8-average-passenger-count-and-revenue)
   - [9. Airport Revenue Generation](#9-airport-revenue-generation)
   - [10. Seasonal Demand Trends](#10-seasonal-demand-trends)
   - [11. Average Fare by Trip Distance](#11-average-fare-by-trip-distance)
   - [12. Surcharge Distribution](#12-surcharge-distribution)
3. [Conclusion](#conclusion)
4. [Dependencies](#dependencies)

## Data Preparation

- **File Management:** Temporary directories are created and cleaned up as needed.
- **Data Loading:** The dataset is loaded from a silver table into a DataFrame (`silver_df`).
- **Outlier Handling:** Random outliers are artificially introduced to trip distances for testing purposes.
- **Missing Values:** Missing values in `fare_amount` and `store_and_fwd_flag` are filled with calculated means or default values.
- **Statistics Calculation:** Mean and standard deviation are computed for key numerical columns.

## Key Analyses

### 1. Highest Revenue Locations

Identifies the pickup and drop-off locations that generate the highest revenue.

```python
df_1 = (gold_df.groupBy('PULocationID','DOLocationID')
        .agg(sum(col('total_amount')).alias('TotalAmount'))
        .orderBy(col('TotalAmount').desc())).limit(20)
```

### 2. Impact of Congestion Surcharge

Analyzes how congestion surcharges affect overall trip revenue.

```python
df_2 = gold_df.agg({'total_amount':'sum','congestion_surcharge':'sum'})
df_2 = df_2.withColumn('percentage',round(col('congestion_surcharge')/col('total_amount')*100,2))
```

### 3. Vendor Comparison

Compares vendors based on average earnings per trip and trip count.

```python
df_3 = gold_df.groupBy('VendorID').agg(round(avg('total_amount'), 2).alias('Average_amount'),
    count('*').alias('TripCount')).orderBy(col('Average_amount').desc())
```

### 4. Payment Method Analysis

Examines the most common payment methods and their impact on revenue.

```python
df_4 = gold_df.groupBy('payment_type').agg(count('*').alias('trips')).orderBy(col('trips').desc())
```

### 5. Peak Taxi Demand Hours

Identifies peak hours for taxi demand and their corresponding trip counts.

```python
df_5 = gold_df.withColumn('Hour',hour(col('tpep_pickup_datetime'))).groupBy('Hour').agg(count('*').alias('trips')).orderBy(col('trips').desc())
```

### 6. Trip Distance and Profitability

Explores how trip distance correlates with profitability.

```python
df_6 = gold_df.groupBy('trip_distance') \
              .agg(round(sum(col('total_amount')), 2).alias('total_amount')) \
              .orderBy(col('total_amount').desc())
```

### 7. Tolls and Fees Impact

Analyzes how tolls and fees impact profit margins for each trip.

```python
result = gold_df.select(
    col("trip_distance"),
    col("total_amount"),
    col("tolls_amount"),
    col("mta_tax"),
    col("improvement_surcharge"),
    round(col("total_amount") - (col("tolls_amount") + col("mta_tax") + col("improvement_surcharge")), 2).alias("profit_margin")
).orderBy(col("profit_margin").desc())
```

### 8. Average Passenger Count and Revenue

Examines the average passenger count per trip and its relation to revenue.

```python
df_8 = gold_df.groupBy(col('passenger_count')).agg(count('*').alias('trips'),round(sum(col('total_amount')),2).alias('total_amount')).orderBy(col('trips').desc())
```

### 9. Airport Revenue Generation

Identifies revenue generated by pickup and drop-off points near airports.

```python
df_9 = gold_df.groupBy(col('PULocationID'),col('DOLocationID')).agg(sum(col('airport_fee')).alias('total_fee'),count('*').alias('number_of_trips')).where(col('total_fee')>0).orderBy(col('total_fee').desc())
```

### 10. Seasonal Demand Trends

Analyzes how different times of the year affect trip volume and revenue.

```python
result = (gold_df
    .groupBy(F.year("tpep_pickup_datetime").alias("year"), 
             F.month("tpep_pickup_datetime").alias("month"))
    .agg(F.round(F.sum("total_amount"), 2).alias("total_amount"),
         F.count("*").alias("trips"))
    .orderBy(F.col("total_amount").desc())
)
```

### 11. Average Fare by Trip Distance

Calculates average fare amounts per trip distance.

```python
result = (gold_df
    .groupBy(F.expr("INT(trip_distance)").alias("distance"))
    .agg(F.round(F.avg("total_amount"), 2).alias("average_fare"))
    .orderBy("distance")
)
```

### 12. Surcharge Distribution

Identifies the distribution of trips by surcharge amount.

```python
df_12 = gold_df.groupBy(col('congestion_surcharge').alias('surcharge')).agg(count('*').alias('number_of_trips')).orderBy(col('surcharge').asc()).where(col('surcharge')>=0)
```

## Conclusion

This project provides comprehensive insights into taxi trip data, highlighting key revenue drivers, seasonal trends, and factors affecting profitability. The findings can be useful for stakeholders looking to optimize operations and enhance revenue generation strategies.

## Dependencies

- Apache Spark (PySpark)
- Databricks or similar Spark environment
- SQL for table management

Feel free to explore the code and adapt it for your own analysis!
