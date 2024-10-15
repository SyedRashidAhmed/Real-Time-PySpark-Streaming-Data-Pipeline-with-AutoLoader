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

## Notes

- Adjust the schema as necessary to fit your data requirements.
- Ensure the DBFS paths are correct and accessible.
- Monitor the stream for errors and performance metrics to optimize data ingestion.

This README serves as a guide to understand the setup and configuration for ingesting data using PySpark's Auto Loader into Delta tables.
