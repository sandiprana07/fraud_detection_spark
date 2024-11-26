#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 28 14:46:50 2024

@author: sandipmac
"""

import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json
from pyspark.sql.functions import hour, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType


# Define the schema using StructType
schema = StructType([
    StructField("TransactionID", StringType(), True),
    StructField("TransactionTime", TimestampType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("V1", DoubleType(), True),
    StructField("V2", DoubleType(), True),
    StructField("V3", DoubleType(), True),
    StructField("Class", IntegerType(), True)
])

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Real-Time Fraud Detection Data Preparation") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()
 


# Read Streaming Data from a Source (e.g., Kafka, Socket)
# transaction_df = spark.read \
#     .format("csv") \
#     .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
#     .schema(schema) \
#     .option("header", "true") \
#     .load("sample_fraud_transactions_1000.csv") 
    
transaction_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "test_topic") \
        .option("startingOffsets", "latest") \
        .load()
        
transaction_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df_transformed = transaction_df.selectExpr("CAST(value AS STRING) as json_value") \
                   .select(from_json("json_value", "TransactionID STRING, TransactionTime TIMESTAMP, Amount DOUBLE, CustomerID STRING, Location STRING," +
                                     "V1 DOUBLE, V2 DOUBLE, V3 DOUBLE, Class INTEGER") \
                           .alias("data")) \
                   .select("data.*")
    
# Perform Data Transformation (Feature Engineering)
transformed_df = df_transformed \
    .withColumn("Hour", hour(col("TransactionTime"))) \
    .withColumn("DayOfWeek", dayofweek(col("TransactionTime"))) \
    .withColumn("IsHighAmount", when(col("Amount") > 1000, 1).otherwise(0))
    


# # Generate Additional Features
# 1. Calculate 'AmountPerHour' as a normalized amount value
tranormed_df = transformed_df.withColumn("AmountPerHour", col("Amount") / (col("Hour") + 1))

# 2. Location Risk Factor: Assign higher risk to certain locations (example: specific city codes)
# Example of high-risk locations
high_risk_locations = ["NY", "CA", "FL"]  
transformed_df = transformed_df.withColumn("LocationRisk",
                                            when(col("Location").isin(high_risk_locations), 1).otherwise(0))

# 3. Generate Categorical Feature: Time of Day (Morning, Afternoon, Evening, Night)
transformed_df = transformed_df.withColumn("TimeOfDay",
                                            when(col("Hour") < 12, "Morning")
                                            .when((col("Hour") >= 12) & (col("Hour") < 17), "Afternoon")
                                            .when((col("Hour") >= 17) & (col("Hour") < 21), "Evening")
                                            .otherwise("Night"))


# Write to local folder for testing
query = df_transformed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "streaming_output") \
    .option("checkpointLocation", "streaming_checkpoint") \
    .start()
    
query.awaitTermination()








