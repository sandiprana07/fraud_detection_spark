#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 18:54:48 2024

@author: sandipmac
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 28 14:46:50 2024

@author: sandipmac
"""

#import findspark
#findspark.init()
#findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
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
    .appName("Fraud Detection Data Preparation in batch") \
    .getOrCreate()

transaction_df = spark.read \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .schema(schema) \
        .option("header", "true") \
        .csv("/Users/sandipmac/Documents/IIT_Jodhpur/semester IV/Software and Data engineer/mini_project/fraud_detection_spark/batch_input")

transaction_df.show()

# Perform Data Transformation
transformed_df = transaction_df \
    .withColumn("Hour", hour(col("TransactionTime"))) \
    .withColumn("DayOfWeek", dayofweek(col("TransactionTime"))) \
    .withColumn("IsHighAmount", when(col("Amount") > 1000, 1).otherwise(0))
    


# # Generate Additional Features
# 1. Calculate 'AmountPerHour' as a normalized amount value
tranormed_df = transformed_df.withColumn("AmountPerHour", col("Amount") / (col("Hour") + 1))

# 2. Location Risk Factor: Assign higher risk to certain locations (example: specific city codes)
high_risk_locations = ["NY", "CA", "FL"]  # Example of high-risk locations
transformed_df = transformed_df.withColumn("LocationRisk",
                                            when(col("Location").isin(high_risk_locations), 1).otherwise(0))

# 3. Generate Categorical Feature: Time of Day (Morning, Afternoon, Evening, Night)
transformed_df = transformed_df.withColumn("TimeOfDay",
                                            when(col("Hour") < 12, "Morning")
                                            .when((col("Hour") >= 12) & (col("Hour") < 17), "Afternoon")
                                            .when((col("Hour") >= 17) & (col("Hour") < 21), "Evening")
                                            .otherwise("Night"))
# tranormed_df.show()
# transformed_df.write.format("csv").mode("append").save("batch_output")
transformed_df.write.mode("overwrite") \
    .format("parquet") \
    .save("/Users/sandipmac/Documents/IIT_Jodhpur/semester IV/Software and Data engineer/mini_project/fraud_detection_spark/batch_output")









