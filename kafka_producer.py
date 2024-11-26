#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 29 00:34:46 2024

@author: sandipmac
"""

import pandas as pd
import time
from kafka import KafkaProducer
import json

# Define the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Update with your Kafka broker address
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Read the CSV file using pandas
csv_file = './batch_input/sample_fraud_transactions_1000.csv'
df = pd.read_csv(csv_file)

# Topic name
topic_name = "test_topic"

# Send each row in the CSV as a separate message to Kafka

chunk_size = 10

for chunk in pd.read_csv(csv_file, 
chunksize=chunk_size,engine="python"):

    for _, row in chunk.iterrows():
        message = row.to_dict()  # Convert row to dictionary
        producer.send(topic_name, message)
        print(f"Sent: {message}")
        time.sleep(10) 
    
# Ensure all messages are sent
producer.flush()
producer.close()
