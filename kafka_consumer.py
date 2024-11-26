#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 29 01:57:55 2024

@author: sandipmac
"""

from kafka import KafkaConsumer
import json

# Define the topic name and Kafka server
topic_name = 'test_topic'
bootstrap_servers = ['localhost:9092']  # Replace with your Kafka broker address

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',        # Read messages from the beginning
    group_id='consumer-group-a',          # Consumer group id
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Start consuming messages
print(f"Consuming messages from the topic: {topic_name}")
for message in consumer:
    print(f"Received message: {message.value}")

