#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 28 14:59:53 2024

@author: sandipmac
"""

import pandas as pd
import numpy as np
from faker import Faker

# Initialize Faker
faker = Faker()

# Define lists to hold generated data
data = {
    "TransactionID": [],
    "TransactionTime": [],
    "Amount": [],
    "CustomerID": [],
    "Location": [],
    "V1": [],
    "V2": [],
    "V3": [],
    "Class": []
}

# Define possible locations
locations = ["NY", "CA", "FL", "TX", "IL", "NV", "NJ", "AZ", "WA", "MA"]

# Create 1000 sample records
for i in range(1, 21):
    # Generate random data for each column
    data["TransactionID"].append(f"TRANS{i:05d}")
    data["TransactionTime"].append(faker.date_time_between(start_date='-30d', end_date='now').strftime("%Y-%m-%d %H:%M:%S"))
    data["Amount"].append(round(np.random.uniform(10.0, 5000.0), 2))
    data["CustomerID"].append(faker.bothify(text='CUST####'))
    data["Location"].append(np.random.choice(locations))
    data["V1"].append(round(np.random.uniform(-2.0, 2.0), 3))
    data["V2"].append(round(np.random.uniform(-2.0, 2.0), 3))
    data["V3"].append(round(np.random.uniform(-2.0, 2.0), 3))
    # Randomly assign fraud status (Class)
    data["Class"].append(np.random.choice([0, 1], p=[0.97, 0.03]))  # 3% chance of being fraudulent

# Create DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv("sample_transactions_20.csv", index=False)

print("File 'sample_transactions_1000.csv' generated successfully!")
