#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 02:37:58 2024

@author: sandipmac
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'sandip',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_submit_in_local_mode',
    default_args=default_args,
    description='DAG to submit Spark jobs using SparkSubmitOperator',
    schedule_interval='*/05 * * * *',  # Runs every 10 minutes
    catchup=False,
)

# Define the SparkSubmitOperator
spark_job = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/Users/sandipmac/fraud_detection_spark/mainapp_batch.py',  # Specify the path to your Spark application
    name='Fraud Detection Data Preparation in batch',
    conn_id=None,
    conn_data={"master": "local[4]"},
    verbose=True,
    dag=dag,
)

# Define the task dependencies (if any)
spark_job
