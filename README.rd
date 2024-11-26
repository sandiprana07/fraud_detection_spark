#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 28 14:51:11 2024

@author: sandipmac
"""

This projetc is to ccapture the transactional data for fraud detection


Step 1: Install pyspark in local:
1. conda install conda-forge::pyspark
2. pyspark - command
3. spark-submit --master local[1] <application>.py [application-arguments]


Step 2: Install Apache Kafka
1.	Install both Kafka and Zookeeper using Homebrew (Kafka comes bundled with Zookeeper, which is required for Kafka).


brew install kafka
Step 3: Start Zookeeper
Zookeeper is a centralized service for maintaining configuration information and providing distributed synchronization, which Kafka needs to operate. Start it using:
/opt/homebrew/opt/kafka/bin/zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
This command starts Zookeeper using the default properties file provided during installation.
Step 4: Start Kafka Server
Open a new terminal tab or window, and run the following command to start Kafka:
/opt/homebrew/opt/kafka/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties
This starts the Kafka server using the default configuration.
Step 5: Create a Kafka Topic
After starting Kafka, you can create a topic to send and receive messages. Use the following command to create a topic named test_topic:
bash
Copy code
kafka-topics --create --topic test_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
Step 6: List Available Topics
To check if the topic has been created, list all available topics:
bash
Copy code
kafka-topics --list --bootstrap-server localhost:9092

Step 7: Start a Kafka Producer
To send messages to the Kafka topic, run a producer in the terminal:
bash
Copy code
kafka-console-producer --topic test_topic --bootstrap-server localhost:9092
Now, type any message and press Enter. These messages will be sent to the test_topic topic.
Step 8: Start a Kafka Consumer
To receive messages from the topic, start a Kafka consumer in another terminal window:
bash
Copy code
kafka-console-consumer --topic test_topic --from-beginning --bootstrap-server localhost:9092
This consumer will print all the messages from the beginning of the topic.
Step 9: Stop Zookeeper and Kafka
After testing, you can stop the Kafka server and Zookeeper using:
•	Stop Kafka:
bash
Copy code
/opt/homebrew/opt/kafka/bin/kafka-server-stop
•	Stop Zookeeper:
bash
Copy code
/opt/homebrew/opt/kafka/bin/zookeeper-server-stop
Common Issues and Solutions
1.	Port Already in Use: If you get a port conflict error, check the processes using the following command:
bash
Copy code
lsof -i :2181    # For Zookeeper
lsof -i :9092    # For Kafka
Kill the process using the conflicting port:
bash
Copy code
kill -9 <process_id>
2.	Kafka Configuration Files: Configuration files for Kafka and Zookeeper are located at:
bash
Copy code
/opt/homebrew/etc/kafka/
Alternative: Using Confluent Platform
If you want a complete Kafka solution with a UI for monitoring, consider using the Confluent Platform. Installation is slightly different but offers a more robust setup.
Let me know if you encounter any issues or need further assistance!



Airflow install:


1.	Set Up a New Conda Environment:
It is recommended to create a new conda environment specifically for Airflow to avoid dependency conflicts.
bash
Copy code
conda create --name airflow_env python=3.8
Replace airflow_env with your preferred environment name. You can also choose the desired Python version, but Airflow generally works best with Python 3.8 or 3.9.
2.	Activate the Conda Environment:
bash
Copy code
conda activate airflow_env
3.	Install Apache Airflow Using Conda:
Conda Forge maintains a package for Airflow, so you need to install it from the conda-forge channel.
bash
Copy code
conda install -c conda-forge airflow
This command will install the latest stable version of Apache Airflow along with its dependencies.
4.	Verify the Installation:
After the installation is complete, you can check if Airflow is installed correctly by running:
bash
Copy code
airflow version
5.	Initialize the Airflow Metadata Database:
Before you start using Airflow, you need to initialize the metadata database:
bash
Copy code
airflow db init
6.	Start the Airflow Web Server:
Start the web server on port 8080 (default port for Airflow):
bash
Copy code
airflow webserver --port 8080
7.	Start the Airflow Scheduler:
In another terminal (with the same environment activated), start the scheduler:
bash
Copy code
airflow scheduler

8.	Access the Airflow UI:
Open a browser and navigate to:
arduino
Copy code
http://localhost:8080
Now you should have Apache Airflow up and running in your conda environment. Let me know if you need help with configurations or adding plugins.


Run the airflow standalone
