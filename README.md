# Kafka Streaming Project

# Project Overview
This project implements a real-time data streaming pipeline using Apache Kafka, Apache Spark Streaming, and Cassandra. The pipeline ingests streaming data, processes it in near real-time with Spark, and stores the results in Cassandra for scalable storage and querying.

The project also includes orchestration using Apache Airflow and is containerized using Docker for easy deployment.

# Key Features
Real-time data ingestion with Apache Kafka

Stream processing and analytics using Apache Spark Streaming

Scalable storage of processed data in Apache Cassandra

Workflow orchestration and scheduling via Apache Airflow

Containerized environment using Docker and Docker Compose

Includes ETL pipelines and fault-tolerant streaming design

# Technologies Used
Apache Kafka

Apache Spark Streaming

Apache Cassandra

Apache Airflow

Docker & Docker Compose

Python (for Spark jobs and Airflow DAGs)

Bash scripting (for entrypoint and setup scripts)

# Project Structure
dags/ — Airflow DAGs for workflow orchestration

spark_stream.py — Spark Streaming job processing Kafka streams

docker-compose.yml — Docker Compose file to run Kafka, Zookeeper, Spark, Cassandra, Airflow, etc.

script/entrypoint.sh — Script to initialize services inside Docker containers

data/ — Sample or test datasets for streaming

requirements.txt — Python dependencies

# Setup & Usage

Clone the repo:
git clone https://github.com/ShaikAmeena2990/Kafka_streaming_project.git

Build and run the Docker containers:
docker-compose up --build

Access the Airflow UI (usually at http://localhost:8080) to monitor and trigger workflows.
Kafka will stream data that Spark consumes and processes in real-time.
Processed data is stored in Cassandra and can be queried as needed.

# How it Works
Kafka topics receive real-time data streams 

Spark Streaming reads data from Kafka topics, applies transformations or analytics.

Results are written to Cassandra for persistent storage.

Airflow schedules and manages workflows to automate tasks such as data ingestion and processing.
