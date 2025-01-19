# Data Transformation Pipeline with GCS, Snowflake, Dataproc, and Airflow
This repository contains a fully automated data transformation pipeline leveraging Google Cloud Storage (GCS), Snowflake, Dataproc, Apache Airflow, and PySpark. The pipeline ingests real-time news data, transforms it using a Spark job, and loads the raw and transformed data into Snowflake for further analysis.

# Key Features:

Real-time Data Ingestion: Fetches news data via API and stores it in GCS.
Data Transformation: Uses PySpark to transform raw data and stores it back in GCS.
Airflow Orchestration: Automates the entire pipeline, including Dataproc cluster creation, Spark job execution, and data loading into Snowflake.
Snowflake Integration: Data is transferred from GCS to Snowflake for efficient storage and analysis.
