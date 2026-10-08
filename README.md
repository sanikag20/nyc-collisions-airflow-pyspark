# NYC Motor Vehicle Collisions: Airflow + PySpark Pipeline

## Overview

This project implements an automated big-data processing pipeline for the NYC Motor Vehicle Collisions dataset from NYC Open Data.

The pipeline uses **Apache Airflow** for workflow orchestration and **Apache Spark (PySpark)** for distributed data processing and transformation.

## Dataset

**NYC Motor Vehicle Collisions**

Source: NYC Open Data

The dataset contains information about motor vehicle collisions reported in New York City, including collision dates, locations, contributing factors, vehicle information, and injury/fatality statistics.

## Architecture

```text
NYC Open Data
      |
      v
 Apache Airflow
      |
      v
  PySpark
      |
      v
Data Cleaning & Transformation
      |
      v
Processed Dataset
```

## Pipeline

The Airflow DAG performs the following steps:

1. Loads the NYC collisions data.
2. Reads the data using PySpark.
3. Performs data cleaning and preprocessing.
4. Handles missing and invalid values.
5. Applies schema and data-type transformations.
6. Produces a cleaned dataset for downstream analysis.

## Technologies

* Python
* Apache Airflow
* Apache Spark / PySpark
* Google Cloud Platform
* NYC Open Data
* Linux
* Git / GitHub

## Project Structure

```text
nyc-collisions-airflow-pyspark/
│
├── dags/
│   └── clean_collisions.py
│
├── data/
│   └── README.md
│
├── output/
│   └── README.md
│
├── requirements.txt
├── .gitignore
└── README.md
```

## Running the Pipeline

The project was developed and executed in a Google Cloud VM running Debian Linux.

The Airflow DAG can be placed in the Airflow `dags/` directory and triggered through the Airflow web interface or CLI.

## Results

The pipeline successfully executes the PySpark transformation workflow and produces a cleaned version of the NYC Motor Vehicle Collisions dataset.

## Future Improvements

* Add data quality checks and validation tasks.
* Store processed data in Google Cloud Storage.
* Add analytical aggregation tasks using Spark.
* Introduce Airflow task-level monitoring and alerting.
* Extend the pipeline for incremental data processing.
