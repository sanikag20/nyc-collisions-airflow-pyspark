# NYC Motor Vehicle Collisions — Airflow + PySpark Data Pipeline

<p align="center">

**An end-to-end big-data processing pipeline for NYC Motor Vehicle Collision data**

Built with **Apache Airflow · Apache Spark · PySpark · Python · Google Cloud Platform**

</p>

---

## Project Overview

This project builds an automated data engineering pipeline for the **NYC Motor Vehicle Collisions** dataset published through NYC Open Data.

The pipeline combines:

* **Apache Airflow** for workflow orchestration
* **PySpark** for large-scale data processing
* **Python** for transformation logic
* **Google Cloud Platform** for execution infrastructure
* **Git/GitHub** for version control and reproducibility

The goal is to transform a large, messy public dataset into a cleaner and more analysis-ready dataset through a repeatable workflow.

---

# Architecture

```text
                    ┌─────────────────────────┐
                    │      NYC Open Data      │
                    │ Motor Vehicle Collisions│
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │      Apache Airflow     │
                    │   Workflow Orchestration │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │        PySpark          │
                    │ Distributed Processing  │
                    └────────────┬────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
                    ▼                         ▼
          ┌──────────────────┐      ┌──────────────────┐
          │ Data Cleaning    │      │ Data Validation  │
          │ & Transformation │      │ & Quality Checks │
          └────────┬─────────┘      └────────┬─────────┘
                   │                         │
                   └────────────┬────────────┘
                                ▼
                    ┌─────────────────────────┐
                    │    Processed Dataset    │
                    │  Analysis-Ready Output   │
                    └─────────────────────────┘
```

---

# Pipeline Flow

The Airflow workflow coordinates the data-processing pipeline.

```text
START
  │
  ▼
┌───────────────────┐
│ Load Source Data  │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ Initialize Spark  │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ Inspect Schema    │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ Clean Data        │
│ • Missing values  │
│ • Invalid values  │
│ • Data types      │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ Transform Data    │
│ • Date processing │
│ • Feature cleanup │
│ • Filtering       │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ Write Output      │
└─────────┬─────────┘
          │
          ▼
        END
```

---

# Technology Stack

| Technology            | Purpose                           |
| --------------------- | --------------------------------- |
| Python                | Pipeline and transformation logic |
| Apache Airflow        | Workflow orchestration            |
| Apache Spark          | Distributed data processing       |
| PySpark               | Python interface for Spark        |
| Google Cloud Platform | Cloud execution environment       |
| Debian Linux          | Runtime environment               |
| Git                   | Version control                   |
| GitHub                | Source-code hosting               |

---

# Dataset

### NYC Motor Vehicle Collisions

The project uses the NYC Open Data **Motor Vehicle Collisions** dataset.

The dataset contains information such as:

* Collision date and time
* Location
* Borough
* Injury information
* Fatality information
* Contributing factors
* Vehicle information
* Geographic information

The dataset is publicly available through NYC Open Data.

> **Note:** The raw dataset is intentionally not included in this repository because of its size. The pipeline can be configured to obtain the source data separately.

---

# Why PySpark?

The dataset contains a large number of records, making it a useful example for distributed data processing.

Instead of relying entirely on local Pandas operations, the project uses **PySpark DataFrames** to demonstrate:

```text
Large Dataset
      │
      ▼
Distributed Spark DataFrame
      │
      ├── Schema Operations
      ├── Filtering
      ├── Column Transformations
      ├── Missing-Value Handling
      └── Data-Type Conversion
      │
      ▼
Processed Dataset
```

This makes the pipeline more scalable than a purely local Python workflow.

---

# Workflow Orchestration with Airflow

Airflow is responsible for managing the pipeline as a reproducible workflow.

The DAG provides:

* Task dependency management
* Execution monitoring
* Retry support
* Logging
* Task-level visibility
* Workflow scheduling
* Execution history

### DAG

```text
                 ┌──────────────┐
                 │    START     │
                 └──────┬───────┘
                        │
                        ▼
              ┌──────────────────┐
              │ Load / Initialize│
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ Spark Processing │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ Data Cleaning    │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ Write Output     │
              └────────┬─────────┘
                       │
                       ▼
                 ┌──────────────┐
                 │     END      │
                 └──────────────┘
```

---

# Repository Structure

```text
nyc-collisions-airflow-pyspark/
│
├── 📁 dags/
│   └── clean_collisions.py
│
├── 📁 data/
│   └── README.md
│
├── 📁 output/
│   └── README.md
│
├── 📁 docs/
│   ├── airflow-grid.png
│   ├── airflow-graph.png
│   └── pyspark-output.png
│
├── 📄 .gitignore
├── 📄 README.md
└── 📄 requirements.txt
```

### Directory descriptions

**`dags/`**

Contains the Airflow DAG responsible for orchestrating the pipeline.

**`data/`**

Reserved for locally downloaded source data. Raw datasets are excluded from Git using `.gitignore`.

**`output/`**

Contains locally generated processed data. Large generated files are also excluded from Git.

**`docs/`**

Contains screenshots and visual documentation of successful pipeline execution.

---

# Getting Started

## 1. Clone the repository

```bash
git clone https://github.com/sanikag20/nyc-collisions-airflow-pyspark.git
```

Move into the project:

```bash
cd nyc-collisions-airflow-pyspark
```

---

## 2. Create a Python virtual environment

```bash
python3 -m venv venv
```

Activate it:

### Linux / macOS

```bash
source venv/bin/activate
```

### Windows

```powershell
venv\Scripts\activate
```

---

## 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

# Install Java for Spark

PySpark requires Java.

Verify Java:

```bash
java -version
```

If Java is not installed on Debian/Ubuntu:

```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
```

Verify:

```bash
java -version
```

---

# Verify PySpark

Run:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

If the installation is successful, the installed Spark version will be displayed.

---

# Airflow Setup

Initialize Airflow:

```bash
airflow db migrate
```

Start the Airflow webserver:

```bash
airflow webserver --port 8080
```

In another terminal, start the scheduler:

```bash
airflow scheduler
```

Copy the DAG into your Airflow DAG directory:

```bash
cp dags/clean_collisions.py ~/airflow/dags/
```

Then open the Airflow UI and trigger the DAG.

---

# Running the Pipeline

Once Airflow is running:

1. Open the Airflow web interface.
2. Locate the NYC collisions DAG.
3. Enable the DAG.
4. Trigger a new DAG run.
5. Monitor the tasks through the **Grid** view.
6. Inspect dependencies through the **Graph** view.
7. Review task logs if required.
8. Verify the generated output.

---

# Pipeline Execution

### Airflow Grid View

![Airflow Grid](docs/airflow-grid.png)

### Airflow Graph View

![Airflow Graph](docs/airflow-graph.png)

### PySpark Output

![PySpark Output](docs/pyspark-output.png)

---

# Data Processing

The pipeline performs several data-processing operations, including:

```text
Raw Data
   │
   ├── Schema inspection
   │
   ├── Missing-value handling
   │
   ├── Data-type conversion
   │
   ├── Date/time processing
   │
   ├── Invalid-record filtering
   │
   ├── Column transformation
   │
   └── Output generation
   │
   ▼
Clean Dataset
```

The transformations are implemented using Spark DataFrame operations rather than collecting the complete dataset into local Python memory.

---

# Results

The pipeline successfully processes the NYC Motor Vehicle Collisions dataset and generates a cleaned dataset suitable for downstream analysis.

### Example validation

```text
Input records
      ↓
Spark DataFrame
      ↓
Cleaning / Transformation
      ↓
Validation
      ↓
Output records
```

Additional runtime statistics and validation results can be added here as the project evolves.

---

# Reproducibility

The project is designed so that another user can reproduce the processing workflow by:

```text
Clone Repository
       ↓
Install Dependencies
       ↓
Obtain Source Dataset
       ↓
Configure Airflow
       ↓
Trigger DAG
       ↓
Run PySpark Processing
       ↓
Inspect Output
```

Environment-specific files, raw data, logs, and generated outputs are excluded from version control.

---

# What Is Not Stored in GitHub?

The repository intentionally excludes:

* Raw datasets
* Generated output files
* Airflow metadata database
* Airflow logs
* Python virtual environments
* Cloud credentials
* API keys
* Service-account credentials
* Other environment-specific files

This keeps the repository lightweight and avoids exposing sensitive information.

---

# Design Decisions

### Why Airflow?

Airflow provides explicit workflow orchestration and makes dependencies between pipeline stages visible.

### Why PySpark?

Spark provides distributed processing capabilities and is better suited to large datasets than processing the complete dataset locally.

### Why GitHub?

GitHub provides version control, reproducibility, documentation, and a centralized location for sharing the project.

### Why GCP?

The pipeline was developed and executed on a cloud VM, providing a realistic environment for experimenting with big-data infrastructure.

---

# Future Improvements

Potential extensions include:

* [ ] Store raw and processed data in Google Cloud Storage
* [ ] Add incremental processing
* [ ] Add automated data-quality checks
* [ ] Add Spark aggregations for analytical workloads
* [ ] Add Airflow retry and failure-alert mechanisms
* [ ] Add unit tests for transformation logic
* [ ] Add Docker-based deployment
* [ ] Add CI/CD using GitHub Actions
* [ ] Add automated pipeline testing
* [ ] Build a dashboard using Streamlit
* [ ] Add monitoring and pipeline performance metrics

---

# Author

**Sanika Gumaste**

M.S. Electrical Engineering
Columbia University

Focused on **Data Science, Machine Learning, Big Data, and AI**.

---

# License

This project is intended for educational and portfolio purposes.
