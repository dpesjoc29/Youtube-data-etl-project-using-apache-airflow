# Youtube Data ETL Project with Apache Airflow

## Overview

This project is an Extract, Transform, and Load (ETL) process that extracts data from YouTube videos using the YouTube API. The extracted data includes video ID, title, like count, comment count, and comment text. The ETL process is orchestrated using Apache Airflow, and the transformed data is loaded into a CSV file, which is then stored in an AWS S3 bucket.

## Requirements

Before getting started, make sure you have the following prerequisites installed:

- [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install)
- [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation.html)
- AWS Free Tier Services
- Youtube API key

## Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/dpesjoc29/Simple-ETL-project-using-Airflow.git
   cd your-repo
   
2. Install Python dependencies:

   pip install -r requirements.txt

3. Configure Airflow:

    Update the airflow.cfg file in the config folder with your specific configurations.

4. Start Airflow:

    airflow standalone

Access the Airflow web UI at http://localhost:8080 to monitor and trigger the DAG.

5. Run the ETL process

    Trigger the ETL process by activating the Airflow DAG (Directed Acyclic Graph) using the Airflow UI.


## Directpry Structure

- /your-repo
  - /dags
    - youtube_dag.py
  - .env
  - extract.py
  - transform.py
  - requirements.txt
  - README.md
  - airflow.cfg






