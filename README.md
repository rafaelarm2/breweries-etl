# Breweries ETL

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline for brewery-related data, following the medallion architecture (bronze, silver, gold layers). It uses Apache Airflow for orchestration, Python scripts for transformation, and Docker for reproducibility. Monitoring is provided via Prometheus and Grafana.

## Assessment Mapping

This solution addresses the BEES Data Engineering – Breweries Case as follows:

- **API Consumption:** Fetches brewery data from the [Open Brewery DB API](https://www.openbrewerydb.org/).
- **Orchestration:** Uses Apache Airflow for scheduling, retries, and error handling.
- **Language:** Python for extraction and transformation.
- **Containerization:** All components run in Docker containers via `docker-compose`.
- **Medallion Architecture:**
  - **Bronze Layer:** Raw API data stored as-is (JSON or CSV).
  - **Silver Layer:** Data transformed to Parquet, partitioned by brewery location (`state` and `city`). Transformations include type normalization (e.g., standardizing brewery types), handling missing values (e.g., filling or flagging nulls), and data cleaning (e.g., trimming whitespace, correcting known data issues).
  - **Gold Layer:** Aggregated analytics—brewery counts by type and location. Output is stored as Parquet and CSV files in the `data/gold/` directory.
- **Monitoring/Alerting:** Pipeline and data quality monitored with Prometheus and Grafana. Airflow alerts on failures.
- **Testing:** Includes unit tests for extraction and transformation logic.
- **Documentation:** This README explains design choices, trade-offs, and usage.

## Architecture & Design

- **Bronze Layer:** Stores raw API responses for traceability and reprocessing.
- **Silver Layer:** Converts raw data to Parquet format, partitions by `state` and `city`, and applies data cleaning (type normalization, missing value handling, whitespace trimming, and correction of known data issues).
- **Gold Layer:** Aggregates brewery counts by type and location for analytics, storing results as Parquet and CSV files in `data/gold/`.
- **Orchestration:** Airflow DAG manages extraction, transformation, and loading, with retries and error handling.
- **Retry Logic:** Each Airflow task is configured with automatic retry logic. If a task fails (e.g., due to a transient API/network error), Airflow will automatically retry the task a configurable number of times with a delay between attempts. This increases pipeline robustness and minimizes manual intervention for intermittent failures.
- **Monitoring:** Prometheus scrapes Airflow and custom metrics; Grafana dashboards visualize pipeline health and data quality.
- **Containerization with Docker:** All components (Airflow, monitoring stack, and supporting services) are containerized using Docker. The project includes a `Dockerfile` for custom images and a `docker-compose.yml` file to orchestrate multi-container deployment, ensuring consistent environments and easy local or cloud setup.

## Architectural Decisions

### Orchestration with Airflow

Apache Airflow was chosen as the orchestration tool for its robust scheduling, dependency management, and built-in support for retries and error handling. Airflow's UI provides clear visibility into pipeline execution and simplifies monitoring and troubleshooting. Its extensibility and strong community support make it a standard for production-grade data pipelines.

### Data Transformation with Python (pandas)

Data extraction and transformation are implemented using Python and pandas. This choice is motivated by the relatively low volume of data returned by the Open Brewery DB API, which does not require the distributed processing capabilities of Apache Spark. Using pandas ensures fast development, easier debugging, and a smaller resource footprint, making the pipeline lightweight and efficient for this use case.

### Why Not Spark?

Apache Spark is a powerful distributed processing engine, but it is best suited for large-scale data processing. In this project, the data volume from the Open Brewery DB API is modest and can be efficiently handled in-memory with pandas. Introducing Spark would add unnecessary complexity and overhead without tangible benefits for this scale.

## Features

- **Airflow DAGs** for orchestrating ETL jobs with scheduling, retries, and error handling
- **Python scripts with pandas** for data transformation and modeling
- **Docker** and **docker-compose** for modular, reproducible deployment (used to run Airflow, Prometheus, Grafana, and supporting services as containers)
- **Medallion data lake**: bronze (raw), silver (curated, partitioned), gold (aggregated)
- **Monitoring** with Prometheus and Grafana
- **Configurable** via environment variables and config files
- **Unit tests** for extraction and transformation logic

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) *(used to run all services as containers)*
- [Docker Compose](https://docs.docker.com/compose/) *(used to orchestrate multi-container setup)*
- [Poetry](https://python-poetry.org/) (for local development)
- **GNU Make** (to use `make start_services`; install with `sudo apt-get install make` on Ubuntu)

### Quick Start

1. **Clone the repository:**
   ```
   git clone https://github.com/yourusername/breweries-etl.git
   cd breweries-etl
   ```

2. **Set up environment variables:**

   Copy `.env.example` to `.env` and adjust as needed.

3. **Start the services:**
   ```
   make start_services
   ```
   *(This command uses Docker Compose to build and start all containers: Airflow, Prometheus, Grafana, and supporting services.)*

4. **Access Airflow:**
   - Visit http://localhost:8080 (default credentials: `airflow`/`airflow`).

5. **Run the ETL Pipeline:**
   - In the Airflow UI, find the `brewery_etl` DAG.
   - Toggle the DAG "On" to enable scheduling, or click the "Play" (Trigger DAG) button to run it manually.
   - The data will be stored locally in the data/dlh folder

6. **Access Monitoring:**
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000 (default credentials: `admin`/`admin`)
   - **Prometheus Example Query:**  
     To view the number of successful Airflow DAG runs, use:
     ```
     brewery_etl_operations_total{status="success"}
     ```
   - **Grafana Instructions:**  
     1. Go to http://localhost:3000 and log in with the default credentials.
     2. In the left sidebar, click on "Dashboards" > "Browse".
     3. Select the "Airflow Monitoring" dashboard (or the dashboard you have configured).
     4. Explore panels for DAG/task status, data ingestion volume, quarantine counts, and data freshness.

## Data Lake Structure

- **Bronze Layer:** `data/bronze/` — raw API data (JSON/CSV)
- **Silver Layer:** `data/silver/` — Parquet files partitioned by `state` and `city`
- **Gold Layer:** `data/gold/` — Aggregated analytics (brewery counts by type/location) as Parquet and CSV
- By default, the `data/` directory is mounted as a Docker volume for persistence.
- Storage locations can be configured in Airflow configs.

## Quarantine Handling

Records that fail validation or have data quality issues during transformation are not discarded. Instead, they are moved to a **quarantine area** (`data/quarantine/`). This allows for later inspection, debugging, and potential reprocessing, ensuring traceability and preventing silent data loss.

## Development

- **Install dependencies:**
   ```
   poetry install
   ```

- **Run tests:**
   ```
   poetry run pytest
   ```
   Test cases cover API extraction and transformation logic.

## Test Cases

Unit tests cover API extraction and transformation logic, including:

- Extraction: Ensures correct API pagination, error handling, and data schema.
- Transformation: Validates type normalization, missing value handling, and partitioning logic.
- Quarantine: Tests that invalid records are correctly routed to the quarantine area.

### Running Tests

To run all tests:

```
poetry run pytest
```

Test files are located in the `tests/` directory. See individual test files for specific cases and coverage.

## Monitoring & Alerting

- **Pipeline Monitoring:** Airflow provides task-level monitoring and logs. Prometheus scrapes Airflow and custom metrics.
- **Alerting:** Airflow can be configured to send alerts on task failures. Grafana dashboards visualize pipeline health and data quality.
- **Data Quality:** Data validation checks are included in the transformation steps; failures are logged and can trigger alerts.

### Grafana Dashboard Metrics

The Grafana dashboards provide real-time visibility into pipeline health and data quality, including:

- **DAG/task status:** Success/failure rates, duration, and retries for Airflow tasks.
- **Data ingestion volume:** Number of records ingested per run/layer.
- **Quarantine counts:** Number of records sent to quarantine due to validation errors.
- **Data freshness:** Time since last successful pipeline run.
- **Custom alerts:** Triggered on pipeline failures, high quarantine rates, or data anomalies.

These metrics help quickly identify issues, monitor trends, and ensure reliable data delivery.

## Design Choices & Trade-offs

- **Medallion Architecture:** Enables traceability, reprocessing, and efficient analytics.
- **Partitioning:** Silver layer partitioned by location for efficient querying and scalability.
- **Containerization:** Ensures reproducibility and easy setup.
- **Error Handling:** Airflow retries failed tasks and logs errors for debugging.
- **Monitoring:** Prometheus and Grafana provide observability into pipeline health and data quality.

## Alternative Architecture: If I Could Choose the Stack

If there were no constraints on the technology stack, I would design the pipeline as follows:

### Modern Data Stack Approach

- **Airbyte** for managed, scalable ingestion from the Open Brewery DB API and other sources, providing robust connectors and easy schema evolution. It simplifies and standardizes data ingestion, with built-in connectors, incremental syncs, and schema management.
- **Google BigQuery** as the cloud data warehouse and data lake, offering serverless scalability, strong partitioning, and low operational overhead. 
- **dbt (Data Build Tool)** for all data transformation and modeling, leveraging SQL and version-controlled analytics engineering best practices. It enables modular, testable, and maintainable transformations directly in the warehouse, with built-in data quality checks and documentation.
- **Apache Airflow** remains the orchestrator, integrating ingestion (Airbyte) and transformation (dbt) workflows, and providing monitoring and alerting.

This architecture would maximize scalability, minimize operational burden, and align with modern data engineering best practices, especially for production-grade, cloud-native analytics pipelines.

## License

This project is licensed under the Apache License 2.0.

---