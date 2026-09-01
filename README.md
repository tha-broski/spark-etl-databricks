# Spark ETL Project

> Production-style batch ETL pipeline built with **PySpark**, **Delta Lake**, and **Databricks**.

This project processes e-commerce data through a Medallion Architecture and demonstrates practical Data Engineering patterns such as snapshot loads, incremental upserts, soft deletes, quarantine handling, idempotent processing, control tables, automated validation, and Databricks Jobs deployed with Asset Bundles.

---

## Architecture

```text
Raw CSV files
      |
      v
Unity Catalog Volume
      |
      v
   Bronze
      |
      v
Transformations
+ Data Quality
   /      \
  v        v
Silver  Quarantine
  |
  v
 Gold
```

The Databricks workflow is orchestrated as:

```text
tests
  |
  v
precheck
  |
  v
etl_pipeline
  |
  v
postcheck
```

The workflow is version-controlled and deployed using **Databricks Asset Bundles**.

---

## Tech Stack

- Python
- PySpark
- Delta Lake
- Databricks
- Databricks Serverless
- Unity Catalog
- Databricks Volumes
- Databricks Jobs
- Databricks Asset Bundles
- pytest
- Git / GitHub

---

## Data Sources

The project processes four e-commerce entities.

### Products

Snapshot source.

```text
product_id
name
category
price
stock_quantity
```

### Customers

Snapshot source.

```text
customer_id
first_name
last_name
email
country
registration_date
```

### Orders

Incremental source.

```text
order_id
customer_id
order_date
status
```

### Order Items

Incremental source.

```text
order_item_id
order_id
product_id
quantity
unit_price
```

---

## Medallion Architecture

### Bronze

Raw CSV files are stored in a Unity Catalog Volume:

```text
/Volumes/workspace/bronze/raw
```

During ingestion, the pipeline adds technical metadata:

```text
ingestion_timestamp
source_file
batch_id
```

Bronze data is stored as managed Delta tables in Unity Catalog.

Example:

```text
workspace.bronze.products
```

### Silver

The Silver layer contains cleaned, typed, and validated business data.

Two loading strategies are used.

#### Snapshot processing

Used for:

- products
- customers

The pipeline supports:

- inserts for new entities
- updates for changed entities
- no-op behavior for unchanged entities
- soft deletes for entities missing from the latest snapshot
- reactivation when previously inactive entities return

Soft deletion is represented with:

```text
is_active = false
```

Invalid records do not cause accidental soft deletes. Snapshot IDs are derived from the complete validated batch before splitting records into valid and invalid datasets.

#### Incremental processing

Used for:

- orders
- order_items

The pipeline uses Delta Lake `MERGE` to:

- insert new records
- update changed records
- skip unchanged records
- preserve records missing from the current incremental batch

---

## Data Quality

Each entity has dedicated validation rules.

Examples include:

- duplicate IDs
- null or invalid IDs
- empty required fields
- invalid email format
- invalid dates
- negative prices
- invalid quantities
- unsupported order statuses

Invalid records are written to dedicated quarantine Delta tables.

Example:

```text
workspace.quarantine.products
```

Quarantine loading is idempotent for the same combination of:

```text
batch_id + entity_id
```

---

## Retry-Safe and Idempotent Processing

The pipeline maintains an append-only control table:

```text
workspace.control.processed_files
```

Each source file is identified using a SHA256 hash.

Processing states include:

```text
BRONZE_WRITTEN
SUCCESS
```

Before processing a file, the pipeline checks its latest state.

```text
SUCCESS
-> skip file

BRONZE_WRITTEN
-> resume processing using the existing batch

new file
-> ingest to Bronze and start processing
```

This prevents duplicate ingestion when the pipeline is run multiple times or retried after a partial failure.

---

## Gold Layer

Gold datasets are rebuilt from Silver data and use completed orders only.

### Daily Sales

Grain: `one row per day`

Metrics:

- order count
- items sold
- revenue
- average order value

### Product Performance

Grain: `one row per product`

Metrics:

- order count
- items sold
- revenue
- average selling price

### Category Performance

Grain: `one row per category`

Metrics:

- order count
- items sold
- revenue
- average selling price

### Customer Metrics

Grain: `one row per customer`

Metrics:

- order count
- items bought
- total spent
- first order date
- last order date
- average order value

Revenue is calculated from the historical price stored in `order_items.unit_price`.

---

## Databricks Setup

The pipeline runs on **Databricks Serverless** and uses **Unity Catalog** to organize data.

Schemas:

```text
workspace.bronze
workspace.silver
workspace.gold
workspace.quarantine
workspace.control
workspace.test
```

Raw input files are stored in:

```text
/Volumes/workspace/bronze/raw
```

Processed datasets are stored as managed Delta tables.

---

## Databricks Workflow

The production workflow contains four tasks:

```text
tests
  |
  v
precheck
  |
  v
etl_pipeline
  |
  v
postcheck
```

### Tests

Runs the automated pytest suite before the pipeline starts.

The test suite covers:

- transformations
- data quality validation
- Gold aggregations
- snapshot Silver loading
- incremental Silver loading
- soft delete and reactivation logic
- quarantine idempotency

If any test fails, the remaining workflow tasks are not started.

### Precheck

Validates that:

- required raw CSV files are accessible
- required Unity Catalog schemas exist

If the precheck fails, the ETL task is not started.

### ETL Pipeline

Runs the full pipeline:

```text
Bronze
-> transformations
-> data quality
-> Silver
-> Quarantine
-> Gold
```

Retry handling is enabled for transient failures.

### Postcheck

Validates that:

- required Gold tables exist
- Gold tables are not empty
- the latest processing status for every source file is `SUCCESS`

The postcheck uses a Spark window function to select the latest status for each `file_hash`.

---

## Databricks Asset Bundles

Workflow configuration is stored as code.

```text
databricks.yml
resources/
└── jobs.yml
```

The bundle defines:

- workflow tasks
- task dependencies
- serverless execution environment
- retry policy
- retry delay
- schedule

The project is packaged as a Python wheel for Databricks execution.

Packaging configuration is defined in:

```text
pyproject.toml
```

The Asset Bundle builds the wheel during deployment and installs it in the Databricks job environment together with pytest.

Example `databricks.yml`:

```yaml
bundle:
  name: spark-etl-project

include:
  - resources/*.yml

artifacts:
  default:
    type: whl
    path: .
    build: python -m pip wheel . -w dist --no-deps

targets:
  dev:
    mode: development
    default: true
```

Deploying the bundle creates or updates the Databricks Job from version-controlled configuration.

---

## Project Structure

```text
spark-etl-databricks/
├── databricks.yml
├── pyproject.toml
├── resources/
│   └── jobs.yml
├── src/
│   ├── config/
│   │   └── paths.py
│   ├── control/
│   │   └── processed_files.py
│   ├── gold/
│   │   ├── category_performance.py
│   │   ├── customer_metrics.py
│   │   ├── daily_sales.py
│   │   └── product_performance.py
│   ├── ingestion/
│   │   ├── bronze.py
│   │   └── csv_reader.py
│   ├── loading/
│   │   ├── quarantine.py
│   │   └── silver.py
│   ├── pipelines/
│   │   ├── customers_pipeline.py
│   │   ├── gold_pipeline.py
│   │   ├── order_items_pipeline.py
│   │   ├── orders_pipeline.py
│   │   └── products_pipeline.py
│   ├── quality/
│   ├── schemas/
│   ├── transformations/
│   ├── utils/
│   ├── main.py
│   ├── precheck.py
│   ├── postcheck.py
│   └── run_tests.py
├── tests/
│   ├── conftest.py
│   ├── gold/
│   ├── loading/
│   ├── quality/
│   └── transformations/
├── pytest.ini
├── requirements.txt
└── README.md
```

---

## Testing

The project includes tests for:

- transformations
- data quality rules
- Gold aggregations
- snapshot Silver loading
- incremental Silver loading
- soft delete and reactivation logic
- quarantine idempotency

The Databricks workflow runs the pytest suite as the first task, followed by precheck and postcheck runtime validations.

Tests are executed inside the Databricks job environment with the project installed as a wheel.

---

## Running in Databricks

Expected raw files:

```text
products.csv
customers.csv
orders.csv
order_items.csv
```

Expected location:

```text
/Volumes/workspace/bronze/raw
```

Required Unity Catalog schemas:

```text
bronze
silver
gold
quarantine
control
test
```

The Databricks Job executes:

```text
tests
-> precheck
-> etl_pipeline
-> postcheck
```

The workflow can be triggered manually or by schedule.

---

## Engineering Concepts Demonstrated

This project demonstrates practical use of:

- Medallion Architecture
- batch ETL
- snapshot ingestion
- incremental ingestion
- Delta Lake `MERGE`
- idempotent processing
- retry-safe pipelines
- soft deletes
- entity reactivation
- data quality validation
- quarantine handling
- control tables
- Unity Catalog
- managed Delta tables
- Databricks Volumes
- Databricks Serverless
- Databricks Jobs
- task dependencies
- scheduled execution
- runtime prechecks and postchecks
- Databricks Asset Bundles
- Python wheel packaging
- pytest-based automated testing

---

## Project Goal

The project was designed as a portfolio-grade Data Engineering pipeline rather than a minimal tutorial example. The focus is on patterns commonly used in production batch-processing systems: reliable ingestion, state tracking, data quality, Delta Lake upserts, layered data modeling, orchestration, automated testing, and reproducible Databricks deployment.
