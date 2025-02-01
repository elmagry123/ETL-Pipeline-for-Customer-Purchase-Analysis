# ETL Pipeline for Customer Purchase Analysis

This project is an ETL (Extract, Transform, Load) pipeline designed to analyze customer purchase data. It focuses on identifying customers who bought AirPods after purchasing an iPhone and customers who only bought AirPods and iPhones. The project follows the **Factory Pattern Design** to create flexible and reusable ETL components.

## Table of Contents
- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Installation](#installation)
- [Usage](#usage)
- [Workflows](#workflows)
- [Contributing](#contributing)
- [License](#license)

## Project Overview
The project consists of the following components:

- **Extractor**: Reads data from various sources (CSV, Parquet, Delta tables).
- **Transformer**: Applies transformation logic to filter and process the data.
- **Loader**: Saves the transformed data to different sinks (DBFS, Delta tables).
- **Workflows**: Combines the extract, transform, and load steps into reusable pipelines.
- **Factory Pattern Design**: Used to create Extractor, Transformer, and Loader objects dynamically, improving code maintainability and scalability.

The two main workflows are:

1. **First Workflow**: Identifies customers who bought AirPods after purchasing an iPhone.
2. **Second Workflow**: Identifies customers who only bought AirPods and iPhones.

## Technologies Used
- **Python**: Primary programming language.
- **PySpark**: Used for distributed data processing.
- **Delta Lake**: For managing and querying Delta tables.
- **DBFS**: Databricks File System for storing intermediate and final results.
- **Factory Pattern Design**: Implemented to manage ETL components dynamically.

## Installation
To run this project, you need the following:

### Databricks Environment
Ensure you have access to a Databricks workspace.

### Dependencies
Install the required dependencies using the following commands:

```bash
pip install pyspark
git install delta-spark
```

## Usage
### Running the Workflows

#### First Workflow
To identify customers who bought AirPods after purchasing an iPhone, run the following:

```python
workflow_runner = WorkFlowRunner("firstWorkFlow")
workflow_runner.runner()
```

#### Second Workflow
To identify customers who only bought AirPods and iPhones, run the following:

```python
workflow_runner = WorkFlowRunner("secondWorkFlow")
workflow_runner.runner()
```

## Input Data
- **Transaction Data**: CSV file located at `dbfs:/FileStore/tables/Transaction_Updated.csv`.
- **Customer Data**: Delta table named `default.customer_delta_table`.

## Output Data

- **First Workflow**: Results are saved to:
  - DBFS: `dbfs:/FileStore/tables/AirPodsData`

- **Second Workflow**: Results are saved to:
  - DBFS: `dbfs:/FileStore/tables/airpodsOnlyIphone` (partitioned by location)
  - Delta Table: `default.OnlyAirPodsAndIphone`

## Workflows
### 1. First Workflow: AirPods After iPhone
- **Extractor**: Reads transaction and customer data.
- **Transformer**: Filters customers who bought AirPods after purchasing an iPhone.
- **Loader**: Saves the results to DBFS.

### 2. Second Workflow: Only AirPods and iPhone
- **Extractor**: Reads transaction and customer data.
- **Transformer**: Filters customers who only bought AirPods and iPhones.
- **Loader**: Saves the results to DBFS (partitioned by location) and a Delta table.

## Contributing
Contributions are welcome! If you'd like to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a detailed description of your changes.

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.
