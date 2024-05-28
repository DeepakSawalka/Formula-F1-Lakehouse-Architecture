<a name="readme-top"></a>

## Project Title: Formula1 - Data Engineering with Medallion Architecture
Welcome to the Formula 1 Data Engineering project repository! This project encapsulates a robust solution architecture leveraging cutting-edge data engineering technologies to process, analyze, and present Formula 1 racing data.

## Project Overview
This project aims to design and implement a robust data engineering solution leveraging the Medallion Architecture and an automated data pipeline. The objective is to seamlessly process raw Formula 1 racing data, transforming it into actionable insights for analysis and presentation. Our solution integrates multiple Azure services such as Azure Databricks, Spark Core, Azure Data Lake Gen2, Delta Lake, Azure Data Factory, Unity Catalog, and Power BI to ensure scalability, reliability, and performance.

## Solution Architecture

Below is a detailed Architecture Diagram illustrating the comprehensive setup and flow of the project:

<p align="center">
<img src="Architecture_Diagram.png" />
</p>

## Data Flow

### 1. Data Ingestion

I ingest data from the Ergast API, which is then stored in the raw (Bronze layer) container within Azure Data Lake Storage Gen 2(ADLS). The data ingestion includes both full loads and incremental loads:

- **Full Load Data:**
    - Circuits (CSV)
    - Races (CSV)
    - Constructors (JSON)
    - Drivers (JSON)

    These datasets are static and do not change frequently.

- **Incremental Load Data:**
    - Results (JSON)
    - Pitstops (JSON)
    - Lap Times (Split CSV Files)
    - Qualifying (Split JSON Files)

    These datasets are dynamic, receiving new data with each new race.

### 2. Data Processing

Using Azure Databricks and Spark Core with PySpark and Spark SQL, I process the ingested data. The processed data is then moved to the Silver layer in Delta Lake. This step includes:

    - Data cleaning and transformation.
    - Ensuring data quality and consistency.

### 3. Data Aggregation

In the Silver layer, I perform further data aggregation, including:

    - GroupBy operations
    - Window functions

The aggregated data is then moved to the Gold layer for business purposes and ad-hoc analysis.

### 4. Data Warehousing 

I utilize Databricks SQL to bring data warehousing capabilities and enhance performance. This allows us to run complex queries and perform data analysis directly on our delta lake.

### 5. Data Visualization

Processed and aggregated data is connected to Power BI for visualization, enabling insightful and interactive dashboards for data analysis.

### 6. Automation

Azure Data Factory orchestrates the data pipeline by:

    1. Automating the execution of Databricks notebooks.
    2. Adding triggers for periodic execution to ensure timely data processing.

### 7. Data Governance

I employed Unity Catalog for comprehensive data governance, including:

    1. Data Discovery
    2. Data Audit
    3. Data Lineage
    4. Data Access Control

### 8. Version Control

I used Azure DevOps for version control capabilities.

### 9. Security and Compliance

Azure Key Vault securely manages secrets, keys, and certificates, ensuring data security.

### 10. User Management

Microsoft Entra ID offers single sign-on (SSO) for Azure Databricks users, facilitating:

    1. Automated user provisioning
    2. Access level assignments
    3. User removal and access denial

### 11. Monitoring and Performance

Azure Monitor collects and analyzes telemetry from Azure resources, proactively identifying issues to maximize performance and reliability.

### 12. Cost Management

Microsoft Cost Management provides financial governance services, optimizing Azure workload costs.



