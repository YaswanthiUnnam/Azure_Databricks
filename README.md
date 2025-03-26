# Azure Databricks for Data Engineers

## Overview  
This project is a hands-on implementation of **Azure Databricks** for data engineering, focusing on building a cloud data platform for **Formula 1 motorsport data**. It covers data ingestion, transformation, orchestration, and storage using:  

- Spark (Python & SQL)  
- Delta Lake  
- Azure Data Lake Gen2  
- Azure Data Factory  
- Azure Key Vault  

## Key Components & Technologies  

- **Azure Databricks** – Unified analytics platform optimized for Azure  
- **Apache Spark** – Distributed compute engine for big data processing  
- **Azure Data Lake Storage Gen2** – Storage layer for raw and processed data  
- **Azure Data Factory (ADF)** – Data pipeline orchestration  
- **Azure Key Vault** – Secure storage for secrets and credentials  
- **Delta Lake** – ACID-compliant storage format for managing big data  

### Features  

- **Databricks Clusters**: Multi-node & single-node setups, runtime optimizations  
- **Data Ingestion**: Supports structured and semi-structured file formats (CSV, JSON, Parquet)  
- **Transformations**: Filtering, joins, aggregations, window functions  
- **Delta Lake**: ACID transactions, time travel, upserts, schema evolution  
- **Spark SQL**: Creating tables, views, and querying data  
- **ADF Integration**: Running notebooks via pipelines, triggers for automation  


## Creating and Managing Azure Databricks Workspace and Clusters

*   Cluster creation in Databricks can be done via the Compute section, offering options for **Multi-node** (with auto-scaling and spot instances) or **Single node** clusters.
*   **Access modes** for clusters include Single User, Shared (Premium only), and No Isolation Shared.
*   Selecting the **Databricks runtime version**, including **LTS (Long Term Support)**, is crucial.
*   Cluster configuration includes node type selection, auto-termination (inactivity timeout), tags, Spark configurations, environment variables, logging, and init scripts.
*   Clusters can be managed (stopped, restarted, cloned, deleted, edited), and libraries can be installed.
*   Monitoring tools include the Event log, Spark User Interface, Driver logs, and Ganglia UI.
*   Cluster pricing is based on Databricks Units (DBU) per hour.

## Databricks Utilities (dbutils)

*   dbutils provides utilities accessible from Python, Scala, and R notebooks for various operations.
*   Common utilities include **File System (dbutils.fs)**, **Secrets (dbutils.secrets)**, **Widget (dbutils.widgets)**, and **Notebook Workflow (dbutils.notebook)**.
*   dbutils.fs allows programmatic interaction with the Databricks File System, offering greater flexibility than the %fs magic command.
*   dbutils.secrets enables retrieving secret values from secret scopes backed by Databricks or Azure Key Vault.
*   Help on dbutils and its methods can be accessed using dbutils.help().

## Azure Data Lake Storage Gen2

*   Creating an ADLS Gen2 account in Azure involves enabling the **hierarchical namespace**, which differentiates it from standard Blob Storage and allows for folder structures and fine-grained access control.
*   Containers (equivalent to folders) are used to organise files.
*   Access to ADLS Gen2 can be managed through **Access Keys** (full access, handle with care, Key Vault recommended), **SAS Tokens** (granular control over resources, permissions, time, IP), and **Service Principals** (Azure AD applications with RBAC permissions, recommended for automated tools).
*   The **ABFS (Azure Blob File System)** driver (abfss://) is recommended for accessing data in ADLS Gen2 from Databricks.
*   **AAD Credential Passthrough** (Premium tier) allows Databricks to use the user's Azure AD credentials to access the storage account based on their permissions.
*   **Mounting** ADLS Gen2 to the Databricks workspace using dbutils.fs.mount() allows accessing data using file system semantics instead of long URLs, typically done with Service Principals.
*   **dbutils.fs.mounts()** lists existing mounts, and **dbutils.fs.unmount()** removes a mount.
*   A function-based approach can be used to mount multiple containers efficiently.

## Recommended Access Patterns for this Project

*   For **student or restricted company subscriptions**, **Cluster Scoped Authentication with Access Keys** is recommended for full access and ease of setup.
*   For subscriptions with **Azure Active Directory access**, using a **Service Principal** is preferred to enable storing credentials in Key Vault and mounting storage.

## Project Structure

```
├── notebooks/                         # Databricks notebooks for data ingestion & transformation
│   ├── 01-DataIngestion.py             # Ingest data from ADLS Gen2
│   ├── 02-DataTransformation.py       # Apply transformations (joins, aggregations, window functions)
│   ├── 03-DeltaLake.py                 # Implement Delta Lake operations
│   ├── 04-SparkSQL.py                  # Utilize Spark SQL for querying
│   └── 05-Orchestration.py             # ADF pipeline integration with Databricks
├── data/                              # Sample data files (CSV, JSON, Parquet)
├── scripts/                           # Supporting scripts (PowerShell, Python, etc.)
└── pipeline/                          # Azure Data Factory pipeline JSON files
```

## Azure Key Vault and Secret Scopes

*   **Azure Key Vault** is used to securely store secrets like **access keys**, **SAS tokens**, and **service principal** credentials.
*   A **Databricks Secret Scope** links to an Azure Key Vault, allowing secure access to secrets within Databricks notebooks.
*   The **#secrets/createScope** command reveals the UI for creating secret scopes, linking them to Key Vault using the Vault URI and Resource ID.
*   **dbutils.secrets.get()** is used to retrieve secret values at runtime, avoiding hardcoding.

## Data Ingestion

- Ingesting raw data from ADLS Gen2 using Spark’s DataFrame API  
- Handling CSV, JSON, and Parquet formats  
- Schema definition and dynamic partitioning  
- Writing raw data to a staging layer  

*   Key steps include defining schemas (using **StructType/StructFields** or DDL strings), reading files with header and schema options, selecting and renaming columns (**select(), withColumnRenamed()**), adding audit columns (**withColumn() with current_timestamp() and lit()**), and writing data to Parquet format (**write.parquet() with overwrite mode**) in the raw container.
*   Specific handling for multi-line JSON (multiLine option) and reading from folders with multiple files is demonstrated.
*   Incremental load strategies using partitioning and dynamic overwrite are introduced.

## Data Transformation

- Using Spark SQL & DataFrame transformations to clean and enrich data  
- Implementing joins, aggregations, and window functions  
- Writing transformed data to Delta Lake

*   **Filter Transformation** (**filter() or where()**) allows selecting data based on conditions, using both Pythonic and SQL syntax.
*   **Join Transformation** (join()) combines data from multiple DataFrames based on join conditions and types (**inner, left outer, right outer, full outer, semi, anti, cross**).
*   **Aggregation** (**groupBy() and agg()** with functions like **sum(), count(), avg()**) is used to derive insights, demonstrated with driver and constructor standings.
*   **Window Functions** (**partitionBy() and orderBy() with ranking functions like rank()**) enable calculations across a set of rows related to the current row, used for ranking drivers.

## Spark SQL

*   DataFrames can be accessed using SQL by creating **Temporary Views** (**createTempView() or createOrReplaceTempView()**) valid within the Spark session or **Global Temporary Views** (**createOrReplaceGlobalTempView()**) accessible across notebooks on the same cluster.
*   **Managed Tables** are created and managed by Spark, with data stored in the warehouse location. Dropping a managed table deletes both metadata and data.
*   **External Tables** are created with a user-specified location for the data. Dropping an external table only removes the metadata, leaving the data in place.
*   **Views** are virtual representations of data from tables, created using SQL (**CREATE VIEW or CREATE OR REPLACE VIEW**).
*   External tables can be created for various file formats (CSV, JSON, Parquet) by specifying the format and path in the **CREATE EXTERNAL TABLE** statement.
*   Managed tables in the processed and presentation layers are created using the **.saveAsTable()** method.
*   Spark SQL can be used for data analysis, including calculating consistent race points and identifying dominant drivers and teams.

## Delta Lake

*   Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing on data lakes.
*   Data can be read and written to Delta Lake using the delta format in Spark read/write operations, similar to Parquet.
*   Managed and external Delta tables can be created.
*   Partitioning Delta tables is done using **.partitionBy()**.
*   Delta Lake supports **updates** (UPDATE), **deletes** (DELETE), and **merges/upserts** (MERGE INTO).
*   **Time Travel** allows querying historical versions of data using VERSION AS OF or TIMESTAMP AS OF.
*   **Vacuum** (VACUUM) removes historical versions of data, important for GDPR compliance.
*   The **Delta Lake transaction log** (_delta_log folder) stores metadata about each change as JSON files, enabling versioning and time travel. Checkpoint files consolidate log information for efficiency.
*   Parquet tables can be converted to Delta tables using CONVERT TO DELTA.
*   Transformation notebooks are converted to read from and write to Delta tables using the delta format.
*   SQL notebooks can also leverage Delta Lake features for incremental loads using merge statements.

## Azure Data Factory (ADF) for Orchestration

*   Azure Data Factory is used to create, schedule, and monitor data pipelines.
*   Pipelines contain Activities, such as Databricks Notebook activity to run Databricks notebooks.
*   A **Linked Service** is required to connect to Azure Databricks, specifying the workspace and cluster (using existing interactive cluster or job cluster). **Managed Identity** is a recommended authentication method.
*   Notebooks and their parameters can be configured in the Databricks Notebook activity settings, using dynamic content to pass pipeline parameters to notebook widgets.
*   Pipelines can be **debugged** by manually providing parameter values.
*   Changes to ADF components (pipelines, linked services, datasets) need to be **published** to be saved.
*   Pipelines can be created to ingest all data files and improved to handle missing files using **Get Metadata** and **If Condition** activities.
*   Transformation pipelines execute transformation notebooks, often dependent on the successful completion of ingestion pipelines. **Execute Pipeline** activity can be used to chain pipelines in a master pipeline.
*   **Triggers** are used to schedule pipeline runs. **Tumbling Window Triggers** are suitable for recurring weekly schedules, allowing parameterisation (window end time) to be passed to pipelines.
*   Trigger runs and pipeline runs can be monitored in the ADF Monitor tab.

# Conclusion
This project demonstrates the power of Azure Databricks and Delta Lake in building a scalable data engineering pipeline. It integrates well with Azure Data Factory for orchestration, ADLS Gen2 for storage, and Key Vault for secure credentials management.

