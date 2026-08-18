# 11 - Data Architecture Patterns

## Table of Contents
1. [Modern Data Architecture](#1-modern-data-architecture)
2. [Data Mesh](#2-data-mesh)
3. [Data Fabric](#3-data-fabric)
4. [Data Integration Patterns](#4-data-integration-patterns)
5. [Interview Questions](#5-interview-questions)

---

## 1. Modern Data Architecture

### Data Lakehouse Architecture

`
+--------------------------------------------------+
|              PRESENTATION LAYER                   |
|  BI Tools  |  ML Platforms  |  Data Apps         |
+--------------------------------------------------+
                        |
+--------------------------------------------------+
|              PROCESSING LAYER                     |
|  Spark  |  Flink  |  dbt  |  Airflow            |
+--------------------------------------------------+
                        |
+--------------------------------------------------+
|              STORAGE LAYER (Medallion)            |
|                                                  |
|  +--------------------------------------------+  |
|  |  BRONZE (Raw)                              |  |
|  |  - Raw data from sources                   |  |
|  |  - Schema-on-read                          |  |
|  |  - Append-only                             |  |
|  +--------------------------------------------+  |
|                      |                           |
|  +--------------------------------------------+  |
|  |  SILVER (Cleansed)                         |  |
|  |  - Deduplicated, validated                 |  |
|  |  - Schema enforced                         |  |
|  |  - CDC applied                             |  |
|  +--------------------------------------------+  |
|                      |                           |
|  +--------------------------------------------+  |
|  |  GOLD (Business-Ready)                     |  |
|  |  - Star schemas, aggregations              |  |
|  |  - Conformed dimensions                    |  |
|  |  - Materialized views                      |  |
|  +--------------------------------------------+  |
+--------------------------------------------------+
                        |
+--------------------------------------------------+
|              GOVERNANCE LAYER                     |
|  Catalog  |  Quality  |  Lineage  |  Security   |
+--------------------------------------------------+
`

### Medallion Architecture (Databricks)

`python
# Bronze: Raw ingestion
df_bronze = (spark.read
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("/raw/orders/"))
df_bronze.write.format("delta").mode("append").save("/bronze/orders")

# Silver: Cleansed
df_silver = (spark.read.format("delta").load("/bronze/orders")
    .dropDuplicates(["order_id"])
    .filter(col("amount") > 0)
    .withColumn("order_date", to_date(col("timestamp"))))
df_silver.write.format("delta").mode("overwrite").save("/silver/orders")

# Gold: Business-ready
df_gold = (spark.read.format("delta").load("/silver/orders")
    .groupBy("customer_id", "order_date")
    .agg(sum("amount").alias("total_amount")))
df_gold.write.format("delta").mode("overwrite").save("/gold/customer_daily")
`

---

## 2. Data Mesh

### Core Principles

`
1. DOMAIN OWNERSHIP
   Marketing    Finance    Operations    Engineering
   owns their   owns their owns their    owns their
   data         data       data          data

2. DATA AS A PRODUCT
   Each domain publishes discoverable, trustworthy data products

3. SELF-SERVE DATA PLATFORM
   Common infrastructure for storage, compute, governance

4. FEDERATED COMPUTATIONAL GOVERNANCE
   Automated policies across domains
`

### Data Mesh Architecture

`
+--------------------------------------------------+
|              SELF-SERVE DATA PLATFORM             |
|  Storage | Compute | Governance | Discovery      |
+--------------------------------------------------+
                        |
   +--------------------+--------------------+
   |                    |                    |
+--v---------+  +------v-------+  +---------v--+
| Marketing  |  |   Finance    |  | Operations |
| Domain     |  |   Domain     |  | Domain     |
|            |  |              |  |            |
| +--------+ |  | +----------+ |  | +--------+ |
| |Data    | |  | |Data      | |  | |Data    | |
| |Product | |  | |Product   | |  | |Product | |
| |- Camp. | |  | |- GL      | |  | |- Inv.  | |
| |- Leads | |  | |- Budget  | |  | |- Ship  | |
| +--------+ |  | +----------+ |  | +--------+ |
+------------+  +--------------+  +------------+
`

### When to Choose Data Mesh

| Choose Data Mesh When | Avoid Data Mesh When |
|----------------------|---------------------|
| Large organization (1000+ employees) | Small team (< 50) |
| Multiple business domains | Single business domain |
| Domain experts available | Limited technical resources |
| Decentralized data needs | Centralized reporting needs |
| Long-term strategic investment | Quick implementation needed |

---

## 3. Data Fabric

### Concept

Data fabric uses metadata and AI/ML to automatically integrate and manage data across environments.

`
+--------------------------------------------------+
|           METADATA & KNOWLEDGE GRAPH             |
|  Auto-discovery | Classification | Lineage       |
+--------------------------------------------------+
                        |
   +--------------------+--------------------+
   |                    |                    |
+--v---------+  +------v-------+  +---------v--+
| On-Prem    |  | Cloud        |  | SaaS       |
| Databases  |  | Data Lakes   |  | APIs       |
+------------+  +--------------+  +------------+
`

### Data Fabric vs Data Mesh

| Aspect | Data Mesh | Data Fabric |
|--------|-----------|-------------|
| Approach | Decentralized, human-driven | Centralized, AI-driven |
| Integration | Domain teams build products | Automated metadata-driven |
| Governance | Federated policies | Centralized automation |
| Best for | Large org with domain expertise | Complex multi-cloud environments |

---

## 4. Data Integration Patterns

### ETL Pattern
`
Source -> Extract -> Transform -> Load -> Target
`

### ELT Pattern
`
Source -> Extract -> Load -> Transform -> Target
`

### Data Virtualization
`
Query --> Virtual Layer --> Multiple Sources
                            |
                    +-------+-------+
                    |       |       |
                   DB1     DB2     API
`

### Data Federation
`
Query --> Federation Engine --> Multiple Sources
                               |
                       Unified SQL interface
`

### Change Data Capture
`
Source DB -> CDC -> Kafka -> Multiple Targets
`

---

## 5. Real-World Scenarios

### Scenario 1: Enterprise Data Lakehouse

`
Banking Data Architecture:

Core Banking  Cards  Loans  Wealth
   |            |      |      |
   +-----+------+------|------+
         |
    Kafka (CDC)
         |
    +----v----+
    |  S3     |
    |  Raw    |
    +----+----+
         |
    Spark Processing
         |
    +----v----+
    |  S3     |
    | Curated |
    +----+----+
         |
    Redshift / Snowflake
         |
    BI Tools | ML | Regulatory Reports
`

### Scenario 2: Multi-Cloud Data Platform

`
AWS             GCP             Azure
+------+       +------+       +------+
|S3    |       |GCS   |       |ADLS  |
|Redshift|     |BigQ  |       |Synapse|
+---+--+       +---+--+       +---+--+
    |              |              |
    +-------+------+------+------+
            |
    +-------v--------+
    | Data Mesh      |
    | (Domain Teams) |
    +----------------+
`

---

## 6. Banking Examples

### Example 1: Regulatory Reporting Architecture

`
Transactional Systems       Processing          Reporting
+------------+             +----------+       +----------+
| Core Bank  |--CDC------->|          |       | Basel III|
| Cards      |--CDC------->| Kafka    |------>| CCAR     |
| Loans      |--CDC------->|          |       | Call Rpt |
+------------+             | +------+ |       | AML Rpt  |
                           | |Spark | |       +----------+
                           | +------+ |
                           +----------+
                                  |
                           +------v------+
                           | Data        |
                           | Warehouse   |
                           | (Gold)      |
                           +-------------+
`

---

## 7. E-Commerce Examples

### Example 1: Real-Time Personalization Platform

`
User Events        Processing           Personalization
+--------+       +----------+        +-------------+
|Web Clicks|--->|Kafka     |------->| Rec Engine  |
|Mobile   |--->|          |        | (ML)        |
|Search   |--->| +------+ |        +------+------+
+--------+   | |Flink | |               |
             | +------+ |        +------v------+
             +----------+        | Real-time   |
                                 | Features    |
                                 | Store       |
                                 +-------------+
                                          |
                                 +------v------+
                                 | User        |
                                 | Profile     |
                                 | (360)       |
                                 +-------------+
`

---

## 8. Interview Questions

### Q1: When would you recommend a data lakehouse over a traditional data warehouse?

**Answer:** Choose **data lakehouse** when: 1) You need both BI and ML workloads. 2) Data volume is very large (PB scale) and cost-sensitive. 3) You have diverse data types (structured + semi-structured). 4) You want to avoid data duplication between lake and warehouse. 5) You need schema evolution without downtime. Choose **traditional warehouse** when: 1) BI/SQL analytics is the primary use case. 2) Strong governance and ACID are critical. 3) Team prefers managed services. 4) Data is already structured.

### Q2: Explain data mesh principles and when to adopt it.

**Answer:** Data mesh has 4 principles: 1) **Domain ownership:** Business teams own their data as products. 2) **Data as a product:** Publish discoverable, quality-assured datasets. 3) **Self-serve platform:** Common infrastructure for all domains. 4) **Federated governance:** Automated policies across domains. Adopt when: organization is large (1000+ employees), multiple business domains exist, domain experts are available, and central data team is a bottleneck. Don't adopt for small teams or when quick implementation is needed.

### Q3: What is the difference between data virtualization and data federation?

**Answer:** **Data virtualization** creates a virtual layer that provides a unified view of data across multiple sources without moving data. Users query through the virtual layer, which translates to source queries. **Data federation** is a specific type of virtualization that combines data from multiple sources into a single virtual database with SQL interface. Federation often materializes results for performance. Both avoid data movement; federation is more focused on SQL integration, while virtualization is broader (APIs, files, streams).

### Q4: How do you design a multi-cloud data architecture?

**Answer:** Key considerations: 1) **Cloud-agnostic storage:** Use open formats (Parquet, Iceberg) that work across clouds. 2) **Metadata layer:** Use tools like DataHub/OpenMetadata for cross-cloud catalog. 3) **Compute abstraction:** Use Spark/dbt that can run on any cloud. 4) **Network connectivity:** VPN/peering between clouds for data movement. 5) **Cost optimization:** Each cloud has different pricing; route workloads strategically. 6) **Governance:** Unified policies across clouds using centralized catalog. Avoid deep vendor-specific services unless benefits outweigh lock-in costs.

### Q5: Describe the medallion architecture and its benefits.

**Answer:** Medallion architecture organizes data into three layers: **Bronze (Raw):** Ingest raw data as-is from sources. Append-only, no transformations. Preserves original format for reprocessing. **Silver (Cleansed):** Deduplicated, validated, schema-enforced data. CDC applied, conformed. **Gold (Business-Ready):** Aggregated, business logic applied. Star schemas, materialized views. Benefits: 1) Clear separation of concerns. 2) Easy debugging (trace from Gold to Bronze). 3) Supports both batch and streaming. 4) Enables data quality at each layer. 5) Simplifies governance (different policies per layer).

---

*Next Section: [12 - Orchestration](../12-Orchestration/README.md)*
