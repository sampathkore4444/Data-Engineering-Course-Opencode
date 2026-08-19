# 11 - Data Architecture Patterns

## Table of Contents
1. [Modern Data Architecture](#1-modern-data-architecture)
2. [Data Mesh](#2-data-mesh)
3. [Data Fabric](#3-data-fabric)
4. [Data Integration Patterns](#4-data-integration-patterns)
5. [Real-World Scenarios](#5-real-world-scenarios)
6. [Hands-On Exercises](#6-hands-on-exercises)
7. [Interview Questions](#7-interview-questions)

---

## 1. Modern Data Architecture

### Data Lakehouse Architecture

```
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
```

### Lakehouse Platform Comparison

| Platform | Key Features | Best For |
|----------|--------------|----------|
| **Databricks** | Delta Lake, Unity Catalog, AutoML | Enterprise ML + Analytics |
| **Snowflake** | Iceberg support, Time Travel, Sharing | Multi-workload analytics |
| **AWS Lake Formation** | Glue integration, Fine-grained access | AWS-native data lakes |
| **Google Dataplex** | Data mesh support, Auto-discovery | GCP ecosystem |
| **Azure Purview** | Unified governance, Data map | Azure ecosystem |
| **Starburst** | Trino-based, Data mesh, Federation | Multi-query engine |

### Medallion Architecture (Databricks)

```python
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
```

---

## 2. Data Mesh

### Core Principles

```
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
```

### Data Mesh Architecture

```
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
```

### Data Mesh Platforms

| Platform | Type | Key Features |
|----------|------|--------------|
| **Databricks Unity Catalog** | Unified Governance | Domain-level access control |
| **DataHub** | Metadata Platform | Open-source, lineage tracking |
| **Amundsen** | Data Discovery | Lyft open-source, search |
| **OpenMetadata** | Metadata Platform | Open-source, REST API |
| **Atlan** | Modern Catalog | Active metadata, collaboration |
| **Snowflake** | Warehouse + Sharing | Native data sharing |

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

```
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
```

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
```
Source -> Extract -> Transform -> Load -> Target
```

### ELT Pattern
```
Source -> Extract -> Load -> Transform -> Target
```

### Data Virtualization Tools

| Tool | Type | Key Features |
|------|------|--------------|
| **Denodo** | Enterprise | Real-time virtualization, pushdown |
| **Dremio** | Open Source | Apache Arrow, lakehouse queries |
| **Trino** | Open Source | Distributed SQL, federated queries |
| **Presto** | Open Source | Facebook, interactive analytics |
| **Starburst** | Commercial | Trino-based, enterprise features |

### Data Virtualization
```
Query --> Virtual Layer --> Multiple Sources
                            |
                    +-------+-------+
                    |       |       |
                   DB1     DB2     API
```

### Data Federation
```
Query --> Federation Engine --> Multiple Sources
                               |
                       Unified SQL interface
```

### Change Data Capture
```
Source DB -> CDC -> Kafka -> Multiple Targets
```

---

## 5. Real-World Scenarios

### Scenario 1: Enterprise Data Lakehouse

```
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
```

### Scenario 2: Multi-Cloud Data Platform

```
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
```

---

## 6. Banking Examples

### Example 1: Regulatory Reporting Architecture

```
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
```

---

## 7. E-Commerce Examples

### Example 1: Real-Time Personalization Platform

```
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
```

---

## 6. Hands-On Exercises

### Exercise 1: Design a Data Lakehouse Architecture
```python
# Task: Design and implement a medallion architecture

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("MedallionArchitecture") \
    .master("local[*]") \
    .getOrCreate()

# Create sample raw data
raw_data = [
    (1, "C001", 100.00, "pending", "2024-01-15"),
    (2, "C002", 250.00, "completed", "2024-01-15"),
    (2, "C002", 250.00, "completed", "2024-01-15"),  # Duplicate
    (3, "C001", None, "completed", "2024-01-16"),    # Null amount
    (4, "C003", -50.00, "cancelled", "2024-01-16")   # Negative
]

df_raw = spark.createDataFrame(raw_data, 
    ["order_id", "customer_id", "amount", "status", "order_date"])

# BRONZE: Raw ingestion (append-only)
df_bronze = df_raw.withColumn("ingestion_time", current_timestamp())
df_bronze.write.mode("overwrite").parquet("/lake/bronze/orders")
print("Bronze layer written")

# SILVER: Cleansed and validated
df_silver = spark.read.parquet("/lake/bronze/orders") \
    .dropDuplicates(["order_id"]) \
    .filter(col("amount").isNotNull() & (col("amount") > 0)) \
    .withColumn("order_date", to_date("order_date")) \
    .withColumn("cleaned_at", current_timestamp())

df_silver.write.mode("overwrite").parquet("/lake/silver/orders")
print("Silver layer written")

# GOLD: Business-ready aggregation
df_gold = spark.read.parquet("/lake/silver/orders") \
    .groupBy("customer_id", "order_date") \
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )

df_gold.write.mode("overwrite").parquet("/lake/gold/customer_daily")
print("Gold layer written")

# Verify layers
print("\nBronze count:", spark.read.parquet("/lake/bronze/orders").count())
print("Silver count:", spark.read.parquet("/lake/silver/orders").count())
print("Gold count:", spark.read.parquet("/lake/gold/customer_daily").count())
```

### Exercise 2: Data Mesh Domain Design
```yaml
# Task: Define data products for different domains

# Marketing Domain - Campaign Performance
apiVersion: v1
kind: DataProduct
metadata:
  name: campaign-performance
  domain: marketing
  owner: marketing-data-team@company.com
  tags:
    - marketing
    - analytics
spec:
  description: "Campaign performance metrics and ROI analysis"
  inputs:
    - name: raw-campaigns
      source: s3://marketing-raw/campaigns/
    - name: raw-clicks
      source: s3://marketing-raw/clicks/
  outputs:
    - name: campaign-metrics
      schema:
        - name: campaign_id
          type: STRING
          description: "Unique campaign identifier"
        - name: impressions
          type: BIGINT
        - name: clicks
          type: BIGINT
        - name: conversions
          type: BIGINT
        - name: spend
          type: DECIMAL(12,2)
        - name: revenue
          type: DECIMAL(12,2)
        - name: roi
          type: DECIMAL(5,2)
  quality:
    - type: freshness
      threshold: 1h
    - type: completeness
      columns: [campaign_id, impressions, clicks]
      threshold: 0.99
  access:
    type: role-based
    roles:
      - name: analyst
        permissions: [read]
      - name: engineer
        permissions: [read, write]
---
# Finance Domain - General Ledger
apiVersion: v1
kind: DataProduct
metadata:
  name: general-ledger
  domain: finance
  owner: finance-data-team@company.com
  tags:
    - finance
    - regulatory
spec:
  description: "General ledger entries for financial reporting"
  inputs:
    - name: source-ledger
      source: oracle://finance-db/GL_ENTRIES
  outputs:
    - name: gl-summary
      schema:
        - name: account_id
          type: STRING
        - name: period
          type: STRING
        - name: debits
          type: DECIMAL(15,2)
        - name: credits
          type: DECIMAL(15,2)
        - name: balance
          type: DECIMAL(15,2)
  quality:
    - type: accuracy
      rule: "debits = credits"
      threshold: 1.0
  access:
    type: attribute-based
    attributes:
      - department: finance
      - clearance: conf+
```

### Exercise 3: Architecture Decision Record
```markdown
# Task: Create an ADR for data platform selection

## ADR-001: Data Lakehouse Platform Selection

### Status
Accepted

### Context
We need to select a data platform for our analytics workloads:
- 500GB of data growing 20% monthly
- Mix of BI reports and ML workloads
- Team of 5 data engineers
- Budget: $10K/month

### Decision
We will use **Databricks** with Delta Lake.

### Consequences
**Positive:**
- Unified platform for BI and ML
- Delta Lake provides ACID transactions
- Good community and documentation
- Auto-scaling clusters

**Negative:**
- Vendor lock-in (Databricks-specific features)
- Learning curve for team
- Cost can increase with usage

### Alternatives Considered
1. **Snowflake** - Good for BI, limited ML support
2. **AWS EMR + Redshift** - More complex, better cost control
3. **Google Dataproc + BigQuery** - Good serverless, GCP lock-in

### Review Date
2025-01-01
```

### Exercise 4: Data Integration Pattern Selection
```python
# Task: Select appropriate integration patterns

def select_integration_pattern(requirements):
    """
    Select integration pattern based on requirements.
    
    Args:
        requirements: dict with keys:
            - latency: 'real-time', 'near-real-time', 'batch'
            - data_freshness: 'current', 'hourly', 'daily'
            - transformation_complexity: 'simple', 'complex'
            - source_systems: number of sources
    """
    patterns = []
    
    # Real-time requirements
    if requirements.get('latency') == 'real-time':
        patterns.append('CDC + Kafka + Stream Processing')
        patterns.append('Event-driven architecture')
    
    # Near-real-time
    elif requirements.get('latency') == 'near-real-time':
        patterns.append('CDC + Micro-batch (Spark Streaming)')
        patterns.append('Message queue + Workers')
    
    # Batch
    else:
        patterns.append('ETL/ELT with orchestration')
        patterns.append('Scheduled batch processing')
    
    # Multiple source systems
    if requirements.get('source_systems', 1) > 3:
        patterns.append('Data virtualization for ad-hoc queries')
        patterns.append('CDC for real-time sync')
    
    # Complex transformations
    if requirements.get('transformation_complexity') == 'complex':
        patterns.append('Spark/dbt for transformations')
        patterns.append('Data quality checks at each layer')
    
    return list(set(patterns))

# Test cases
test_cases = [
    {'latency': 'real-time', 'source_systems': 5},
    {'latency': 'batch', 'transformation_complexity': 'complex'},
    {'latency': 'near-real-time', 'source_systems': 2}
]

for test in test_cases:
    print(f"\nRequirements: {test}")
    print(f"Recommended patterns: {select_integration_pattern(test)}")
```

---

## 7. Interview Questions

### Q1: When would you recommend a data lakehouse over a traditional data warehouse?

**Answer:** 
Choose **data lakehouse** when: 
1) You need both BI and ML workloads. 

2) Data volume is very large (PB scale) and cost-sensitive. 

3) You have diverse data types (structured + semi-structured). 

4) You want to avoid data duplication between lake and warehouse. 

5) You need schema evolution without downtime. 

Choose **traditional warehouse** when: 
1) BI/SQL analytics is the primary use case. 

2) Strong governance and ACID are critical. 

3) Team prefers managed services. 

4) Data is already structured.

### Q2: Explain data mesh principles and when to adopt it.

**Answer:** Data mesh has 4 principles: 

1) **Domain ownership:** Business teams own their data as products. 

2) **Data as a product:** Publish discoverable, quality-assured datasets. 

3) **Self-serve platform:** Common infrastructure for all domains. 

4) **Federated governance:** Automated policies across domains. Adopt when: organization is large (1000+ employees), multiple business domains exist, domain experts are available, and central data team is a bottleneck. Don't adopt for small teams or when quick implementation is needed.

### Q3: What is the difference between data virtualization and data federation?

**Answer:** 

**Data virtualization** creates a virtual layer that provides a unified view of data across multiple sources without moving data. Users query through the virtual layer, which translates to source queries. 

**Data federation** is a specific type of virtualization that combines data from multiple sources into a single virtual database with SQL interface. Federation often materializes results for performance. Both avoid data movement; federation is more focused on SQL integration, while virtualization is broader (APIs, files, streams).

### Q4: How do you design a multi-cloud data architecture?

**Answer:** Key considerations: 

1) **Cloud-agnostic storage:** Use open formats (Parquet, Iceberg) that work across clouds. 

2) **Metadata layer:** Use tools like DataHub/OpenMetadata for cross-cloud catalog. 

3) **Compute abstraction:** Use Spark/dbt that can run on any cloud. 

4) **Network connectivity:** VPN/peering between clouds for data movement. 

5) **Cost optimization:** Each cloud has different pricing; route workloads strategically. 

6) **Governance:** Unified policies across clouds using centralized catalog. Avoid deep vendor-specific services unless benefits outweigh lock-in costs.

### Q5: Describe the medallion architecture and its benefits.

**Answer:** Medallion architecture organizes data into three layers: 

**Bronze (Raw):** Ingest raw data as-is from sources. Append-only, no transformations. Preserves original format for reprocessing. 

**Silver (Cleansed):** Deduplicated, validated, schema-enforced data. CDC applied, conformed. 

**Gold (Business-Ready):** Aggregated, business logic applied. Star schemas, materialized views. 

Benefits: 

1) Clear separation of concerns. 

2) Easy debugging (trace from Gold to Bronze). 

3) Supports both batch and streaming. 

4) Enables data quality at each layer. 

5) Simplifies governance (different policies per layer).

### Q6: What is the difference between batch and streaming architecture?

**Answer:**
**Batch Architecture:**
- Processes data in scheduled intervals
- Higher latency (minutes to hours)
- Simpler error handling
- Good for historical analytics
- Tools: Airflow, dbt, Spark

**Streaming Architecture:**
- Processes data continuously
- Low latency (milliseconds to seconds)
- Complex state management
- Good for real-time dashboards
- Tools: Flink, Kafka Streams, Spark Streaming

**Hybrid (Lambda/Kappa):**
- Combines both approaches
- Best of both worlds
- More complex to maintain

### Q7: How do you evaluate a data architecture?

**Answer:** Key criteria:
1. **Scalability:** Can handle growth in data volume and users
2. **Performance:** Meets latency and throughput requirements
3. **Cost:** Total cost of ownership (TCO)
4. **Flexibility:** Supports diverse workloads (BI, ML, streaming)
5. **Governance:** Meets compliance and security requirements
6. **Operational Complexity:** Team can manage and maintain
7. **Vendor Lock-in:** Portability and migration options
8. **Time to Value:** How quickly can we deliver insights

---

## Summary Checklist

### Architecture Patterns
- [ ] Understand Data Lakehouse architecture (Bronze-Silver-Gold)
- [ ] Know Data Mesh principles and when to adopt
- [ ] Compare Data Fabric vs Data Mesh approaches

### Data Integration
- [ ] Choose appropriate pattern (ETL, ELT, CDC, Virtualization)
- [ ] Design for batch and streaming requirements
- [ ] Implement data virtualization for multiple sources

### Platform Selection
- [ ] Compare lakehouse platforms (Databricks, Snowflake, etc.)
- [ ] Evaluate multi-cloud strategies
- [ ] Consider cost, performance, and vendor lock-in

### Practical Skills
- [ ] Design medallion architecture implementations
- [ ] Create data product specifications
- [ ] Write Architecture Decision Records (ADRs)
- [ ] Select integration patterns based on requirements

---

*Next Section: [12 - Orchestration](../12-Orchestration/README.md)*
