# 01 - Foundational Concepts for Data Engineering

## Table of Contents
1. [Data Types](#1-data-types)
2. [Data Formats](#2-data-formats)
3. [Data Serialization](#3-data-serialization)
4. [Storage Systems](#4-storage-systems)
5. [Data Lifecycle Management](#5-data-lifecycle-management)
6. [Data Architecture Principles](#6-data-architecture-principles)
7. [Data Quality Fundamentals](#7-data-quality-fundamentals)
8. [Real-World Scenarios](#8-real-world-scenarios)
9. [Banking Examples](#9-banking-examples)
10. [E-Commerce Examples](#10-e-commerce-examples)
11. [Hands-On Exercises](#11-hands-on-exercises)
12. [Interview Questions](#12-interview-questions)

---

## 1. Data Types

### 1.1 Structured Data

Structured data is highly organized information that fits neatly into a predefined schema, typically stored in relational databases with rows and columns. Every data point follows the same format, making it easily searchable and queryable.

**Characteristics:**
- Fixed schema (predefined columns and data types)
- Stored in tables with rows and columns
- Easily queryable using SQL
- Enforces data integrity through constraints
- Low storage overhead per record

**Examples:** Database tables, spreadsheets, CSV files with consistent columns.

**Tools:** PostgreSQL, MySQL, Amazon Redshift, Google BigQuery, Snowflake, Apache Hive

### 1.2 Semi-Structured Data

Semi-structured data does not conform to rigid table structures but contains tags, markers, or keys that provide organizational hierarchy. It is flexible and schema-free or schema-on-read.

**Characteristics:**
- Flexible schema that can vary between records
- Self-describing with embedded metadata
- Supports nested and hierarchical data
- Can be queried using specialized languages (XPath, JSONPath)
- Moderate storage overhead

**Examples:** JSON, XML, YAML, email (headers are structured, body is unstructured), log files with key-value pairs.

**Tools:** MongoDB, Elasticsearch, Apache Cassandra, Couchbase

### 1.3 Unstructured Data

Unstructured data has no predefined format or organization. It is the most abundant type of data (estimated 80-90% of all data) but also the most challenging to analyze.

**Characteristics:**
- No predefined schema
- Difficult to query directly
- Requires preprocessing for analysis
- High storage requirements
- Rich in information but hard to extract

**Examples:** Text documents, images, videos, audio files, PDFs, social media posts, satellite imagery.

**Tools:** Apache Tika, AWS Rekognition, Google Cloud Vision, OpenCV

### Comparison Table

| Feature | Structured | Semi-Structured | Unstructured |
|---------|-----------|-----------------|--------------|
| Schema | Fixed | Flexible | None |
| Storage | RDBMS, Data Warehouse | Document stores, Data Lakes | Object storage, Data Lakes |
| Query | SQL | JSONPath, XPath | NLP, ML required |
| Volume | ~10-20% of data | ~10-20% of data | ~60-80% of data |
| Processing | Easy | Moderate | Complex |
| Cost | High (per GB) | Moderate | Low (per GB) |

---

## 2. Data Formats

### 2.1 Text-Based Formats

#### CSV (Comma-Separated Values)
- **Structure:** Plain text with comma-separated values, one row per line
- **Pros:** Human-readable, universally supported, simple to generate
- **Cons:** No data type enforcement, no schema, no nested structures, ambiguous with commas in data
- **Use Case:** Data exchange between systems, simple data exports

**Tools:** pandas, Apache Airflow (for ETL), dbt (for transformations)

#### JSON (JavaScript Object Notation)
- **Structure:** Key-value pairs with nested objects and arrays
- **Pros:** Human-readable, supports nested data, widely supported in web APIs
- **Cons:** Verbose (high storage overhead), slow to parse, no schema enforcement
- **Use Case:** API responses, configuration files, web applications

**Tools:** Apache Kafka (Avro/JSON), Confluent Schema Registry, Apache NiFi

#### XML (eXtensible Markup Language)
- **Structure:** Tag-based hierarchical document with attributes and elements
- **Pros:** Self-describing, supports complex schemas (XSD), extensible, metadata via attributes
- **Cons:** Very verbose, slow to parse, complex to write
- **Use Case:** Enterprise systems, SOAP APIs, document storage, configuration

#### YAML (YAML Ain't Markup Language)
- **Structure:** Human-readable key-value format with indentation-based nesting
- **Pros:** Very readable, supports comments, lightweight
- **Cons:** Indentation-sensitive (error-prone), limited tooling
- **Use Case:** Configuration files, Kubernetes manifests, CI/CD pipelines

### 2.2 Binary/Columnar Formats

#### Apache Parquet
- **Structure:** Column-oriented binary format with row groups and pages
- **Pros:** Excellent compression, column pruning, predicate pushdown, schema evolution
- **Cons:** Not human-readable, write-heavy workloads slower
- **Use Case:** Analytics workloads, data lake storage, columnar queries

**Tools:** Apache Spark, Databricks, AWS Athena, Google BigQuery

#### Apache ORC (Optimized Row Columnar)
- **Structure:** Column-oriented with built-in indexes (bloom filters, min/max statistics)
- **Pros:** Built-in indexes, excellent compression, ACID transactions support (with Hive)
- **Cons:** Primarily optimized for Hive ecosystem
- **Use Case:** Hive-based analytics, large-scale data warehousing

**Tools:** Apache Hive, Apache Spark, Trino (PrestoSQL)

#### Apache Avro
- **Structure:** Row-oriented binary format with embedded schema (JSON)
- **Pros:** Schema evolution, compact, fast serialization, excellent for streaming
- **Cons:** Not columnar (slower analytics), less compression than Parquet
- **Use Case:** Kafka message serialization, data lake ingestion, streaming pipelines

**Tools:** Apache Kafka, Confluent Platform, Apache Flink

#### Protocol Buffers (Protobuf)
- **Structure:** Binary serialization with .proto schema definition
- **Pros:** Extremely fast, compact, strong typing, backward/forward compatible
- **Cons:** Not human-readable, requires schema compilation
- **Use Case:** gRPC services, high-performance messaging, internal APIs

**Tools:** gRPC, Envoy Proxy, Istio

### Format Comparison

| Format | Type | Compression | Schema Evolution | Nested Data | Read Performance | Write Performance | Query Efficiency | File Size (relative) | Best For |
|--------|------|-------------|-----------------|-------------|-----------------|-------------------|-----------------|---------------------|----------|
| CSV | Text | None (gzip) | No | No | Slow (full scan, no column pruning) | Fast (append-only, simple) | Low (no predicate pushdown, no indexing) | Large (1x baseline) | Data exchange |
| JSON | Text | Low | Partial | Yes | Moderate (must parse entire document) | Fast (human-readable, easy to write) | Low (no columnar access, verbose parsing) | Large (~0.8x) | APIs, configs |
| XML | Text | Medium | Yes (XSD) | Yes | Slow (tree traversal, verbose tags) | Slow (tag nesting, verbose output) | Low (XPath/XQuery complexity) | Very Large (~1.2x) | Enterprise |
| Parquet | Columnar | Excellent (Dictionary Encoding) | Yes | Yes | Very Fast (column pruning, predicate pushdown, page-level skips) | Moderate (column reorganization, compression overhead) | Very High (reads only needed columns, vectorized execution) | Small (~0.1-0.2x) | Analytics |
| ORC | Columnar | Excellent | Yes | Yes | Very Fast (built-in indexes, bloom filters, min/max stats) | Moderate (similar to Parquet, Hive overhead) | Very High (index-based skips, stripe-level pruning) | Small (~0.1-0.2x) | Hive analytics |
| Avro | Row | Good (Binary Encoding) | Yes | Yes | Fast (sequential row reads, no parsing overhead) | Very Fast (append-friendly, compact writes) | Moderate (row-oriented, no column pruning) | Small (~0.15-0.3x) | Streaming |
| Protobuf | Row | Excellent | Yes | Yes | Very Fast (compact binary, minimal parsing) | Very Fast (zero-copy deserialization possible) | Moderate (row-oriented, requires schema) | Smallest (~0.05-0.1x) | Microservices |

#### Key Takeaways

- **For analytics (OLAP):** Parquet and ORC dominate — columnar storage means queries only read the columns they need, dramatically reducing I/O. A query on 3 out of 50 columns reads ~6% of the data instead of 100%.
- **For streaming/ingestion:** Avro and Protobuf win — row-oriented formats are faster to write and serialize, and they embed schemas for safe evolution.
- **For human readability/debugging:** CSV and JSON — easy to inspect manually, but poor performance at scale.
- **File size matters:** Columnar formats (Parquet/ORC) compress 5-10x better than text formats due to similar values in each column being stored together. Protobuf is smallest because it uses varint encoding and omits field names.
- **Predicate pushdown** (Parquet/ORC) pushes WHERE clause filters down to the storage layer, skipping entire row groups that don't match — this is why analytical queries are 10-100x faster than full table scans on CSV.

---

## 3. Data Serialization

### 3.1 What is Serialization?

Serialization is the process of converting in-memory data objects into a format that can be stored or transmitted, and deserialization is the reverse process.

### 3.2 Serialization Methods

#### JSON Serialization
```python
import json

# Serialization (Python object -> JSON string)
data = {"name": "Alice", "amount": 1500.50, "date": "2024-01-15"}
json_string = json.dumps(data, indent=2)
print(json_string)

# Deserialization (JSON string -> Python object)
parsed = json.loads(json_string)
print(parsed["name"])  # Alice
```

#### Pickle Serialization (Python-specific)
```python
import pickle

# Serialization
data = {"transactions": [100, 200, 300], "currency": "USD"}
pickled = pickle.dumps(data)

# Deserialization
restored = pickle.loads(pickled)
```

#### Avro Serialization (Big Data)
```python
import fastavro
import io

schema = {
    "type": "record",
    "name": "Transaction",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ]
}

# Serialization
records = [{"id": "TXN001", "amount": 1500.50, "timestamp": 1705305600}]
buf = io.BytesIO()
fastavro.writer(buf, schema, records)

# Deserialization
buf.seek(0)
reader = fastavro.reader(buf)
for record in reader:
    print(record)
```

#### Parquet Serialization
```python
import pandas as pd

# Create DataFrame
df = pd.DataFrame({
    "customer_id": ["C001", "C002", "C003"],
    "amount": [1500.50, 2300.00, 890.75],
    "product": ["laptop", "phone", "tablet"]
})

# Write to Parquet
df.to_parquet("transactions.parquet", engine="pyarrow", compression="snappy")

# Read from Parquet (only reads required columns - column pruning)
df_read = pd.read_parquet("transactions.parquet", columns=["customer_id", "amount"])
```

---

## 4. Storage Systems

### 4.1 File Systems

#### Local File System
- Direct attached storage (DAS)
- Limited scalability
- Fast for single-machine workloads
- Examples: NTFS, ext4, APFS

#### Network File System (NFS/CIFS)
- Shared file access over network
- Centralized storage
- Moderate scalability
- Limited performance at scale

### 4.2 Object Storage

Object storage stores data as objects (blob + metadata + unique ID) rather than files or blocks.

**Architecture Diagram:**
```
+--------------------------------------------------+
|                  APPLICATION LAYER                |
+--------------------------------------------------+
|          REST API (PUT/GET/DELETE)                |
+--------------------------------------------------+
|    OBJECT STORAGE SERVICE                        |
|    +----------+  +----------+  +----------+      |
|    | Object 1 |  | Object 2 |  | Object 3 |      |
|    | (blob +  |  | (blob +  |  | (blob +  |      |
|    | metadata)|  | metadata)|  | metadata)|      |
|    +----------+  +----------+  +----------+      |
+--------------------------------------------------+
|         DISTRIBUTED STORAGE LAYER                |
|    +----------+  +----------+  +----------+      |
|    | Disk 1   |  | Disk 2   |  | Disk 3   |      |
|    +----------+  +----------+  +----------+      |
+--------------------------------------------------+
```

**Characteristics:**
- Flat namespace (no hierarchy)
- HTTP/REST API access
- Unlimited scalability
- Built-in redundancy (replication across zones/regions)
- Pay-per-use pricing

**Examples:** Amazon S3, Azure Blob Storage, Google Cloud Storage, MinIO

**Tools for Data Lakes:** Delta Lake, Apache Iceberg, Apache Hudi

### 4.3 Block Storage

Block storage divides data into fixed-size blocks, each with a unique address. The operating system manages the file system on top.

**Characteristics:**
- Fixed-size blocks (typically 4KB-1MB)
- Low latency, high performance
- Used for databases and transactional workloads
- Requires a file system or application to interpret

**Examples:** AWS EBS, Azure Managed Disks, Google Persistent Disk

**Tools:** Amazon RDS, Google Cloud SQL, Azure SQL Database

### 4.4 Storage Comparison

| Feature | File | Object | Block |
|---------|------|--------|-------|
| Access | Mount point | REST API | Device mount |
| Performance | Moderate | Moderate | High |
| Scalability | Limited | Unlimited | Limited |
| Cost | Moderate | Low | High |
| Use Case | Shared files | Unstructured data | Databases |
| Metadata | Limited | Rich | None |

### 4.5 Data Warehouse

A **Data Warehouse** is a centralized repository designed to store **structured, processed, and curated** data from multiple sources. It is optimized for **analytical querying and reporting**, not for day-to-day transaction processing.

**Key Characteristics:**
- Stores **cleaned, transformed, and integrated** data
- Uses **schema-on-write** — structure is enforced before data is stored
- Optimized for **read-heavy analytical workloads** (OLAP)
- Supports complex queries, aggregations, and reporting
- Provides a **single source of truth** for business intelligence
- Typically follows a **dimensional model** (Star or Snowflake schema)

**How It Works:**
```
Source Systems              ETL/ELT              Data Warehouse
+----------+            +----------+         +------------------+
| CRM      |---Extract->|          |         |  Sales Fact      |
+----------+            | Transform|-------->|  Customer Dim    |
| ERP      |---Extract->| & Load   |         |  Product Dim     |
+----------+            |          |         |  Time Dim        |
| Logs     |---Extract->|          |         +------------------+
+----------+            +----------+                |
                                              +----v----+
                                              | BI/OLAP |
                                              | Tools   |
                                              +---------+
```

**Example Use Cases:**
- Monthly sales reporting across all regions
- Customer segmentation and lifetime value analysis
- Regulatory and compliance reporting (Basel III, GDPR)
- Executive dashboards and KPI tracking

**Popular Tools:** Amazon Redshift, Google BigQuery, Snowflake, Azure Synapse, Apache Hive

---

### 4.6 Data Lake

A **Data Lake** is a vast, centralized storage repository that holds **raw data in its native format** — structured, semi-structured, and unstructured — until it is needed for analysis.

**Key Characteristics:**
- Stores **raw, unprocessed** data in original format
- Uses **schema-on-read** — structure is applied only when data is queried
- Supports **all data types**: JSON, CSV, Parquet, images, videos, logs
- **Low cost per GB** — typically built on object storage (S3, GCS, ADLS)
- Designed for **flexibility** — store first, figure out the schema later
- Ideal for **data science, machine learning, and exploratory analysis**

**How It Works:**
```
Data Sources (All Types)            Data Lake              Consumers
+-------------------+          +------------------+    +-------------+
| App Logs (JSON)   |--------->|                  |    | Data Science|
| IoT Sensors       |--------->|  RAW ZONE        |--->| ML Training |
| Database Dumps    |--------->|  (Parquet/JSON/  |    | Exploration |
| Clickstream       |--------->|   CSV/etc.)      |    +-------------+
| Social Media      |--------->|                  |    +-------------+
| Images/Videos     |--------->|                  |--->| Data Eng    |
+-------------------+          +------------------+    | (Transform) |
                                                       +-------------+
```

**Example Use Cases:**
- Storing raw clickstream data for later behavioral analysis
- Ingesting diverse IoT sensor data before defining a schema
- Storing raw logs for security auditing and threat detection
- Training machine learning models on unstructured data (images, text)

**Popular Tools:** Amazon S3, Azure Data Lake Storage, Google Cloud Storage, Delta Lake, Apache Iceberg, Apache Hudi

> **Important:** A data lake by itself is just a "data swamp" without proper governance, cataloging, and quality controls. Tools like AWS Lake Formation, Apache Atlas, and Delta Lake help bring order to raw data lakes.

---

### 4.7 Data Mart

A **Data Mart** is a **subset of a data warehouse** that focuses on a **specific business line, department, or subject area**. It contains only the data relevant to a particular team's needs.

**Key Characteristics:**
- **Focused scope** — contains data for one department or business function
- **Faster queries** — smaller dataset means quicker response times
- **Easier to manage** — simpler to build, maintain, and secure
- Typically built using a **Star Schema** for simplicity and performance
- Can be **dependent** (sourced from a central data warehouse) or **independent** (sourced directly from operational systems)

**Types of Data Marts:**

| Type | Source | Use Case |
|------|--------|----------|
| **Dependent** | Central Data Warehouse | Large enterprises with an existing DW — pull curated data for each department |
| **Independent** | Operational Source Systems | Small teams or quick-start projects — bypass the central DW |
| **Logical** | Virtual views over the DW | No physical separation — use SQL views to provide department-specific slices |

**How It Works:**
```
                     Data Warehouse (Central)
                    +-----------------------+
                    | Facts + Dimensions    |
                    +-----------+-----------+
                                |
              +-----------------+------------------+
              |                 |                  |
      +-------v------+  +------v-------+  +------v--------+
      | Sales Data   |  | Finance Data |  | Marketing     |
      | Mart         |  | Mart         |  | Data Mart     |
      |              |  |              |  |               |
      | - Sales Rep  |  | - Budget     |  | - Campaigns   |
      | - Region     |  | - P&L        |  | - Channels    |
      | - Product    |  | - Forecast   |  | - Conversions |
      +--------------+  +--------------+  +---------------+
       BI Reports        CFO Dashboard    Marketing KPIs
```

**Example Use Cases:**
- A **Sales Data Mart** with revenue, quotas, and pipeline data for the sales team
- A **Finance Data Mart** with budget, P&L, and cost center data for the finance team
- A **Marketing Data Mart** with campaign performance, conversion rates, and attribution

**Popular Tools:** Same as data warehouses (Redshift, Snowflake, BigQuery) — data marts are typically created as schemas/databases within these platforms.

---

### 4.8 Data Lakehouse

A **Data Lakehouse** is a modern architecture that **combines the flexibility of a data lake with the reliability of a data warehouse** — all in one platform.

**Key Characteristics:**
- **Open file formats** (Parquet, ORC) on cheap object storage
- **ACID transactions** — data integrity guarantees like a warehouse
- **Schema enforcement and evolution** — structured queries on raw data
- **Supports both BI and ML** workloads in one system
- **No data duplication** — same data serves analysts and data scientists
- Uses **open table formats**: Delta Lake, Apache Iceberg, Apache Hudi

**How It Works:**
```
Bronze (Raw)        Silver (Cleaned)       Gold (Business-Ready)
+-----------+       +---------------+      +-------------------+
| Raw Data  |------>| Deduplicated  |----->| Aggregated        |
| (Ingested)|       | Typed         |      | Business Metrics  |
| No schema |       | Cleaned       |      | Star Schemas      |
+-----------+       +---------------+      +-------------------+
       |                    |                       |
  Data Lake            Data Warehouse          BI / ML
  (Storage)            (Processing)          (Consumption)
```

**Example Use Cases:**
- Unified analytics platform serving both BI reports and ML pipelines
- Companies replacing separate data lake + warehouse with a single system
- Organizations wanting open formats to avoid vendor lock-in

**Popular Tools:** Databricks (Delta Lake), AWS Lake Formation, Snowflake (external tables), Google BigLake

---

### 4.9 Comparison: Data Warehouse vs Data Lake vs Data Mart vs Data Lakehouse

| Feature | Data Warehouse | Data Lake | Data Mart | Data Lakehouse |
|---------|---------------|-----------|-----------|----------------|
| **Data Type** | Structured only | All types (raw) | Structured (curated) | All types (structured via table format) |
| **Schema** | Schema-on-write | Schema-on-read | Schema-on-write | Schema enforcement + evolution |
| **Processing** | ETL (transform before load) | ELT (load raw, transform later) | ETL from DW or source | ELT with ACID transactions |
| **Users** | Business analysts, BI teams | Data scientists, ML engineers | Specific department teams | Both BI analysts and data scientists |
| **Cost** | High (compute + storage) | Low (cheap object storage) | Moderate (subset of DW cost) | Moderate (open formats on cheap storage) |
| **Query Speed** | Fast (optimized for OLAP) | Slow (raw data, no optimization) | Very fast (small, focused dataset) | Fast (table format optimizations) |
| **Scope** | Enterprise-wide | Enterprise-wide | Department or business line | Enterprise-wide |
| **Governance** | Strong | Weak (without tools) | Strong (small scope) | Strong (with Unity Catalog, etc.) |
| **ACID Support** | Yes | No (without table format) | Yes | Yes |
| **Best For** | Reporting, dashboards, compliance | ML, exploration, raw data storage | Dept-specific analytics | Unified analytics (BI + ML) |

### When to Use What?

```
Do you need raw data storage for ML/exploration?
  YES --> Data Lake
  NO  --> Do you need a focused dataset for one team?
            YES --> Data Mart
            NO  --> Do you need unified analytics for both BI and ML?
                      YES --> Data Lakehouse
                      NO  --> Data Warehouse
```

### How They Work Together

In practice, most enterprises use **a combination** of these systems:

```
Source Systems         Data Lake (Raw)        Data Warehouse        Data Marts
+----------+         +---------------+      +---------------+    +------------+
| CRM      |-------->|               |      |               |    | Sales Mart |
| ERP      |-------->|  Bronze Layer |----->|  Gold Layer   |--->| Finance    |
| Logs     |-------->|  (Raw Data)   |      |  (Curated)    |    | Marketing  |
| IoT      |-------->|               |      |               |    +------------+
+----------+         +---------------+      +---------------+
                                    |                               |
                                    +-----> ML/AI pipelines         +---> BI Tools
```

**Typical Flow:**
1. **Ingest** raw data into the **Data Lake** (bronze layer)
2. **Transform** and clean data into the **Data Warehouse** (silver/gold layers)
3. **Create Data Marts** from the warehouse for specific departments
4. **Feed ML pipelines** directly from the lake or warehouse
5. **Use a Lakehouse** to unify all of the above in a single platform (modern trend)

---

## 5. Data Lifecycle Management

### 5.1 Lifecycle Stages

```
+-------+    +--------+    +-----------+    +----------+    +---------+
| CREATE|--->| STORE  |--->| PROCESS  |--->| ARCHIVE |--->| DELETE |
+-------+    +--------+    +-----------+    +----------+    +---------+
     |            |              |               |               |
  Capture      Persist      Transform       Retire          Purge
  Ingest       Cache        Aggregate       Compress        Clean
  Generate     Backup       Enrich          Move            Sanitize
```

### 5.2 Data Retention Policies

| Data Type | Retention Period | Storage Tier | Access Pattern |
|-----------|-----------------|--------------|----------------|
| Transaction logs | 7 years | Hot (1yr) -> Warm (3yr) -> Cold (3yr) | Real-time (1yr) -> Batch (3yr) -> Archive (3yr) |
| Customer PII | Until consent revoked | Hot (encrypted) | Real-time |
| Marketing analytics | 2 years | Hot (6mo) -> Warm (1.5yr) | Real-time (6mo) -> Daily (1.5yr) |
| Audit trails | 7-10 years | Hot (1yr) -> Cold (6-9yr) | On-demand |
| Sensor/IoT data | 90 days | Hot (30d) -> Warm (60d) | Real-time |

### 5.3 Data Lineage

Data lineage tracks the journey of data from source to destination, including all transformations applied.

```
Source Systems          Transformations         Destination
+-----------+          +---------------+       +-----------+
| CRM       |----+     | Cleanse       |----->| Data      |
+-----------+    |     | Deduplicate   |      | Warehouse |
+-----------+    +---->| Enrich        |      +-----------+
| ERP       |----+     | Aggregate     |           |
+-----------+    |     +---------------+           v
+-----------+    |                           +-----------+
| Web Logs  |----+                           | BI Tools  |
+-----------+                                | Reports   |
                                             | Dashboards|
                                             +-----------+
```

**Why lineage matters:**
- Debugging data quality issues
- Impact analysis before changes
- Regulatory compliance (GDPR, SOX)
- Trust and transparency in data

**Tools:** Apache Atlas, Amundsen, DataHub, OpenLineage, Marquez

---

## 6. Data Architecture Principles

### 6.1 Single Source of Truth (SSOT)

A single authoritative source for each data element, ensuring consistency across the organization.

```
    WRONG: Multiple Sources of Truth
    +---------+     +---------+     +---------+
    | System A|     | System B|     | System C|
    | Customer|     | Customer|     | Customer|
    | Data    |     | Data    |     | Data    |
    +---------+     +---------+     +---------+
        |               |               |
        v               v               v
    +------------------------------------------+
    |        Inconsistent Reports!             |
    +------------------------------------------+

    RIGHT: Single Source of Truth
    +---------+     +---------+     +---------+
    | System A|     | System B|     | System C|
    +---------+     +---------+     +---------+
        |               |               |
        v               v               v
    +------------------------------------------+
    |         Master Data Management           |
    |       (Single Source of Truth)           |
    +------------------------------------------+
                         |
                         v
    +------------------------------------------+
    |       Consistent, Trustworthy Data       |
    +------------------------------------------+
```

### 6.2 Schema-on-Read vs Schema-on-Write

```
SCHEMA-ON-WRITE (Data Warehouse)          SCHEMA-ON-READ (Data Lake)
+-----------------------------+          +-----------------------------+
| Raw Data                    |          | Raw Data                    |
|    |                        |          |    |                        |
|    v                        |          |    v                        |
| Apply Schema (Transform)    |          | Store as-is                 |
|    |                        |          |    |                        |
|    v                        |          |    v                        |
| Load to Warehouse           |          | Apply Schema when querying  |
|    |                        |          |    |                        |
|    v                        |          |    v                        |
| Query                       |          | Query                       |
+-----------------------------+          +-----------------------------+

Pros: Data quality, query performance       Pros: Flexibility, fast ingestion
Cons: Slower ingestion, less flexible        Cons: Data quality issues, slower queries
```

### 6.3 ACID vs BASE Properties

#### ACID (Traditional Databases) - ACID prioritizes immediate data consistency over availability
- **A**tomicity: All or nothing transactions
- **C**onsistency: Data always in valid state
- **I**solation: Concurrent transactions don't interfere
- **D**urability: Committed data survives failures

#### BASE (Distributed/NoSQL Systems) - BASE prioritizes continuous system availability and performance over immediate consistency
- **B**asically **A**vailable: System is always available
- **S**oft state: State may change over time
- **E**ventual consistency: Will become consistent eventually

### 6.4 CAP Theorem

In a distributed system, you can only guarantee two of three:

```
           Consistency
              /\
             /  \
            /    \
           /  CP  \
          /________\
         /    AP    \
        /____________\
  Availability    Partition
                  Tolerance

CP: Consistency + Partition Tolerance (e.g., HBase, MongoDB)
AP: Availability + Partition Tolerance (e.g., Cassandra, DynamoDB)
CA: Consistency + Availability (e.g., PostgreSQL single node)
```

### 6.5 Data Mesh vs Data Fabric

#### Data Mesh
A decentralized, domain-oriented approach to data architecture where each business domain owns and publishes their data as products.

```
+----------------------------------------------------------+
|                    DATA MESH                              |
+----------------------------------------------------------+
|                                                          |
|  +-------------+  +-------------+  +-------------+       |
|  |  Marketing  |  |   Finance   |  | Operations  |       |
|  |  Domain     |  |   Domain    |  | Domain      |       |
|  |  +--------+ |  |  +--------+ |  |  +--------+ |       |
|  |  | Data   | |  |  | Data   | |  |  | Data   | |       |
|  |  | Product| |  |  | Product| |  |  | Product| |       |
|  |  +--------+ |  |  +--------+ |  |  +--------+ |       |
|  +------+------+  +------+------+  +------+------+       |
|         |                |                |               |
|         v                v                v               |
|  +----------------------------------------------------+  |
|  |        Self-Serve Data Platform                    |  |
|  |   (Compute, Storage, Governance, Discovery)       |  |
|  +----------------------------------------------------+  |
+----------------------------------------------------------+
```

**Key Principles:**
1. Domain ownership
2. Data as a product
3. Self-serve data platform
4. Federated computational governance

**Tools:** dbt (data transformations), Dagster (orchestration), DataHub (discovery)

#### Data Fabric
A centralized, technology-driven approach that uses metadata and AI/ML to automatically integrate and manage data across environments.

```
+----------------------------------------------------------+
|                    DATA FABRIC                            |
+----------------------------------------------------------+
|                                                          |
|  +----------------------------------------------------+  |
|  |            Metadata & Knowledge Graph              |  |
|  |   (Automated discovery, classification, lineage)   |  |
|  +----------------------------------------------------+  |
|                         |                                |
|  +------+------+------+------+------+------+            |
|  |      |      |      |      |      |      |            |
|  v      v      v      v      v      v      v            |
| +--+  +--+  +--+  +--+  +--+  +--+  +--+              |
| |DB|  |DL|  |DW|  |API|  |ML|  |ETL|  |BI|              |
| +--+  +--+  +--+  +--+  +--+  +--+  +--+              |
+----------------------------------------------------------+
```

#### Banking Example: Data Mesh in Action

**Scenario:** A large bank with Retail Banking, Corporate Banking, Wealth Management, and Risk & Compliance divisions struggles with a centralized data team that cannot keep up with demand. Each domain has unique data needs and compliance requirements.

**Data Mesh Implementation:**
```
+----------------------------------------------------------+
|                    BANKING DATA MESH                       |
+----------------------------------------------------------+
|                                                          |
|  +------------------+  +------------------+  +----------+|
|  | Retail Banking   |  | Corporate Banking|  |  Risk &  ||
|  | Domain           |  | Domain           |  |Compliance||
|  |                  |  |                  |  |  Domain  ||
|  | Data Product:    |  | Data Product:    |  |          ||
|  | - Customer 360   |  | - SME Lending    |  | Data     ||
|  | - Transaction    |  | - Trade Finance  |  | Product: ||
|  |   History        |  | - Cash Mgmt     |  | - Fraud  ||
|  | - Product        |  |                  |  |   Alerts ||
|  |   Holdings       |  | SLA: 99.9%      |  | - AML    ||
|  |                  |  | freshness       |  |   Reports||
|  | SLA: 99.9%      |  |                  |  | - Basel  ||
|  | freshness       |  | Owner: Corp     |  |   III     ||
|  |                  |  | Banking Data    |  |          ||
|  | Owner: Retail    |  | Team            |  | SLA:     ||
|  | Data Team        |  |                  |  | 99.99%   ||
|  +--------+--------+  +--------+--------+  +----+-----+|
|           |                    |                  |      |
|           v                    v                  v      |
|  +----------------------------------------------------+ |
|  |         Self-Serve Data Platform                   | |
|  |  - Data Catalog (DataHub)                         | |
|  - Compute (Snowflake)                              | |
|  |  - Governance (Unity Catalog)                     | |
|  +----------------------------------------------------+ |
+----------------------------------------------------------+
```

**How it works in practice:**
1. **Retail Banking team** owns customer transaction data. They publish a "Customer 360" data product with freshness SLA of 15 minutes. Other teams can discover and subscribe to this product via the data catalog.
2. **Corporate Banking team** owns SME lending data. They publish an "SME Credit Risk" data product. The Risk team subscribes to it for compliance reporting.
3. **Risk & Compliance team** owns fraud detection models. They publish "Real-Time Fraud Alerts" as a data product consumed by the operations team.
4. **Self-serve platform** provides shared compute (Snowflake), storage (S3), and governance tools so each team can build independently.

**Key benefit:** Instead of waiting 3 months for the central data team to build a report, each domain team builds and publishes their own data products in days.

#### Banking Example: Data Fabric in Action

**Scenario:** The same bank has data scattered across 15+ systems — core banking (mainframe), CRM (Salesforce), risk engine (custom), market data (Bloomberg), and regulatory systems. A metadata-driven approach is needed to unify access without moving all data.

**Data Fabric Implementation:**
```
+----------------------------------------------------------+
|                   BANKING DATA FABRIC                      |
+----------------------------------------------------------+
|                                                          |
|  +----------------------------------------------------+ |
|  |         Metadata & Knowledge Graph                 | |
|  |  - Auto-discovered schemas from 15+ source systems | |
|  |  - AI-classified PII (customer names, SSN, etc.)   | |
|  |  - End-to-end lineage: source -> transform -> BI   | |
|  +----------------------------------------------------+ |
|                         |                                |
|  +------+------+------+------+------+------+            |
|  v      v      v      v      v      v      v            |
|+----+ +----+ +----+ +----+ +----+ +----+ +----+        |
|Core| |CRM | |Risk| |Mkt | |Reg | |Fraud| |ATM |        |
|Bank| |    | |Eng | |Data| |Sys | |Det | |Logs |        |
|+----+ +----+ +----+ +----+ +----+ +----+ +----+        |
+----------------------------------------------------------+
```

**How it works in practice:**
1. **Auto-discovery:** The metadata layer automatically scans all 15 source systems, cataloging tables, columns, and relationships — no manual documentation needed.
2. **Smart classification:** AI/ML models automatically detect and classify sensitive data (PII, PCI, PHI) across all systems, applying regulatory tags for GDPR, SOX, and Basel III.
3. **Virtual integration:** Instead of physically moving data, a risk analyst queries across core banking + CRM + market data through a single SQL interface. The fabric optimizes the query and pulls data from each source in real-time.
4. **Automated lineage:** When a regulator asks "where does this number come from?", the fabric traces the full lineage from the final report back to the source system, including every transformation applied.

**Key benefit:** The bank avoids a 2-year data migration project. A risk analyst can run a cross-system query in minutes instead of waiting weeks for the data engineering team to build ETL pipelines.

#### When to Choose Which?

| Factor | Data Mesh | Data Fabric |
|--------|-----------|-------------|
| **Best for** | Organizations with strong domain teams | Organizations with diverse, legacy systems |
| **Approach** | Decentralized (teams own their data) | Centralized (metadata-driven automation) |
| **Implementation** | Requires organizational change | Technology-focused, less org change |
| **Governance** | Federated (each domain enforces) | Automated (AI/ML classifies and governs) |
| **Banking example** | Each division publishes data products | Metadata layer connects 15+ core systems |
| **Time to value** | Months (requires culture shift) | Weeks (auto-discovery and cataloging) |
| **Maintenance** | Ongoing per domain | Centralized, AI-assisted |

> **In practice:** Many large banks use **both** — a Data Fabric to connect legacy systems and auto-govern data, combined with Data Mesh principles for new cloud-native domains. The fabric provides the technical foundation; the mesh provides the organizational model.

### 6.6 Data as a Product (DaaP)

Treating data with the same rigor as a product:
- **Discoverable:** Easy to find via catalogs
- **Addressable:** Unique identifiers and endpoints
- **Trustworthy:** Quality SLAs and monitoring
- **Self-describing:** Rich metadata and documentation
- **Interoperable:** Standard formats and APIs
- **Secure:** Access control and compliance

---

## 7. Real-World Scenarios

### Scenario 1: Healthcare Data Platform
**Challenge:** A hospital network needs to aggregate patient data from 50+ facilities for analytics while maintaining HIPAA compliance.

**Solution Architecture:**
```
Source Systems           Ingestion           Processing          Analytics
+----------+          +----------+        +----------+        +----------+
| EHR      |---CDC--->| Kafka    |------->| Spark    |------->| Redshift |
+----------+          +----------+        +----------+        +----------+
| Lab      |---API--->|          |        |          |        |          |
+----------+          |          |        | HIPAA    |        | BI Tools |
| Imaging  |---FTP--->|          |        | Compliant|        | ML       |
+----------+          +----------+        +----------+        +----------+
```

### Scenario 2: IoT Sensor Data Pipeline
**Challenge:** A manufacturing company collects sensor data from 10,000 machines at 1-second intervals, requiring real-time anomaly detection and historical analytics.

**Key Decisions:**
- Hot path: Kafka -> Flink -> Alerting (real-time)
- Cold path: Kafka -> S3 (Parquet) -> Spark -> Redshift (batch analytics)
- Retention: 30 days hot, 2 years warm, 5 years cold

---

## 8. Banking Examples

### Example 1: Anti-Money Laundering (AML) Data Pipeline

**Challenge:** Banks must detect suspicious transactions in real-time while maintaining complete audit trails for regulators.

```
Transaction Sources        Real-Time Processing      Storage & Analytics
+--------------+         +------------------+      +------------------+
| ATM          |---CDC-->|                  |      | Transaction      |
| Mobile App   |---CDC-->| Apache Kafka     |----->| Data Lake        |
| Wire Transfer|---API-->|                  |      | (Parquet/S3)     |
| POS          |---CDC-->| +----------------+|      +--------+---------+
| Branch       |---CDC-->| | Flink Engine   ||               |
+--------------+         | | - Rule engine  ||      +--------v---------+
                         | | - ML scoring   ||      | Data Warehouse   |
                         | | - Alerting     ||      | - AML Reports    |
                         | +----------------+|      | - Risk Dashboard |
                         +------------------+      | - Audit Trail    |
                                                   +------------------+
```

**Key Metrics Monitored:**
- Transaction velocity per customer
- Unusual geographic patterns
- Large cash deposits followed by wire transfers
- Structuring patterns (just below reporting thresholds)

---

## 7. Data Quality Fundamentals

### 7.1 What is Data Quality?

Data quality measures how well data serves its intended purpose. Poor data quality costs organizations an average of $12.9 million annually (Gartner).

### 7.2 Data Quality Dimensions

| Dimension | Description | Example Issue |
|-----------|-------------|---------------|
| **Accuracy** | Data correctly represents real-world entities | Wrong customer address |
| **Completeness** | All required data is present | Missing email field |
| **Consistency** | Data is the same across systems | Different customer names in CRM vs billing |
| **Timeliness** | Data is up-to-date | Stale inventory levels |
| **Validity** | Data conforms to defined formats | Invalid email format |
| **Uniqueness** | No duplicate records | Same customer stored twice |

### 7.3 Data Quality Pipeline

```
+-----------+    +-----------+    +-----------+    +-----------+
| Profile   |--->| Validate  |--->| Cleanse   |--->| Monitor   |
| (Discover)|    | (Rules)   |    | (Fix)     |    | (Alert)   |
+-----------+    +-----------+    +-----------+    +-----------+
     |                |                |                |
  Identify        Apply          Transform        Track
  anomalies       constraints    & impute         SLAs
```

### 7.4 Data Validation Techniques

```python
import pandas as pd
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset

# Example: Data validation with Great Expectations
def validate_customer_data(df):
    """Validate customer data quality."""
    ge_df = PandasDataset(df)
    
    # Check completeness
    ge_df.expect_column_values_to_not_be_null("customer_id")
    ge_df.expect_column_values_to_not_be_null("email")
    
    # Check uniqueness
    ge_df.expect_column_values_to_be_unique("customer_id")
    
    # Check format validity
    ge_df.expect_column_values_to_match_regex(
        "email", r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    )
    
    # Check range validity
    ge_df.expect_column_values_to_be_between("age", min_value=0, max_value=150)
    
    # Check value set
    ge_df.expect_column_values_to_be_in_set("status", ["active", "inactive", "pending"])
    
    return ge_df.validate()
```

### 7.5 Common Data Quality Tools

| Tool | Type | Best For |
|------|------|----------|
| **Great Expectations** | Python library | Data validation, documentation, profiling |
| **dbt tests** | SQL-based | Data transformations quality checks |
| **Monte Carlo** | SaaS | Data observability, anomaly detection |
| **Soda** | CLI/Library | Data quality checks, monitoring |
| **Apache Griffin** | Open source | Data quality for big data |
| **Deequ (AWS)** | Spark library | Data quality at scale |

### Example 2: Core Banking Data Warehouse

**Challenge:** Consolidate data from multiple banking systems (deposits, loans, credit cards, wealth management) into a unified data warehouse for regulatory reporting and business analytics.

```
Core Banking Systems              Data Warehouse
+-----------+                    +--------------------+
| Deposits  |----ETL---->       | Fact: Daily        |
+-----------+                    | Account Balances   |
| Loans     |----ETL---->       +--------------------+
+-----------+                    | Dim: Customer      |
| Credit    |----ETL---->       | Dim: Product       |
| Cards     |                    | Dim: Branch        |
+-----------+                    | Dim: Date          |
| Wealth    |----ETL---->       | Dim: Currency      |
| Mgmt      |                    +--------------------+
+-----------+                           |
                                  +-----v------+
                                  | Regulatory |
                                  | Reports:   |
                                  | - Basel III|
                                  | - CCAR     |
                                  | - Call     |
                                  |   Reports  |
                                  +------------+
```

---

## 9. E-Commerce Examples

### Example 1: Customer 360 Data Platform

**Challenge:** Build a unified customer view from multiple touchpoints (website, mobile app, stores, customer service) to enable personalized marketing and improve customer experience.

```
Customer Touchpoints           Processing           Unified View
+------------------+         +------------+      +------------------+
| Website          |--Kafka->|            |      | Customer 360     |
| Mobile App       |--Kafka->| Spark      |----->| Profile          |
| POS (In-Store)   |--CDC-->| Streaming  |      | - Demographics   |
| Customer Service |--API-->|            |      | - Purchase Hist  |
| Email/SMS        |--ETL-->|            |      | - Browsing Hist  |
| Social Media     |--API-->|            |      | - Preferences    |
+------------------+         +------------+      | - LTV Score      |
                                                 | - Churn Risk     |
                                                 +------------------+
                                                        |
                                                 +------v--------+
                                                 | Applications:  |
                                                 | - Rec Engine   |
                                                 | - Campaigns    |
                                                 | - Churn Alert  |
                                                 +---------------+
```

### Example 2: Real-Time Inventory Management

**Challenge:** Maintain accurate inventory levels across warehouses, stores, and fulfillment centers while supporting real-time availability on the website.

```
Data Sources              Processing           Applications
+------------------+    +-------------+     +------------------+
| Warehouse SCADA  |--->|             |     | Website:         |
| POS Transactions |--->| Kafka       |---->| - Stock Display  |
| E-Commerce Orders|--->| Streams     |     | - ETA Calculator |
| Supplier Feeds   |--->|             |     +------------------+
| Returns/Adjust   |--->| +---------+ |     | Warehouse:       |
+------------------+    | | Flink   | |---->| - Pick Lists     |
                        | | - Real  | |     | - Replenishment  |
                        | |   time  | |     +------------------+
                        | |   stock | |     | Analytics:       |
                        | |   calc  | |---->| - Demand Forecast|
                        | +---------+ |     | - Stockout Alert |
                        +-------------+     +------------------+
```

---

## 11. Hands-On Exercises

### Exercise 1: Data Format Conversion (Python)
```python
import pandas as pd
import json
import csv

# Task: Convert between data formats

def csv_to_json(csv_file, json_file):
    """Convert CSV file to JSON."""
    df = pd.read_csv(csv_file)
    records = df.to_dict(orient='records')
    with open(json_file, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"Converted {len(records)} records")
    return records

def json_to_parquet(json_file, parquet_file):
    """Convert JSON to Parquet for efficient storage."""
    with open(json_file, 'r') as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
    print(f"Parquet file size: {os.path.getsize(parquet_file) / 1024:.2f} KB")
    return df

# Practice: Create sample data and test conversions
sample_data = [
    {"id": 1, "name": "Alice", "amount": 1500.50, "date": "2024-01-15"},
    {"id": 2, "name": "Bob", "amount": 2300.00, "date": "2024-01-16"},
    {"id": 3, "name": "Charlie", "amount": 890.75, "date": "2024-01-17"}
]
```

### Exercise 2: Data Quality Validation
```python
import pandas as pd
import numpy as np

# Task: Implement data quality checks

def check_data_quality(df, rules):
    """
    Check data quality based on rules.
    
    Rules format:
    {
        'column_name': {
            'not_null': True,
            'unique': True,
            'min': 0,
            'max': 100,
            'allowed_values': ['A', 'B', 'C']
        }
    }
    """
    issues = []
    
    for column, rule in rules.items():
        if column not in df.columns:
            issues.append(f"Column '{column}' not found")
            continue
        
        if rule.get('not_null'):
            null_count = df[column].isnull().sum()
            if null_count > 0:
                issues.append(f"{column}: {null_count} null values")
        
        if rule.get('unique'):
            dup_count = df[column].duplicated().sum()
            if dup_count > 0:
                issues.append(f"{column}: {dup_count} duplicates")
        
        if 'min' in rule:
            below_min = (df[column] < rule['min']).sum()
            if below_min > 0:
                issues.append(f"{column}: {below_min} values below min ({rule['min']})")
        
        if 'max' in rule:
            above_max = (df[column] > rule['max']).sum()
            if above_max > 0:
                issues.append(f"{column}: {above_max} values above max ({rule['max']})")
        
        if 'allowed_values' in rule:
            invalid = ~df[column].isin(rule['allowed_values'])
            if invalid.sum() > 0:
                issues.append(f"{column}: {invalid.sum()} invalid values")
    
    return issues

# Test with sample data
df = pd.DataFrame({
    'id': [1, 2, 2, 4],  # Duplicate
    'name': ['Alice', 'Bob', None, 'Dave'],  # Null
    'age': [25, -5, 30, 200],  # Invalid range
    'status': ['active', 'pending', 'invalid', 'active']  # Invalid value
})

rules = {
    'id': {'not_null': True, 'unique': True},
    'name': {'not_null': True},
    'age': {'min': 0, 'max': 150},
    'status': {'allowed_values': ['active', 'inactive', 'pending']}
}

print("Quality Issues Found:")
for issue in check_data_quality(df, rules):
    print(f"  - {issue}")
```

### Exercise 3: Schema Evolution (PySpark)
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Task: Handle schema evolution with Parquet

spark = SparkSession.builder \
    .appName("Schema Evolution Demo") \
    .getOrCreate()

# Initial schema
schema_v1 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("amount", DoubleType(), True)
])

# Updated schema (new column added)
schema_v2 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True)  # New column
])

# Write data with schema v1
df_v1 = spark.createDataFrame(
    [(1, "Alice", 100.0), (2, "Bob", 200.0)],
    schema_v1
)
df_v1.write.mode("overwrite").parquet("data/schema_evolution")

# Read with schema evolution enabled
df_read = spark.read.option("mergeSchema", "true").parquet("data/schema_evolution")
df_read.show()
```

### Exercise 4: Data Lineage Tracking (SQL)
```sql
-- Task: Implement simple data lineage tracking

-- Create lineage metadata table
CREATE TABLE data_lineage (
    pipeline_id VARCHAR(50),
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    transformation_type VARCHAR(50),
    transformation_logic TEXT,
    created_at TIMESTAMP,
    created_by VARCHAR(50)
);

-- Log lineage for a transformation
INSERT INTO data_lineage VALUES (
    'pipeline_001',
    'raw.transactions',
    'staging.transactions_clean',
    'cleanse',
    'Removed duplicates, standardized date format, filtered null amounts',
    CURRENT_TIMESTAMP,
    'data_engineer_1'
);

-- Query lineage to understand data flow
SELECT 
    source_table,
    target_table,
    transformation_type,
    transformation_logic
FROM data_lineage
WHERE target_table LIKE '%transactions%'
ORDER BY created_at;
```

---

## 12. Interview Questions

### Q1: What is the difference between structured, semi-structured, and unstructured data? Give examples.

**Answer:** 
Structured data follows a fixed schema with predefined columns and types (e.g., relational database tables, CSV with consistent columns). Semi-structured data has some organizational properties but no rigid schema (e.g., JSON, XML with varying fields). Unstructured data has no predefined format (e.g., images, videos, free text). In practice, 80% of enterprise data is unstructured, requiring different processing approaches like NLP for text or computer vision for images.

### Q2: When would you choose Parquet over CSV for storing analytical data?

**Answer:** 
Parquet is superior for analytics on multiple performance dimensions: 

**Read Performance** — columnar storage enables column pruning (only reads needed columns) and predicate pushdown (skips row groups that don't match filters), while CSV requires full table scans. 

**Write Performance** — CSV is faster to write (simple append), but Parquet's write overhead is justified for read-heavy workloads. 

**Query Efficiency** — Parquet queries are 10-100x faster due to vectorized execution and page-level metadata, while CSV has no indexing or pushdown. 

**File Size** — Parquet compresses 5-10x better than CSV due to columnar encoding (dictionary, run-length, delta encoding). For a data warehouse with billions of rows queried by specific columns, Parquet reduces storage costs by 70% and query times by 10x. CSV is only preferred for data exchange or when human readability matters.

### Q3: Explain CAP Theorem and its implications for distributed data systems.

**Answer:** 
CAP Theorem states a distributed system can guarantee only two of three: Consistency (all nodes see same data), Availability (every request gets a response), and Partition Tolerance (system works despite network failures). Since network partitions are unavoidable, the real choice is CP vs AP. CP systems (like HBase) sacrifice availability during partitions - useful for banking where consistency is critical. AP systems (like Cassandra) sacrifice consistency for availability - useful for social media feeds where eventual consistency is acceptable.

### Q4: What is schema-on-read vs schema-on-write? When would you use each?

**Answer:** 
Schema-on-write enforces structure before storing data (data warehouses) - ensuring quality but requiring upfront design. Schema-on-read applies structure when reading (data lakes) - allowing flexible ingestion but risking quality issues. Use schema-on-write for curated analytics datasets where consistency is critical. Use schema-on-read for raw data lakes where you need to ingest diverse data quickly and explore it later.

### Q5: Describe a data architecture you would design for a company processing 1TB of daily transaction data.

**Answer:** 
I would design a lakehouse architecture: Ingest via Kafka for real-time needs, land raw data in S3 as Parquet in a bronze layer, process through Spark for cleansing and deduplication into a silver layer, aggregate into business-ready gold layer tables. Use Delta Lake for ACID transactions and schema enforcement. Compute with Spark on EMR/Databricks. Serve via Redshift/BigQuery for BI. Use Airflow for orchestration. Monitor with Great Expectations for data quality and Monte Carlo for observability.

### Q6: What is the difference between a data lake and a data warehouse? When would you use each?

**Answer:** 

A data warehouse stores **structured, cleaned, and transformed** data optimized for analytical queries (BI, reporting). It uses **schema-on-write** — data must conform to a schema before loading. 

A data lake stores **raw data in its native format** (structured, semi-structured, unstructured) using **schema-on-read** — structure is applied at query time. 

Use a **data warehouse** when you need fast, reliable reporting with well-defined business metrics. 

Use a **data lake** when you need to store diverse raw data for ML, exploratory analysis, or when schemas aren't known upfront. Most modern architectures combine both.

### Q7: What is a data mart, and how does it differ from a data warehouse?

**Answer:** A data mart is a **subset of a data warehouse** focused on a **single department or business function** (e.g., Sales, Finance, Marketing). Key differences:
- **Scope:** Warehouse is enterprise-wide; data mart is department-specific
- **Size:** Data marts are smaller, making queries faster and management simpler
- **Purpose:** Warehouses serve all analytics needs; data marts serve a team's focused needs
- **Types:** Dependent (sourced from a central DW), Independent (sourced directly from operational systems), or Logical (virtual views over the DW)
- **Best practice:** Build a centralized enterprise DW first, then create data marts from it (Inmon approach), or build data marts with conformed dimensions and integrate later (Kimball approach).

### Q8: What is a data lakehouse, and why is it becoming popular?

**Answer:** A data lakehouse combines the **flexibility and cost-effectiveness of a data lake** with the **reliability and performance of a data warehouse** in a single platform. It uses **open table formats** (Delta Lake, Apache Iceberg, Apache Hudi) on object storage (S3, GCS) to provide ACID transactions, schema enforcement, and time travel on raw data. It's popular because it eliminates the need for separate systems — one platform serves BI analysts (structured queries) and data scientists (ML on raw data). It also avoids vendor lock-in through open formats.

### Q9: How do data warehouses, data lakes, and data marts work together in an enterprise?

**Answer:** In a typical enterprise architecture:
1. Raw data is **ingested into a data lake** (bronze layer) from all source systems
2. Data is **transformed and loaded into a data warehouse** (silver/gold layers) for clean, business-ready analytics
3. **Data marts are created** from the warehouse (or directly from sources) to serve specific departments
4. **ML pipelines** consume data directly from the lake or warehouse
5. Modern trend: use a **data lakehouse** to unify all of these in one platform, reducing complexity and duplication.

---

## Summary Checklist

### Core Concepts
- [ ] Understand differences between structured, semi-structured, and unstructured data
- [ ] Know major data formats and when to use each
- [ ] Understand format trade-offs: Read/Write Performance, Query Efficiency, and File Size
- [ ] Understand serialization methods and their trade-offs
- [ ] Be able to design storage architecture (file vs object vs block)
- [ ] Understand the differences between Data Warehouse, Data Lake, Data Mart, and Data Lakehouse
- [ ] Know when to use a data warehouse vs data lake vs data mart
- [ ] Know data lifecycle management principles
- [ ] Understand ACID vs BASE and CAP theorem
- [ ] Can design data mesh vs data fabric architectures

### Modern Tools
- [ ] Know key tools: dbt, Great Expectations, Apache Iceberg, Delta Lake
- [ ] Understand data lineage tools (Apache Atlas, DataHub, OpenLineage)
- [ ] Familiar with data quality frameworks

### Practical Skills
- [ ] Practice with real-world scenarios and examples
- [ ] Complete coding exercises with Python and SQL
- [ ] Understand data quality dimensions and validation techniques
- [ ] Can implement schema evolution with Parquet

- [ ] Understand differences between structured, semi-structured, and unstructured data
- [ ] Know major data formats and when to use each
- [ ] Understand format trade-offs: Read/Write Performance, Query Efficiency, and File Size
- [ ] Understand serialization methods and their trade-offs
- [ ] Be able to design storage architecture (file vs object vs block)
- [ ] Know data lifecycle management principles
- [ ] Understand ACID vs BASE and CAP theorem
- [ ] Can design data mesh vs data fabric architectures
- [ ] Practice with real-world scenarios and examples
- [ ] Complete coding exercises with Python and SQL
- [ ] Understand data quality dimensions and validation techniques
- [ ] Know modern data engineering tools (dbt, Great Expectations, Iceberg)

---

*Next Section: [02 - Data Warehousing](../02-Data-Warehousing/README.md)*
