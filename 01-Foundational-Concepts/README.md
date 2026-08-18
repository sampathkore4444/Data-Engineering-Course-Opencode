# 01 - Foundational Concepts for Data Engineering

## Table of Contents
1. [Data Types](#1-data-types)
2. [Data Formats](#2-data-formats)
3. [Data Serialization](#3-data-serialization)
4. [Storage Systems](#4-storage-systems)
5. [Data Lifecycle Management](#5-data-lifecycle-management)
6. [Data Architecture Principles](#6-data-architecture-principles)

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

### 1.2 Semi-Structured Data

Semi-structured data does not conform to rigid table structures but contains tags, markers, or keys that provide organizational hierarchy. It is flexible and schema-free or schema-on-read.

**Characteristics:**
- Flexible schema that can vary between records
- Self-describing with embedded metadata
- Supports nested and hierarchical data
- Can be queried using specialized languages (XPath, JSONPath)
- Moderate storage overhead

**Examples:** JSON, XML, YAML, email (headers are structured, body is unstructured), log files with key-value pairs.

### 1.3 Unstructured Data

Unstructured data has no predefined format or organization. It is the most abundant type of data (estimated 80-90% of all data) but also the most challenging to analyze.

**Characteristics:**
- No predefined schema
- Difficult to query directly
- Requires preprocessing for analysis
- High storage requirements
- Rich in information but hard to extract

**Examples:** Text documents, images, videos, audio files, PDFs, social media posts, satellite imagery.

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

#### JSON (JavaScript Object Notation)
- **Structure:** Key-value pairs with nested objects and arrays
- **Pros:** Human-readable, supports nested data, widely supported in web APIs
- **Cons:** Verbose (high storage overhead), slow to parse, no schema enforcement
- **Use Case:** API responses, configuration files, web applications

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

#### Apache ORC (Optimized Row Columnar)
- **Structure:** Column-oriented with built-in indexes (bloom filters, min/max statistics)
- **Pros:** Built-in indexes, excellent compression, ACID transactions support (with Hive)
- **Cons:** Primarily optimized for Hive ecosystem
- **Use Case:** Hive-based analytics, large-scale data warehousing

#### Apache Avro
- **Structure:** Row-oriented binary format with embedded schema (JSON)
- **Pros:** Schema evolution, compact, fast serialization, excellent for streaming
- **Cons:** Not columnar (slower analytics), less compression than Parquet
- **Use Case:** Kafka message serialization, data lake ingestion, streaming pipelines

#### Protocol Buffers (Protobuf)
- **Structure:** Binary serialization with .proto schema definition
- **Pros:** Extremely fast, compact, strong typing, backward/forward compatible
- **Cons:** Not human-readable, requires schema compilation
- **Use Case:** gRPC services, high-performance messaging, internal APIs

### Format Comparison

| Format | Type | Compression | Schema Evolution | Nested Data | Best For |
|--------|------|-------------|-----------------|-------------|----------|
| CSV | Text | None (gzip) | No | No | Data exchange |
| JSON | Text | Low | Partial | Yes | APIs, configs |
| XML | Text | Medium | Yes (XSD) | Yes | Enterprise |
| Parquet | Columnar | Excellent | Yes | Yes | Analytics |
| ORC | Columnar | Excellent | Yes | Yes | Hive analytics |
| Avro | Row | Good | Yes | Yes | Streaming |
| Protobuf | Row | Excellent | Yes | Yes | Microservices |

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

### 4.3 Block Storage

Block storage divides data into fixed-size blocks, each with a unique address. The operating system manages the file system on top.

**Characteristics:**
- Fixed-size blocks (typically 4KB-1MB)
- Low latency, high performance
- Used for databases and transactional workloads
- Requires a file system or application to interpret

**Examples:** AWS EBS, Azure Managed Disks, Google Persistent Disk

### 4.4 Storage Comparison

| Feature | File | Object | Block |
|---------|------|--------|-------|
| Access | Mount point | REST API | Device mount |
| Performance | Moderate | Moderate | High |
| Scalability | Limited | Unlimited | Limited |
| Cost | Moderate | Low | High |
| Use Case | Shared files | Unstructured data | Databases |
| Metadata | Limited | Rich | None |

### 4.5 Data Lake vs Data Warehouse vs Data Lakehouse

```
DATA LAKE                          DATA WAREHOUSE
+---------------------------+      +---------------------------+
| Raw Data (all types)      |      | Structured Data (cleaned) |
| Schema-on-Read            |      | Schema-on-Write           |
| Low cost per GB           |      | Higher cost per GB        |
| Flexible exploration      |      | Optimized for analytics   |
| Minimal transformations   |      | Heavy transformations     |
+---------------------------+      +---------------------------+

DATA LAKEHOUSE (Modern)
+---------------------------------------------------+
| Combines benefits of both:                        |
| - Raw data storage (like data lake)               |
│ - ACID transactions (like data warehouse)         │
│ - Schema enforcement + schema evolution           │
│ - Cost-effective object storage                   │
│ - BI and ML workload support                      │
+---------------------------------------------------+
Examples: Delta Lake, Apache Iceberg, Apache Hudi
```

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

#### ACID (Traditional Databases)
- **A**tomicity: All or nothing transactions
- **C**onsistency: Data always in valid state
- **I**solation: Concurrent transactions don't interfere
- **D**urability: Committed data survives failures

#### BASE (Distributed/NoSQL Systems)
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

## 10. Interview Questions

### Q1: What is the difference between structured, semi-structured, and unstructured data? Give examples.

**Answer:** 
Structured data follows a fixed schema with predefined columns and types (e.g., relational database tables, CSV with consistent columns). Semi-structured data has some organizational properties but no rigid schema (e.g., JSON, XML with varying fields). Unstructured data has no predefined format (e.g., images, videos, free text). In practice, 80% of enterprise data is unstructured, requiring different processing approaches like NLP for text or computer vision for images.

### Q2: When would you choose Parquet over CSV for storing analytical data?

**Answer:** 
Parquet is superior for analytics because it's columnar (only reads needed columns), compresses 2-10x better than CSV, supports predicate pushdown (filters at storage level), and has built-in schema evolution. CSV is better for data exchange between different systems or when human readability is important. For a data warehouse with billions of rows queried by specific columns, Parquet reduces storage costs by 70% and query times by 10x.

### Q3: Explain CAP Theorem and its implications for distributed data systems.

**Answer:** 
CAP Theorem states a distributed system can guarantee only two of three: Consistency (all nodes see same data), Availability (every request gets a response), and Partition Tolerance (system works despite network failures). Since network partitions are unavoidable, the real choice is CP vs AP. CP systems (like HBase) sacrifice availability during partitions - useful for banking where consistency is critical. AP systems (like Cassandra) sacrifice consistency for availability - useful for social media feeds where eventual consistency is acceptable.

### Q4: What is schema-on-read vs schema-on-write? When would you use each?

**Answer:** 
Schema-on-write enforces structure before storing data (data warehouses) - ensuring quality but requiring upfront design. Schema-on-read applies structure when reading (data lakes) - allowing flexible ingestion but risking quality issues. Use schema-on-write for curated analytics datasets where consistency is critical. Use schema-on-read for raw data lakes where you need to ingest diverse data quickly and explore it later.

### Q5: Describe a data architecture you would design for a company processing 1TB of daily transaction data.

**Answer:** 
I would design a lakehouse architecture: Ingest via Kafka for real-time needs, land raw data in S3 as Parquet in a bronze layer, process through Spark for cleansing and deduplication into a silver layer, aggregate into business-ready gold layer tables. Use Delta Lake for ACID transactions and schema enforcement. Compute with Spark on EMR/Databricks. Serve via Redshift/BigQuery for BI. Use Airflow for orchestration. Monitor with Great Expectations for data quality and Monte Carlo for observability.

---

## Summary Checklist

- [ ] Understand differences between structured, semi-structured, and unstructured data
- [ ] Know major data formats and when to use each
- [ ] Understand serialization methods and their trade-offs
- [ ] Be able to design storage architecture (file vs object vs block)
- [ ] Know data lifecycle management principles
- [ ] Understand ACID vs BASE and CAP theorem
- [ ] Can design data mesh vs data fabric architectures
- [ ] Practice with real-world scenarios and examples
- [ ] Complete coding exercises with Python and SQL

---

*Next Section: [02 - Data Warehousing](../02-Data-Warehousing/README.md)*
