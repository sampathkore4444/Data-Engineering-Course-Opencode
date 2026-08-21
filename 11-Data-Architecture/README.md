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

### What is Data Mesh?

**Data Mesh** is a decentralized data architecture paradigm that treats data as a product owned by domain teams, rather than a centralized data team. It was introduced by Zhamak Dehghani.

### Why Data Mesh?

| Problem | Data Mesh Solution |
|---------|-------------------|
| Central data team is a bottleneck | Domain teams own their data |
| Data quality issues | Data as a product with SLAs |
| Slow time-to-insight | Self-serve platform for quick access |
| Governance challenges | Federated computational governance |

### Core Principles (Detailed)

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

| Principle | Description | Example |
|-----------|-------------|--------|
| **Domain Ownership** | Business teams own their data end-to-end | Marketing owns campaign data |
| **Data as a Product** | Data must be discoverable, addressable, trustworthy | Campaign data has SLAs, documentation |
| **Self-Serve Platform** | Common infrastructure for all domains | Shared storage, compute, governance tools |
| **Federated Governance** | Automated policies across domains | Universal data quality standards |

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

### When to Choose Data Mesh

| Choose Data Mesh When | Avoid Data Mesh When |
|----------------------|---------------------|
| Large organization (1000+ employees) | Small team (< 50) |
| Multiple business domains | Single business domain |
| Domain experts available | Limited technical resources |
| Decentralized data needs | Centralized reporting needs |
| Long-term strategic investment | Quick implementation needed |
| Central data team is a bottleneck | Data team is not overloaded |

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

### What is Data Fabric?

**Data Fabric** is a data architecture approach that uses metadata and AI/ML to automatically integrate and manage data across hybrid and multi-cloud environments. It provides a unified layer that discovers, catalogs, and connects data across all environments.

### Why Data Fabric?

| Problem | Data Fabric Solution |
|---------|---------------------|
| Data scattered across clouds | Unified metadata layer |
| Manual data discovery | AI-powered auto-discovery |
| Complex data lineage | Automated lineage tracking |
| Data governance challenges | Centralized policy automation |

### How Data Fabric Works

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

### Data Fabric Key Components

| Component | Description |
|-----------|-------------|
| **Metadata Collection** | Automatically discover and catalog data across all environments |
| **Knowledge Graph** | Map relationships between data assets |
| **AI/ML Automation** | Auto-classify, tag, and recommend data |
| **Data Lineage** | Track data flow from source to consumption |
| **Self-Service Discovery** | Enable users to find and understand data |

### Data Fabric vs Data Mesh

| Aspect | Data Mesh | Data Fabric |
|--------|-----------|-------------|
| **Approach** | Decentralized, human-driven | Centralized, AI-driven |
| **Integration** | Domain teams build products | Automated metadata-driven |
| **Governance** | Federated policies | Centralized automation |
| **Best for** | Large org with domain expertise | Complex multi-cloud environments |
| **Implementation** | Requires domain team maturity | Requires metadata infrastructure |

### When to Use Data Fabric

| Scenario | Use Data Fabric? | Why |
|----------|------------------|-----|
| Multi-cloud environment | ✅ Yes | Unified view across clouds |
| Complex data landscape | ✅ Yes | AI-powered discovery and classification |
| Need automated governance | ✅ Yes | Centralized policy automation |
| Small, single-cloud | ❌ No | Overkill for simple environments |
| Domain teams are mature | ⚠️ Consider Data Mesh instead | Data Mesh may be more appropriate |

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

### Data Virtualization – Banking Use Case (Easy to Understand)

> **Think of Data Virtualization as a "universal translator" for your bank's data.**

Imagine a large bank with **5 different systems** that don't talk to each other:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    THE PROBLEM (Without Virtualization)              │
│                                                                     │
│   Customer calls the bank: "What is my total relationship?"         │
│                                                                     │
│   Bank staff must log into 5 different systems manually:            │
│                                                                     │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐  ┌───────┐ │
│   │ Core     │  │ Credit   │  │ Home     │  │ Mutual │  │ Demat │ │
│   │ Banking  │  │ Cards    │  │ Loan     │  │ Funds  │  │       │ │
│   │ (Oracle) │  │ (Mainfrm)│  │ (SQL Srv)│  │ (API)  │  │(Excel)│ │
│   └──────────┘  └──────────┘  └──────────┘  └────────┘  └───────┘ │
│                                                                     │
│   Result: 30+ minutes, manual effort, risk of missing data          │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                 THE SOLUTION (With Data Virtualization)             │
│                                                                     │
│   Bank staff runs ONE SQL query on the virtual layer:               │
│                                                                     │
│   SELECT customer_name,                                            │
│          savings_balance + credit_card Outstanding + home_loan +    │
│          mutual_fund_value + demat_value AS total_relationship      │
│   FROM virtual_customer_360                                         │
│   WHERE customer_id = 'CUST-12345';                                │
│                                                                     │
│            ┌──────────────────────────────────┐                     │
│            │   DATA VIRTUALIZATION LAYER      │                     │
│            │   (Denodo / Starburst / Trino)   │                     │
│            │                                  │                     │
│            │  • Single SQL interface           │                     │
│            │  • Real-time queries              │                     │
│            │  • No data movement               │                     │
│            │  • Data stays in source systems   │                     │
│            └───────────────┬──────────────────┘                     │
│                   ┌────────┼────────┬──────────┬──────────┐        │
│                   ▼        ▼        ▼          ▼          ▼        │
│              ┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐   │
│              │ Core   ││ Credit ││ Home   ││ Mutual ││ Demat  │   │
│              │ Banking││ Cards  ││ Loan   ││ Funds  ││        │   │
│              └────────┘└────────┘└────────┘└────────┘└────────┘   │
│                                                                     │
│   Result: 5 seconds, no data movement, always fresh data           │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Banks Love Data Virtualization

| Banking Challenge | How Virtualization Solves It | Example |
|-------------------|------------------------------|---------|
| **Customer 360° View** | Unified view across all products | Single query shows savings, cards, loans, investments |
| **Regulatory Reports (RBI/SEBI)** | Real-time data without ETL delays | Generate Basel III reports from live data |
| **Fraud Detection** | Access multiple systems instantly | Correlate card transactions with account activity |
| **M&A Integration** | Quick data access from acquired bank | Virtualize acquired bank's data without migration |
| **Legacy System Coexistence** | Work with old + new systems together | Mainframe + Cloud API in one query |
| **Data Governance** | Centralized access control | Enforce policies across all 5 systems |

### Banking Virtualization Example – Customer 360° Query

```sql
-- WITHOUT Data Virtualization: 5 separate queries, 5 different logins
-- Step 1: Login to Core Banking → Run query for account balance
-- Step 2: Login to Cards System → Run query for card outstanding
-- Step 3: Login to Loans System → Run query for loan balance
-- Step 4: Login to Mutual Funds → Call API for fund NAV
-- Step 5: Login to Demat → Download Excel, calculate total
-- Total Time: 30+ minutes

-- WITH Data Virtualization: 1 unified query, 5 seconds
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_type,
    
    -- From Core Banking (Oracle)
    cb.savings_balance,
    cb.current_balance,
    
    -- From Cards System (Mainframe)
    cc.card_number,
    cc.outstanding_amount,
    cc.available_credit,
    
    -- From Loans System (SQL Server)
    hl.loan_account,
    hl.principal_outstanding,
    hl.emi_amount,
    
    -- From Mutual Funds (REST API)
    mf.folio_number,
    mf.current_value AS mutual_fund_value,
    
    -- From Demat (Flat File)
    df.demat_account,
    df.holdings_value,
    
    -- Computed: Total Relationship Value
    (cb.savings_balance + cb.current_balance 
     + cc.outstanding_amount 
     + hl.principal_outstanding 
     + mf.current_value 
     + df.holdings_value) AS total_relationship_value
    
FROM virtual_customer_360 c
LEFT JOIN core_banking cb ON c.customer_id = cb.customer_id
LEFT JOIN cards_system cc ON c.customer_id = cc.customer_id
LEFT JOIN loans_system hl ON c.customer_id = hl.customer_id
LEFT JOIN mutual_funds mf ON c.customer_id = mf.customer_id
LEFT JOIN demat_holdings df ON c.customer_id = df.customer_id
WHERE c.customer_id = 'CUST-12345';
```

### Key Takeaways for Banking

```
┌─────────────────────────────────────────────────────────────┐
│                 DATA VIRTUALIZATION IN BANKING               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ✅ NO DATA MOVEMENT    → Data stays in source systems      │
│  ✅ REAL-TIME ACCESS     → Always fresh, no ETL delays       │
│  ✅ SINGLE SQL QUERY     → One query across 5+ systems       │
│  ✅ FASTER COMPLIANCE    → RBI/SEBI reports in minutes       │
│  ✅ COST EFFECTIVE       → No data duplication/storage       │
│  ✅ LEGACY FRIENDLY      → Works with Mainframe + Cloud      │
│  ✅ GOVERNANCE           → Centralized access control         │
│                                                             │
│  ⚠️  LIMITATIONS:                                           │
│  • Performance depends on source system speed               │
│  • Complex transformations still need ETL                   │
│  • Not ideal for historical data analysis                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Data Virtualization vs Data Warehouse (Banking Context)

> **Common Question:** "Can't we just use a Data Warehouse instead of Data Virtualization?"
> **Answer:** Yes, you CAN — but they solve different problems. Here's how they compare:

```
┌─────────────────────────────────────────────────────────────────────────┐
│              OPTION 1: DATA WAREHOUSE APPROACH                          │
│                                                                         │
│   Source Systems → ETL/ELT → Data Warehouse → Reports                  │
│                                                                         │
│   ┌──────────┐    ┌─────┐    ┌─────────────┐    ┌─────────┐           │
│   │ Core     │───►│     │    │             │───►│ Reports │           │
│   │ Banking  │    │     │    │  Data       │    │         │           │
│   ├──────────┤    │ ETL │    │  Warehouse  │    │ Dashb.  │           │
│   │ Cards    │───►│     │───►│  (Copy of   │───►│         │           │
│   ├──────────┤    │     │    │   ALL data) │    │ BI Tools│           │
│   │ Loans    │───►│     │    │             │    │         │           │
│   ├──────────┤    └─────┘    └─────────────┘    └─────────┘           │
│   │ Mutual   │───►                                                         │
│   ├──────────┤                                                            │
│   │ Demat    │───►                                                        │
│   └──────────┘                                                            │
│                                                                         │
│   ⏱️  Data freshness: Hours to Days (batch ETL)                         │
│   💾 Storage: DUPLICATE data (5TB source → 5TB warehouse)               │
│   💰 Cost: HIGH (storage + compute + ETL maintenance)                   │
│   🔧 Maintenance: HIGH (ETL pipelines break, schema changes)            │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│              OPTION 2: DATA VIRTUALIZATION APPROACH                     │
│                                                                         │
│   Source Systems → Virtual Layer → Reports (NO COPY)                    │
│                                                                         │
│   ┌──────────┐    ┌───────────┐    ┌─────────┐                         │
│   │ Core     │◄──►│           │◄──►│ Reports │                         │
│   │ Banking  │    │  Virtual  │    │         │                         │
│   ├──────────┤◄──►│  Layer    │◄──►│ Dashb.  │                         │
│   │ Cards    │    │ (NO COPY) │    │         │                         │
│   ├──────────┤◄──►│           │◄──►│ BI Tools│                         │
│   │ Loans    │    └───────────┘    └─────────┘                         │
│   ├──────────┤                                                            │
│   │ Mutual   │◄──►                                                        │
│   ├──────────┤                                                            │
│   │ Demat    │◄──►                                                        │
│   └──────────┘                                                            │
│                                                                         │
│   ⏱️  Data freshness: REAL-TIME (query goes to source)                  │
│   💾 Storage: ZERO duplication (data stays in source)                   │
│   💰 Cost: LOWER (no extra storage, no ETL jobs)                        │
│   🔧 Maintenance: LOWER (no ETL pipelines to manage)                    │
└─────────────────────────────────────────────────────────────────────────┘
```

| Factor | Data Warehouse | Data Virtualization | Winner |
|--------|---------------|---------------------|--------|
| **Data Freshness** | Hours/Days (batch) | Real-time | ✅ Virtualization |
| **Storage Cost** | High (duplicate data) | Zero (no copy) | ✅ Virtualization |
| **Query Performance** | Fast (pre-processed) | Depends on source speed | ✅ Warehouse |
| **Complex Analytics** | Excellent (pre-aggregated) | Limited (on-the-fly) | ✅ Warehouse |
| **Historical Analysis** | Excellent (time travel) | Poor (current data only) | ✅ Warehouse |
| **ML/AI Workloads** | Excellent (feature store) | Not suitable | ✅ Warehouse |
| **Setup Time** | Weeks/Months | Days | ✅ Virtualization |
| **ETL Maintenance** | High (pipelines break) | None | ✅ Virtualization |
| **Legacy System Support** | Needs ETL connectors | Native connectors | ✅ Virtualization |

### When to Use Which?

| Use Data Warehouse When | Use Data Virtualization When | Use Both Together |
|------------------------|------------------------------|-------------------|
| Need historical analysis (trends over years) | Need REAL-TIME data (fraud detection) | Virtualization for real-time ops |
| Run complex ML/AI models | Have MANY source systems (5+) | Warehouse for historical analytics |
| Need pre-aggregated dashboards | Want to avoid data duplication | Best of both worlds (Most Banks!) |
| Data doesn't change every second | Need quick setup (M&A, new regulations) | |
| Regulatory reports can be daily | Have legacy systems (Mainframe + Cloud) | |

```
┌─────────────────────────────────────────────────────────────────┐
│           HYBRID APPROACH (What Most Banks Actually Use)        │
│                                                                 │
│   ┌──────────┐                                                  │
│   │ Core     │──┐                                               │
│   │ Banking  │  │    ┌──────────────┐    ┌──────────────┐      │
│   ├──────────┤  ├───►│              │    │              │      │
│   │ Cards    │──┤    │   VIRTUAL    │───►│ Real-time    │      │
│   ├──────────┤  │    │   LAYER      │    │ Dashboards   │      │
│   │ Loans    │──┤    │              │    │ Fraud Detect │      │
│   ├──────────┤  │    └──────┬───────┘    │ Call Center  │      │
│   │ Mutual   │──┤           │            └──────────────┘      │
│   ├──────────┤  │           │                                   │
│   │ Demat    │──┘           │                                   │
│   └──────────┘              ▼                                   │
│                      ┌──────────────┐    ┌──────────────┐      │
│                      │  ETL (Batch) │───►│              │      │
│                      │  Nightly     │    │  DATA        │      │
│                      └──────────────┘    │  WAREHOUSE   │      │
│                                          │              │      │
│                                          │  Historical  │      │
│                                          │  Analytics   │      │
│                                          │  ML/AI       │      │
│                                          │  Regulatory  │      │
│                                          └──────────────┘      │
│                                                                 │
│   Virtualization → Real-time needs (instant queries)           │
│   Warehouse      → Analytics needs (historical, ML)            │
└─────────────────────────────────────────────────────────────────┘
```

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

## 8. Vietnam Banking Data Architecture (Case Study)

### Vietnam Banking Landscape Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                VIETNAM BANKING DIGITAL TRANSFORMATION                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Population: 100M+  |  Banked Population: 69%  |  Mobile Users: 72M+   │
│                                                                         │
│  State Bank of Vietnam (SBV) - Central Regulatory Authority             │
│                                                                         │
│  Key Trends:                                                            │
│  • Cashless payment target: 80% by 2025                                 │
│  • Open Banking framework under development                             │
│  • Cloud-first policy for new systems (2021+)                           │
│  • Data localization requirements                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Major Vietnamese Banks & Tech Adoption

| Bank | Type | Data Maturity | Likely Architecture | Focus Area |
|------|------|---------------|---------------------|------------|
| **Techcombank** | Private | ⭐⭐⭐⭐⭐ | Cloud (Azure/AWS) | Data-driven, advanced analytics |
| **VIB** | Private | ⭐⭐⭐⭐ | Cloud + On-prem | Digital transformation pioneer |
| **MB Bank** | Military | ⭐⭐⭐⭐ | Cloud hybrid | Mobile-first banking |
| **VPBank** | Private | ⭐⭐⭐⭐ | Cloud (AWS) | Fintech partnerships |
| **Vietcombank** | State | ⭐⭐⭐ | On-prem + Cloud | Government services |
| **BIDV** | State | ⭐⭐⭐ | On-prem (legacy) | Largest state bank |
| **VietinBank** | State | ⭐⭐⭐ | On-prem (legacy) | Core modernization |
| **ACB** | Private | ⭐⭐⭐ | Hybrid | Retail banking |
| **Sacombank** | Private | ⭐⭐⭐ | On-prem | Payment systems |
| **HDBank** | Private | ⭐⭐⭐ | Cloud emerging | SME banking |

### Typical Vietnam Banking Data Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│           CURRENT STATE: MOST VIETNAMESE BANKS                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   Legacy Systems                    Modern Systems                      │
│   ┌──────────────┐                  ┌──────────────┐                    │
│   │ Core Banking │                  │ Digital      │                    │
│   │ (T24/Flexcube│                  │ Channels     │                    │
│   │  /Oracle)    │                  │ (Mobile/App) │                    │
│   └──────┬───────┘                  └──────┬───────┘                    │
│          │                                 │                            │
│          │     ┌───────────────────────────┘                            │
│          │     │                                                        │
│          ▼     ▼                                                        │
│   ┌──────────────────┐                                                  │
│   │   ETL Layer      │                                                  │
│   │ (Informatica /   │                                                  │
│   │  Talend / SSIS)  │                                                  │
│   └────────┬─────────┘                                                  │
│            │                                                            │
│            ▼                                                            │
│   ┌──────────────────┐                                                  │
│   │ Data Warehouse   │                                                  │
│   │ (SQL Server /    │                                                  │
│   │  Oracle On-prem) │                                                  │
│   └────────┬─────────┘                                                  │
│            │                                                            │
│            ▼                                                            │
│   ┌──────────────────┐                                                  │
│   │ BI / Reports     │                                                  │
│   │ (Power BI /      │                                                  │
│   │  Crystal Reports)│                                                  │
│   └──────────────────┘                                                  │
│                                                                         │
│   ❌ No Data Virtualization                                              │
│   ❌ No Real-time CDC                                                    │
│   ❌ Siloed data domains                                                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Future State: Recommended Architecture for Vietnamese Banks

```
┌─────────────────────────────────────────────────────────────────────────┐
│           FUTURE STATE: MODERN VIETNAMESE BANK DATA ARCHITECTURE        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   Source Systems                                                        │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │
│   │ Core Banking │  │ Cards/Payments│  │ Digital      │                 │
│   │ (T24/Flexcube│  │ (Visa/Master)│  │ Channels     │                 │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                 │
│          │                 │                  │                          │
│          └────────┬────────┴──────────────────┘                          │
│                   │                                                     │
│              CDC (Debezium)                                             │
│                   │                                                     │
│                   ▼                                                     │
│          ┌──────────────────┐                                           │
│          │   Apache Kafka   │                                           │
│          │   (Event Stream) │                                           │
│          └────────┬─────────┘                                           │
│                   │                                                     │
│          ┌────────┴────────────────────────────┐                        │
│          │                                     │                        │
│          ▼                                     ▼                        │
│   ┌──────────────────┐              ┌──────────────────┐                │
│   │ Data Lake (S3/   │              │ Real-time        │                │
│   │ Azure Blob)      │              │ Processing       │                │
│   │ Bronze-Silver-Gold│              │ (Flink/Spark)    │                │
│   └────────┬─────────┘              └────────┬─────────┘                │
│            │                                 │                          │
│            │    ┌────────────────────────────┘                          │
│            │    │                                                       │
│            ▼    ▼                                                       │
│   ┌─────────────────────────────────────────────────────┐              │
│   │              DATA VIRTUALIZATION LAYER               │              │
│   │         (Denodo / Starburst / Dremio)                │              │
│   │                                                      │              │
│   │  • Customer 360° view                                │              │
│   │  • Real-time fraud detection                         │              │
│   │  • Unified regulatory reporting                      │              │
│   └──────────────────────┬──────────────────────────────┘              │
│                          │                                             │
│          ┌───────────────┴───────────────┐                            │
│          ▼                               ▼                            │
│   ┌──────────────────┐          ┌──────────────────┐                  │
│   │ Real-time        │          │ Batch Analytics  │                  │
│   │ Dashboards       │          │ (ML/AI/Reports)  │                  │
│   │ • Fraud Alert    │          │ • Basel III/SBV  │                  │
│   │ • Live Monitor   │          │ • Customer Seg   │                  │
│   └──────────────────┘          └──────────────────┘                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Vietnam Banking Use Cases for Data Virtualization

| Use Case | Challenge | Virtualization Solution | Business Value |
|----------|-----------|------------------------|----------------|
| **Customer 360°** | Data spread across 5+ core systems | Single query across all systems | Better customer service |
| **SBV Regulatory Reports** | Manual data extraction from silos | Real-time unified data access | Faster compliance reporting |
| **Fraud Detection** | Delayed data access across systems | Real-time cross-system correlation | Reduced fraud losses |
| **AML Monitoring** | Transaction data in multiple systems | Unified transaction view | Better suspicious activity detection |
| **Open Banking** | Legacy systems don't expose APIs | Virtual layer as API gateway | Enable fintech partnerships |
| **M&A Integration** | Acquired bank has different systems | Quick data access without migration | Faster integration |

### SBV (State Bank of Vietnam) Compliance Requirements

```
┌─────────────────────────────────────────────────────────────────────────┐
│           STATE BANK OF VIETNAM (SBV) DATA REQUIREMENTS                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  📋 Circular 39/2014 (AML/CFT)                                         │
│     • Customer due diligence data retention: 5 years minimum           │
│     • Transaction monitoring: Real-time or near-real-time              │
│     • Suspicious transaction reports (STR) to SBV                      │
│                                                                         │
│  📋 Circular 23/2014 (Reporting)                                        │
│     • Monthly prudential reports to SBV                                 │
│     • Quarterly financial statements                                    │
│     • Annual audited reports                                            │
│                                                                         │
│  📋 Decision 1168/QD-NHNN (2023 - Digital Transformation)              │
│     • Cloud adoption encouraged (with data localization)                │
│     • Open Banking framework development                                │
│     • Cybersecurity requirements (NIST framework)                       │
│                                                                         │
│  📋 Data Localization                                                    │
│     • Customer data must stay in Vietnam (or approved cloud regions)    │
│     • Cross-border data transfer requires SBV approval                  │
│     • Cloud providers: AWS, Azure, GCP have Vietnam regions            │
│                                                                         │
│  💡 How Data Virtualization Helps:                                       │
│     • Centralized access control for SBV audit                          │
│     • Real-time data for regulatory reporting                           │
│     • Data lineage for compliance tracking                              │
│     • No data movement = better security                                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Vietnam Banking - Implementation Roadmap

```
┌─────────────────────────────────────────────────────────────────────────┐
│           3-PHASE IMPLEMENTATION ROADMAP                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  PHASE 1: FOUNDATION (6-12 months)                                      │
│  ─────────────────────────────────────                                  │
│  ✅ Inventory all data sources (Core, Cards, Loans, Digital)            │
│  ✅ Implement CDC from core systems (Debezium)                          │
│  ✅ Set up Apache Kafka for event streaming                              │
│  ✅ Build Bronze layer in Data Lake                                      │
│  ✅ Deploy basic ETL for SBV reports                                     │
│                                                                         │
│  PHASE 2: VIRTUALIZATION (6-12 months)                                  │
│  ─────────────────────────────────────                                  │
│  ✅ Deploy Data Virtualization tool (Denodo/Starburst)                   │
│  ✅ Create virtual Customer 360° view                                    │
│  ✅ Build virtual regulatory data marts                                  │
│  ✅ Implement real-time fraud detection feeds                            │
│  ✅ Enable self-service analytics for business users                     │
│                                                                         │
│  PHASE 3: ADVANCED (12-18 months)                                       │
│  ─────────────────────────────────────                                  │
│  ✅ ML/AI platform integration (Credit scoring, Fraud models)           │
│  ✅ Open Banking API layer                                              │
│  ✅ Real-time personalization engine                                     │
│  ✅ Advanced data governance (Data Mesh optional)                        │
│  ✅ Multi-cloud strategy (AWS + Azure for resilience)                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Vietnamese Banks - Technology Stack Comparison

| Component | Current (Typical) | Recommended (Future) |
|-----------|-------------------|----------------------|
| **Core Banking** | T24 (Temenos) / Flexcube | Cloud-native core |
| **Data Warehouse** | SQL Server / Oracle On-prem | Snowflake / Databricks |
| **ETL** | Informatica / SSIS | dbt + Spark |
| **CDC** | Manual / Batch | Debezium + Kafka |
| **Data Virtualization** | ❌ Not used | Denodo / Starburst |
| **BI Tools** | Crystal Reports / Power BI | Tableau / Looker |
| **Cloud** | On-premise only | AWS + Azure (hybrid) |
| **Governance** | Manual policies | Collibra / Alation |

### Key Takeaways for Vietnam Banking

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    KEY INSIGHTS FOR VIETNAMESE BANKS                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1️⃣  DATA VIRTUALIZATION IS NOT YET WIDELY ADOPTED                       │
│      → Opportunity to be early adopter and gain competitive advantage   │
│                                                                         │
│  2️⃣  SBV COMPLIANCE DRIVES DATA ARCHITECTURE                            │
│      → AML, reporting, and data localization are key requirements        │
│                                                                         │
│  3️⃣  LEGACY MODERNIZATION IS PRIORITY #1                                 │
│      → Most banks are still on T24/Flexcube on-premise                  │
│                                                                         │
│  4️⃣  CLOUD ADOPTION IS ACCELERATING                                      │
│      → AWS, Azure, GCP all have Vietnam regions (HCMC)                 │
│                                                                         │
│  5️⃣  OPEN BANKING WILL FORCE DATA INTEGRATION                            │
│      → Fintech partnerships require unified data access                 │
│                                                                         │
│  💡 RECOMMENDATION: Start with Data Virtualization for                  │
│     real-time SBV reporting and Customer 360° - these are              │
│     quick wins that demonstrate value without major disruption.         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
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
