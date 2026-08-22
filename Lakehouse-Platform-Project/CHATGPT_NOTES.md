## Please read this after going through the project notes - Lakehouse-Platform-Project

## List out different approaches of ETL Pipelines


ETL pipelines can be classified in several ways: **by processing mode, architecture, transformation location, data movement technology, and orchestration style**.

## 1. Traditional Batch ETL

The classic approach:

```text
Source Systems
     |
     v
Extract
     |
     v
Staging Area
     |
     v
Transform
     |
     v
Data Warehouse
     |
     v
BI / Reports
```

**Example:** Core Banking → Oracle staging → transformations → Enterprise Data Warehouse.

**Best for:** Daily/monthly reporting, regulatory reporting, historical analytics.

**Tools:** Informatica, Talend, SSIS, Pentaho, Python, SQL, Airflow.

---

## 2. ELT — Extract, Load, Transform

Instead of transforming before loading:

```text
Source
  |
  | Extract
  v
Data Warehouse / Data Lake
  |
  | Transform
  v
Analytics Tables
```

For example:

```text
Oracle FLEXCUBE
      |
      v
    Raw Data
      |
      v
Snowflake / BigQuery / Databricks
      |
      v
     dbt
      |
      v
Analytics Models
```

**Best for:** Cloud data warehouses and large analytical workloads.

**Popular stack:**

```text
Fivetran / Airbyte
        ↓
Snowflake / BigQuery
        ↓
dbt
        ↓
BI
```

---

# 3. Batch ETL vs Incremental ETL

Instead of processing everything every time:

### Full Load

```text
10 Million records
       |
       v
Extract ALL
       |
       v
Transform ALL
       |
       v
Load ALL
```

### Incremental Load

```text
10 Million existing records
             +
       50,000 new/changed
             |
             v
       Process only 50K
```

Common techniques:

* `last_updated_timestamp`
* High-water mark
* Sequence/ID
* Change Data Capture
* Database logs

For banking, **incremental ETL is usually much more practical** than repeatedly processing the entire database.

---

# 4. CDC-Based ETL

CDC = **Change Data Capture**

Instead of repeatedly querying the source database:

```text
Oracle / MySQL
      |
      | Database Changes
      v
     CDC
      |
      v
Kafka / Streaming Platform
      |
      v
Processing
      |
      v
DWH / Data Lake
```

Example:

```text
CUSTOMER
   |
   | UPDATE
   v
Oracle Redo Logs
   |
   v
Debezium
   |
   v
Kafka
   |
   v
Spark / Flink
   |
   v
Data Lake / DWH
```

**Best for:**

* Near-real-time analytics
* Fraud detection
* Customer 360
* Operational dashboards
* Data replication

---

# 5. Streaming ETL

Data is processed continuously rather than once per day.

```text
Transaction
     |
     v
Kafka
     |
     v
Flink / Spark Streaming
     |
     +---------> Fraud Detection
     |
     +---------> Real-time DWH
     |
     +---------> Alerts
```

Example banking transaction:

```text
Customer pays $5,000
        ↓
Kafka
        ↓
Fraud Model
        ↓
Risk Score = 0.92
        ↓
Alert
```

**Typical technologies:**

* Kafka
* Apache Flink
* Spark Structured Streaming
* Kafka Streams
* Pulsar

---

# 6. Micro-Batch ETL

Between batch and streaming.

Instead of processing every event individually:

```text
Events
 ↓
Collect for 30 seconds
 ↓
Process batch
 ↓
Load
```

Example:

```text
10:00:00 ─┐
10:00:01  |
10:00:05  | → Micro-batch
10:00:20  |
10:00:29 ─┘
             ↓
          Process
```

Useful when you don't need millisecond-level processing.

---

# 7. Lambda Architecture

Combines batch + streaming.

```text
                    ┌── Batch Layer ──→ Historical Data
                    │
Source ─────────────┤
                    │
                    └── Speed Layer ──→ Real-time Data
                                     
                         ↓
                    Serving Layer
                         ↓
                       BI/API
```

Example:

```text
Bank Transactions
       |
       +----→ Kafka → Streaming → Real-time Fraud
       |
       +----→ Data Lake → Batch → Historical Analytics
```

**Advantage:** Supports both historical and real-time processing.

**Disadvantage:** Two processing paths increase complexity.

---

# 8. Kappa Architecture

Simplifies Lambda by using streaming as the primary processing model.

```text
Sources
   |
   v
 Kafka
   |
   v
Stream Processing
   |
   +----→ Data Lake
   |
   +----→ DWH
   |
   +----→ Real-time Applications
```

Historical data can be replayed from the event log.

**Best for:** Event-driven organizations with strong streaming requirements.

---

# 9. ETL Using Data Lake

Modern architecture:

```text
Operational Systems
        |
        v
      ETL/ELT
        |
        v
   Data Lake
        |
        +---- Raw
        |
        +---- Clean
        |
        +---- Curated
        |
        v
   Data Warehouse
        |
        v
      BI/ML
```

For example:

```text
Oracle FLEXCUBE
MySQL
Kafka
APIs
     |
     v
S3 / ADLS / GCS
     |
     v
Spark / Databricks
     |
     v
Delta Lake / Iceberg
     |
     v
DWH
```

---

# 10. Medallion Architecture

Very common with modern data platforms.

```text
             DATA LAKE
                |
        ┌───────┴───────┐
        ↓               ↓
      Bronze
        ↓
      Silver
        ↓
      Gold
        ↓
       BI/ML
```

### Bronze

Raw data.

```text
Oracle → Bronze
```

### Silver

Cleaned and standardized.

```text
Bronze
 ↓
Deduplication
Validation
Standardization
 ↓
Silver
```

### Gold

Business-ready.

```text
Silver
 ↓
Business transformations
 ↓
Gold
 ↓
Reports / ML
```

---

# 11. API-Based ETL

Data is extracted through APIs rather than databases.

```text
External API
     |
     v
API Extractor
     |
     v
JSON
     |
     v
Transform
     |
     v
Data Warehouse
```

Example:

```text
Payment Gateway API
       ↓
Python
       ↓
JSON
       ↓
Validation
       ↓
PostgreSQL
```

Common for:

* Payment providers
* CRM
* SaaS applications
* External financial data
* Government APIs

---

# 12. File-Based ETL

Still extremely common in banking.

```text
Bank
 |
 | CSV / XML / JSON
 v
SFTP
 |
 v
Landing Zone
 |
 v
ETL
 |
 v
Data Warehouse
```

Example:

```text
Visa
 ↓
Settlement File
 ↓
SFTP
 ↓
Landing
 ↓
Validation
 ↓
Transformation
 ↓
DWH
```

Formats include:

* CSV
* XML
* JSON
* Fixed-width
* Parquet
* Avro

---

# 13. Database-to-Database ETL

Very traditional enterprise approach.

```text
Oracle
   |
   v
ETL
   |
   v
PostgreSQL
```

or:

```text
MySQL
   |
   v
ETL
   |
   v
SQL Server
```

Common for migrations and integration.

---

# 14. Reverse ETL

This is the **opposite direction** of traditional ETL.

Traditional:

```text
Operational Systems
        ↓
      DWH
        ↓
       BI
```

Reverse ETL:

```text
             DWH
              |
              v
        Reverse ETL
          /   |   \
         ↓    ↓    ↓
       CRM  Marketing  Core Apps
```

Example:

```text
Customer 360
     ↓
Snowflake
     ↓
Reverse ETL
     ↓
Salesforce
```

Useful when analytics-derived information needs to go back into operational systems.

---

# 15. Data Mesh Approach

Instead of one central ETL team owning everything:

```text
                 Data Platform
                      |
       ┌──────────────┼──────────────┐
       ↓              ↓              ↓
   Payments        Loans          Cards
   Data Product    Data Product   Data Product
       ↓              ↓              ↓
   Analytics       Analytics      Analytics
```

Each domain owns its **data product**.

For a bank:

```text
Customer Domain
Loan Domain
Card Domain
Payment Domain
Fraud Domain
```

Each domain can publish trusted datasets.

---

# 16. Orchestrated ETL

ETL jobs are managed by an orchestration engine.

```text
           Airflow
              |
      ┌───────┼────────┐
      ↓       ↓        ↓
   Extract  Transform  Load
      ↓       ↓        ↓
    Oracle   Spark     DWH
```

The orchestrator handles:

* Scheduling
* Dependencies
* Retries
* Monitoring
* Failure handling
* Alerts
* Backfills

Popular tools:

* Apache Airflow
* Dagster
* Prefect
* Azure Data Factory
* AWS Glue Workflows

---

# 17. Event-Driven ETL

Instead of a fixed schedule:

```text
New Customer Created
        |
        v
      Event
        |
        v
     Kafka
        |
        v
     ETL Job
        |
        v
 Customer Data Lake
```

For example:

```text
Loan Approved
     ↓
LoanApproved Event
     ↓
Kafka
     ↓
Customer 360
     ↓
Analytics
     ↓
Notification
```

This is especially useful for modern banking systems.

---

# 18. Serverless ETL

Infrastructure is managed by the cloud provider.

Example:

```text
S3
 ↓
AWS Lambda
 ↓
AWS Glue
 ↓
S3 / Redshift
```

Or:

```text
Cloud Storage
     ↓
Cloud Function
     ↓
BigQuery
```

**Best for:** Event-driven workloads and workloads with variable volume.

---

# 19. ETL with Data Virtualization

Sometimes you don't physically move all the data.

```text
Oracle
   \
PostgreSQL ----→ Virtualization Layer → BI
   /
MySQL
```

The virtualization layer provides a unified view.

Examples include:

* Trino
* Presto
* Denodo

Useful when copying all data is expensive or unnecessary.

---

# 20. Modern Lakehouse ETL

One of the most powerful modern approaches:

```text
                Sources
                   |
        ┌──────────┼──────────┐
        ↓          ↓          ↓
      Oracle      Kafka      APIs
        |          |          |
        └──────────┼──────────┘
                   ↓
              Data Lake
                   ↓
          Delta/Iceberg/Hudi
                   ↓
              Spark/Flink
                   ↓
             Data Warehouse
                   ↓
               BI / ML
```

The idea is to combine **data lake flexibility** with **data warehouse capabilities**.

---

# Summary

| Approach            | Processing           | Typical Use            |
| ------------------- | -------------------- | ---------------------- |
| Traditional ETL     | Batch                | Enterprise DWH         |
| ELT                 | Batch                | Cloud DWH              |
| Incremental ETL     | Batch                | Large databases        |
| CDC                 | Near real-time       | Database replication   |
| Streaming ETL       | Real-time            | Fraud/transactions     |
| Micro-batch         | Near real-time       | Frequent analytics     |
| Lambda              | Batch + streaming    | Hybrid systems         |
| Kappa               | Streaming            | Event-driven systems   |
| Data Lake ETL       | Batch/stream         | Big data               |
| Medallion           | Batch/stream         | Lakehouse              |
| API ETL             | Batch/real-time      | SaaS/API integration   |
| File ETL            | Batch                | Banking settlement     |
| DB-to-DB            | Batch                | Migration/integration  |
| Reverse ETL         | Batch/near real-time | DWH → operational apps |
| Data Mesh           | Domain-oriented      | Large enterprises      |
| Orchestrated ETL    | Any                  | Production pipelines   |
| Event-driven ETL    | Event-based          | Modern applications    |
| Serverless ETL      | Event/batch          | Cloud-native           |
| Data Virtualization | Query-time           | Unified access         |
| Lakehouse           | Batch + streaming    | Modern analytics/ML    |

### For a banking environment

I would typically recommend **combining several approaches rather than choosing only one**:

```text
                 CORE BANKING
               Oracle FLEXCUBE
                      |
             ┌────────┴────────┐
             ↓                 ↓
          CDC/Batch           APIs
             ↓                 ↓
           Kafka          API Gateway
             ↓                 ↓
        Streaming/ETL         ETL
             └────────┬────────┘
                      ↓
                  Data Lake
                 Bronze Layer
                      ↓
                 Silver Layer
                      ↓
                  Gold Layer
                      ↓
              Data Warehouse
                /          \
               ↓            ↓
             BI            ML
```

For the kind of **banking + data engineering architecture** you've been discussing, the most important approaches to master are **Batch ETL, Incremental ETL, CDC, Streaming ETL, ELT, Medallion/Lakehouse, and Orchestrated ETL**.



## Above summary Data Lake and Data warehouse one server?

No. **Data Lake and Data Warehouse do not have to be on the same server.** In a modern banking architecture, they are usually **separate storage/compute platforms**, although they can sometimes be physically or logically combined.

### Typical architecture

```text
             Core Banking
          Oracle FLEXCUBE
                  |
                  v
          CDC / Batch ETL
                  |
                  v
        ┌───────────────────┐
        │     DATA LAKE     │
        │                   │
        │ Bronze            │
        │ Silver            │
        │ Gold              │
        └─────────┬─────────┘
                  |
                  | ETL / ELT
                  v
        ┌───────────────────┐
        │ DATA WAREHOUSE    │
        │                   │
        │ Fact tables       │
        │ Dimension tables  │
        │ Aggregations      │
        └─────────┬─────────┘
                  |
             ┌────┴────┐
             ↓         ↓
            BI        ML
```

### What is the difference?

|                 | Data Lake                              | Data Warehouse                            |
| --------------- | -------------------------------------- | ----------------------------------------- |
| Primary purpose | Store large amounts of raw/varied data | Analytics/reporting                       |
| Data            | Raw + semi-structured + structured     | Mostly structured                         |
| Schema          | Schema-on-read                         | Schema-on-write                           |
| Cost            | Usually cheaper                        | Usually more expensive                    |
| Users           | Data engineers/data scientists         | BI analysts/business                      |
| Examples        | S3, ADLS, GCS, MinIO                   | Snowflake, BigQuery, Redshift, PostgreSQL |
| Typical data    | JSON, CSV, Parquet, logs, CDC events   | Facts/dimensions, aggregates              |

### But there is an important modern exception

With a **Lakehouse**, you can reduce the need for a separate traditional warehouse:

```text
             Sources
                |
                v
          ┌─────────────┐
          │ Data Lake   │
          │             │
          │ Bronze      │
          │ Silver      │
          │ Gold        │
          └──────┬──────┘
                 |
        SQL / BI / ML
```

Technologies such as **Delta Lake, Apache Iceberg and Apache Hudi** allow the lake to provide many warehouse-like capabilities.

So:

**Traditional architecture:**

```text
Data Lake → Data Warehouse → BI
```

**Modern Lakehouse architecture:**

```text
Data Lakehouse → BI / ML / Analytics
```

And importantly, **"server" is not quite the right mental model** for cloud systems. For example, an S3-based data lake and Snowflake warehouse may have completely separate storage and compute infrastructure, and you don't manage either as a single physical server.

For your **banking architecture**, I'd normally recommend starting with **Data Lake + Data Warehouse as separate logical layers**, unless your scale and requirements strongly favor a Lakehouse.


## In the above typical architecture, what does bronze silver gold contain and what does data warehouse contain. Show me with one banking example

Absolutely. Let's use **one concrete banking example: a customer making a debit-card transaction** and follow that data from the core banking system all the way to the Data Warehouse.

![Image](https://images.openai.com/static-rsc-4/06YMdYuNJ1DLKWOdETz47sP0_dn_ItNPfjwIJb-Appy3amQUHZg4p80spEOq8NB_Jp_0MYghL8s5rJuWwzlZ2662Mtd8jiAebOVJAe9cDhdNA1SRGD72LrjIwR9eupCDdGRJmURzGV3YljIxSaiiDl_1UaErFH095XAkTnOmXpbWBcjI7YXhoPOcUOvEbo9h?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/pVxDuf3GsVKU4KHQ_ciQgllr_y-iorQJJ6BC63PcVGLjd5U2nlhg1msj0omLaZzj7T1TYAIcqXyrojNORQEOut0SDWuW7AXmJ_-WXHmW3A1ooq7WRf7PNY01XpKwTresQKELw6adSAjQYXRo08QfnaYfEi23sYG075Fm_llUrz7eiA1Dw7tPOv8pLCKKitYS?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/if_fvnu65ax5HMnD3EB1MqDbLN8YyF-I0z_ELdiWunDppyt6WKtVr8GSqTHkSaA5VWdvTAYFbwwrvkBbkWTxYV-XK85UG8hruGxJdt_sIxLPdwClXgLdfYRzgMTKewKiuwv3JDyMI1TTAfq_XaAvt4Fqd9NL8oAiMXT_vlKljN0SF-y8va1YZZSdf4QHmQLW?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/4vq2_9fic8DMcO9wDN045wgGhTRPLCv2s4QoLf3tziClmtR4grdZlvJvpanVgCGhXmMvH7smStK4bc8Jsy4F65R6fcsJp9YZ2aOerDYMIc2oTcQhRhyRutEyLsHWDqGHJa_MNZG0qquuNwczIwreP2tyjfXgy6yciH7Yx0nJTAknnoAg7uX2AuLN-prw5sMh?purpose=fullsize)

## 1. Overall flow

```text
              CORE BANKING / CARD SYSTEM
                         |
                         | CDC / ETL
                         v
                ┌─────────────────┐
                │  BRONZE LAYER   │
                │   Raw Data      │
                └────────┬────────┘
                         |
                         | Clean / Validate
                         v
                ┌─────────────────┐
                │  SILVER LAYER   │
                │ Cleaned Data    │
                └────────┬────────┘
                         |
                         | Business Rules
                         v
                ┌─────────────────┐
                │   GOLD LAYER    │
                │ Business Data   │
                └────────┬────────┘
                         |
                         | ETL / ELT
                         v
                ┌─────────────────┐
                │ DATA WAREHOUSE  │
                │ Star Schema     │
                └────────┬────────┘
                         |
                  ┌──────┴──────┐
                  ↓             ↓
                 BI            ML
```

Let's follow **one transaction**.

---

# 2. Source system

Suppose a customer uses their debit card.

```text
Customer: C10001
Account:  AC50001
Card:     4111XXXXXXXX1234

Transaction:
Amount:       $250
Merchant:     AEON Mall
Location:     Phnom Penh
Date:         2026-08-22 10:35:21
```

The core banking/card system might have a table:

```text
CARD_TRANSACTION

txn_id
account_id
card_id
merchant_id
amount
currency
txn_date
txn_type
status
channel
```

Example:

| txn_id  | account_id | amount | currency | merchant | status  |
| ------- | ---------- | -----: | -------- | -------- | ------- |
| TX10001 | AC50001    |    250 | USD      | AEON001  | SUCCESS |

---

# 3. Bronze Layer — RAW DATA

The **Bronze layer is basically the landing zone for source data**.

You don't try to make the data perfect here.

```text
Oracle / MySQL
      |
      v
   Bronze
```

You might store:

```text
bronze/card_transaction/
    2026/
      08/
        22/
          transactions_1035.json
```

Example:

```json
{
  "txn_id": "TX10001",
  "account_id": "AC50001",
  "amount": "250.00",
  "currency": "USD",
  "merchant_id": "AEON001",
  "txn_date": "2026-08-22 10:35:21",
  "status": "SUCCESS",
  "source": "CARD_SYSTEM"
}
```

### Bronze characteristics

**Don't modify much.**

It should preserve:

* Original data
* Original format
* Source system information
* Ingestion timestamp
* CDC metadata
* Source transaction ID

You might add technical fields:

```text
source_system
ingestion_timestamp
batch_id
operation_type
record_hash
```

For example:

```text
TX10001
AC50001
250.00
USD
AEON001
SUCCESS
CARD_SYSTEM
2026-08-22 10:36:02
INSERT
```

### Why keep Bronze?

Because if your transformation is wrong:

```text
Bronze
   |
   X  Transformation bug
   |
Silver ❌
```

You can go back to:

```text
Bronze
   |
   v
Correct transformation
   |
   v
Silver
```

So **Bronze = source-of-truth landing layer**.

---

# 4. Silver Layer — CLEANED & STANDARDIZED

Now we clean the Bronze data.

```text
Bronze
   |
   | Data Quality
   | Standardization
   | Deduplication
   | Validation
   v
Silver
```

Suppose Bronze has:

```text
amount = "250.00"
currency = "USD"
status = "SUCCESS"
```

Silver converts it into proper types:

```text
txn_id       STRING
account_id   STRING
amount       DECIMAL(18,2)
currency     STRING
txn_date     TIMESTAMP
status       STRING
```

Example:

| txn_id  | account_id | amount | currency | txn_date            | status  |
| ------- | ---------- | -----: | -------- | ------------------- | ------- |
| TX10001 | AC50001    | 250.00 | USD      | 2026-08-22 10:35:21 | SUCCESS |

But Silver may also **join reference/master data**.

For example:

```text
CARD_TRANSACTION
       +
CUSTOMER
       +
ACCOUNT
       +
MERCHANT
```

Now Silver might contain:

| txn_id  | customer_id | account_id | merchant  | city       | amount | currency |
| ------- | ----------- | ---------- | --------- | ---------- | -----: | -------- |
| TX10001 | C10001      | AC50001    | AEON Mall | Phnom Penh |    250 | USD      |

### Silver does things like:

* Remove duplicates
* Validate data
* Standardize dates
* Standardize currencies
* Convert data types
* Handle nulls
* Validate account IDs
* Join reference data
* Mask sensitive data
* Apply basic data quality rules

So:

> **Silver = clean, trusted, standardized data.**

---

# 5. Gold Layer — BUSINESS-READY DATA

Now we apply **business logic**.

```text
Silver
   |
   | Business rules
   | Aggregations
   | Calculations
   | KPIs
   v
Gold
```

For example, management doesn't necessarily want individual transactions.

They may want:

> "How much did each customer spend this month?"

From millions of Silver transactions:

```text
Silver Transactions
        |
        v
GROUP BY customer_id
        |
        v
Gold Customer Spending
```

Example:

| customer_id | month   | txn_count | total_spend | avg_txn |
| ----------- | ------- | --------: | ----------: | ------: |
| C10001      | 2026-08 |        42 |      $8,750 | $208.33 |
| C10002      | 2026-08 |        18 |      $2,350 | $130.56 |

Another Gold dataset could be:

### Customer 360

| customer_id | deposits | loans | card_spend | txn_count | risk_segment |
| ----------- | -------: | ----: | ---------: | --------: | ------------ |
| C10001      |     $25K |  $10K |      $8.7K |        42 | HIGH_VALUE   |
| C10002      |      $5K |    $0 |      $2.3K |        18 | MASS         |

Another:

### Daily Branch Performance

| branch | date       | deposits | withdrawals | transactions |
| ------ | ---------- | -------: | ----------: | -----------: |
| PP001  | 2026-08-22 |    $1.2M |       $450K |       12,500 |

So:

> **Gold = business-ready information.**

---

# 6. Then why do we need a Data Warehouse?

This is the important part.

You might ask:

> "If Gold already contains business-ready data, why do we need a Data Warehouse?"

Because **Gold and Data Warehouse serve somewhat different purposes**.

The Gold layer is typically optimized for **data engineering, data science, broad analytical use, and flexible storage**.

The Data Warehouse is optimized for:

* SQL analytics
* BI
* Reporting
* Regulatory reporting
* Consistent business definitions
* Dimensional modeling
* High-performance dashboards

---

# 7. Gold → Data Warehouse

Suppose Gold has:

```text
customer_spending
customer_360
daily_branch_performance
```

We transform/model them into a **Data Warehouse star schema**.

For example:

```text
                  DIM_CUSTOMER
                       |
                       |
DIM_DATE ---- FACT_TRANSACTION ---- DIM_MERCHANT
                       |
                       |
                  DIM_ACCOUNT
```

This is very different from simply storing raw transactions.

---

# 8. Data Warehouse FACT table

We create:

```text
FACT_TRANSACTION
```

Example:

| transaction_key | date_key | customer_key | account_key | merchant_key | amount |
| --------------: | -------: | -----------: | ----------: | -----------: | -----: |
|          100001 | 20260822 |          501 |        1001 |         9001 |    250 |
|          100002 | 20260822 |          501 |        1001 |         9002 |     75 |
|          100003 | 20260822 |          502 |        1002 |         9001 |    120 |

Notice something important.

The Data Warehouse usually doesn't repeatedly store:

```text
customer_name
customer_address
merchant_address
branch_name
```

Instead it uses **keys**.

---

# 9. DIM_CUSTOMER

```text
DIM_CUSTOMER
```

| customer_key | customer_id | customer_name | segment | city       |
| -----------: | ----------- | ------------- | ------- | ---------- |
|          501 | C10001      | John          | Premium | Phnom Penh |
|          502 | C10002      | David         | Mass    | Siem Reap  |

---

# 10. DIM_MERCHANT

```text
DIM_MERCHANT
```

| merchant_key | merchant_id | merchant_name  | category |
| -----------: | ----------- | -------------- | -------- |
|         9001 | AEON001     | AEON Mall      | Retail   |
|         9002 | REST001     | Restaurant ABC | Food     |

---

# 11. DIM_DATE

```text
DIM_DATE
```

| date_key | date       | month | quarter | year |
| -------: | ---------- | ----: | ------: | ---: |
| 20260822 | 2026-08-22 |     8 |      Q3 | 2026 |

Now the BI tool can easily ask:

> "How much did Premium customers spend in Retail during August?"

The query becomes conceptually:

```text
FACT_TRANSACTION
       |
       +--- DIM_CUSTOMER
       |
       +--- DIM_MERCHANT
       |
       +--- DIM_DATE
```

---

# 12. Complete journey of our $250 transaction

The same transaction travels through the architecture:

```text
SOURCE
──────────────────────────────
Oracle / Card System

TX10001
AC50001
$250
AEON001
SUCCESS
       |
       v
BRONZE
──────────────────────────────
Raw source data

TX10001
AC50001
"250.00"
"USD"
"AEON001"
"SUCCESS"
       |
       v
SILVER
──────────────────────────────
Cleaned + standardized

TX10001
C10001
AC50001
AEON Mall
Phnom Penh
250.00 USD
SUCCESS
       |
       v
GOLD
──────────────────────────────
Business-ready

Customer C10001
August spending = $8,750
Transactions = 42
Segment = HIGH_VALUE
       |
       v
DATA WAREHOUSE
──────────────────────────────
Star Schema

FACT_TRANSACTION
       |
       +--- DIM_CUSTOMER
       +--- DIM_ACCOUNT
       +--- DIM_MERCHANT
       +--- DIM_DATE
       |
       v
BI / Reporting
```

---

# 13. The easiest way to remember it

Think of a bank's data as going through **four levels of maturity**:

```text
BRONZE
"What did the source system give us?"
        ↓
SILVER
"Can we trust and understand this data?"
        ↓
GOLD
"What does this data mean to the business?"
        ↓
DATA WAREHOUSE
"How do we organize it efficiently for
enterprise analytics and reporting?"
```

### In one sentence:

**Bronze = Raw → Silver = Clean → Gold = Business-ready → Data Warehouse = Analytics-optimized model.**

One important nuance: **Gold does not always have to feed a separate Data Warehouse**. In a Lakehouse architecture, Gold tables can themselves be the primary analytics layer. But in the traditional architecture we were discussing, **Gold → Data Warehouse** is a very common pattern.


## What is an incremental ETL? One banking example pls

**Incremental ETL** means: **instead of extracting and processing all historical records every time, you process only the records that are new or changed since the previous ETL run.**

For banking, this is extremely important because a core banking table can contain **millions or hundreds of millions of transactions**.

## 1. Full ETL vs Incremental ETL

Suppose your bank has:

```text
CAVW_ENTRIES
----------------------
500 million transactions
```

### Full ETL

Every night:

```text
500 million records
       ↓
Extract ALL
       ↓
Transform ALL
       ↓
Load ALL
       ↓
Data Warehouse
```

This is expensive and slow.

### Incremental ETL

Suppose today only 200,000 transactions were created/updated:

```text
500 million existing records
          +
200,000 new/changed records
          ↓
     Extract only
     200,000
          ↓
       Transform
          ↓
        Load
          ↓
    Data Warehouse
```

That's the basic idea.

---

# 2. Banking example

Let's take a **bank transaction table**.

Source:

```text
CAVW_ENTRIES
```

Suppose it contains:

| ENTRY_ID | ACCOUNT_ID | AMOUNT | TYPE   | STATUS | UPDATED_AT   |
| -------: | ---------- | -----: | ------ | ------ | ------------ |
|    10001 | A100       |    500 | DEBIT  | POSTED | Aug 21 10:00 |
|    10002 | A101       |    200 | CREDIT | POSTED | Aug 21 10:05 |
|    10003 | A102       |    750 | DEBIT  | POSTED | Aug 21 10:10 |
|      ... | ...        |    ... | ...    | ...    | ...          |
|     500M | ...        |    ... | ...    | ...    | ...          |

Yesterday's ETL finished at:

```text
2026-08-21 23:59:59
```

We save this as our **watermark**:

```text
last_successful_timestamp =
2026-08-21 23:59:59
```

---

# 3. Next day's ETL

At 1:00 AM:

Instead of:

```sql
SELECT *
FROM CAVW_ENTRIES;
```

we execute:

```sql
SELECT *
FROM CAVW_ENTRIES
WHERE UPDATED_AT > '2026-08-21 23:59:59';
```

Suppose this returns only:

```text
200,000 records
```

Those are our **incremental records**.

---

# 4. What happens during the day?

Imagine these transactions occur:

### Transaction 1

```text
10:05 AM

ENTRY_ID = 500000001
ACCOUNT = A500
AMOUNT = 1,000
STATUS = POSTED
```

### Transaction 2

```text
10:07 AM

ENTRY_ID = 500000002
ACCOUNT = A501
AMOUNT = 2,500
STATUS = POSTED
```

### Transaction 3

Initially:

```text
ENTRY_ID = 500000003
AMOUNT = 5,000
STATUS = PENDING
```

Later:

```text
STATUS = POSTED
```

So we have both:

```text
INSERT
```

and:

```text
UPDATE
```

Incremental ETL should capture both.

---

# 5. Incremental pipeline

A typical banking pipeline would look like:

```text
                 CORE BANKING
                     |
                     |
              CAVW_ENTRIES
                     |
                     v
             Incremental Extract
                     |
                     |
              UPDATED_AT >
              Last Watermark
                     |
                     v
                  BRONZE
                     |
                     v
                  SILVER
                     |
                     v
                   GOLD
                     |
                     v
              DATA WAREHOUSE
```

---

# 6. What is the watermark?

The **watermark** tells the ETL:

> "I have successfully processed everything up to this point."

For example:

```text
ETL Run #1
Watermark:
2026-08-20 23:59:59
```

After successful processing:

```text
ETL Run #2
Watermark:
2026-08-21 23:59:59
```

After the next successful run:

```text
ETL Run #3
Watermark:
2026-08-22 23:59:59
```

So each run knows where to continue.

---

# 7. Where do we store the watermark?

Usually in a control table.

For example:

```sql
CREATE TABLE ETL_CONTROL (
    pipeline_name VARCHAR(100),
    source_table VARCHAR(100),
    last_watermark TIMESTAMP,
    last_run_status VARCHAR(20),
    updated_at TIMESTAMP
);
```

Example:

| pipeline_name   | source_table | last_watermark      | status  |
| --------------- | ------------ | ------------------- | ------- |
| transaction_etl | CAVW_ENTRIES | 2026-08-21 23:59:59 | SUCCESS |

---

# 8. But there is a problem with UPDATEs

This is an important interview question.

Suppose:

```text
Transaction created:
10:00 AM

STATUS = PENDING
```

Then:

```text
10:30 AM

STATUS = POSTED
```

If you only extract newly created transactions, you may miss the update.

Therefore:

```sql
WHERE UPDATED_AT > :last_watermark
```

is usually better than:

```sql
WHERE CREATED_AT > :last_watermark
```

because `UPDATED_AT` captures both:

```text
INSERT
UPDATE
```

---

# 9. How do we load into the Data Warehouse?

Suppose the warehouse has:

```text
FACT_TRANSACTION
```

We receive:

```text
500000003
A500
5000
POSTED
```

We can use an **upsert/MERGE** operation.

Conceptually:

```sql
MERGE INTO FACT_TRANSACTION target
USING STAGING_TRANSACTION source
ON target.entry_id = source.entry_id

WHEN MATCHED THEN
    UPDATE SET
        target.amount = source.amount,
        target.status = source.status

WHEN NOT MATCHED THEN
    INSERT (...);
```

So:

```text
New transaction
      ↓
INSERT

Existing transaction changed
      ↓
UPDATE
```

---

# 10. Complete example

Imagine your bank has:

```text
CAVW_ENTRIES = 500,000,000 records
```

Yesterday:

```text
Last watermark
2026-08-21 23:59:59
```

Today:

```text
New transactions      = 180,000
Updated transactions  = 20,000
```

Total:

```text
200,000 records
```

### Full ETL

```text
500,000,000
     ↓
Extract
     ↓
Transform
     ↓
Load

Very expensive
```

### Incremental ETL

```text
200,000
   ↓
Extract
   ↓
Transform
   ↓
MERGE
   ↓
DWH
```

So you're processing:

```text
200,000 / 500,000,000
= 0.04%
```

instead of 100% of the table.

That's the major performance benefit.

---

# 11. Three common ways to implement incremental ETL

### Approach 1 — Timestamp

```sql
WHERE UPDATED_AT > :last_watermark
```

Simple and very common.

---

### Approach 2 — Increasing ID

If IDs are guaranteed to increase:

```sql
WHERE ENTRY_ID > :last_entry_id
```

For example:

```text
Last ID = 500000000

Next run:

ENTRY_ID > 500000000
```

Good for **insert-only** data.

But it doesn't reliably capture updates to old records.

---

### Approach 3 — CDC

For banking systems, CDC can be even better.

```text
Oracle
   |
   | Redo/Archive Logs
   v
Debezium / CDC
   |
   v
Kafka
   |
   v
ETL / Streaming
   |
   v
Data Lake / DWH
```

Now you capture events such as:

```text
INSERT transaction
UPDATE transaction
DELETE transaction
```

without repeatedly querying the entire source table.

---

# 12. Interview-level definition

If someone asks:

> **"What is incremental ETL?"**

A strong answer is:

> **Incremental ETL processes only the data that has been newly created or changed since the previous successful ETL execution, rather than reprocessing the entire source dataset. In banking, for example, instead of extracting all 500 million records from a transaction table every night, we can use an `UPDATED_AT` watermark or CDC to extract only the new and modified transactions, transform them, and upsert them into the data warehouse.**

### The key concept

```text
FULL ETL

Source
  ↓
ALL DATA
  ↓
DWH


INCREMENTAL ETL

Source
  ↓
NEW + CHANGED DATA
  ↓
DWH
```

And **CDC is essentially a more advanced/event-driven way of implementing incremental data capture**, particularly useful when you need near-real-time banking data.


## Is there any tool for incremental ETL?

Yes. There are many tools specifically suited for **incremental ETL/ELT**. The important point is that the tool usually implements incremental loading using one of several techniques: **watermarks, timestamps, CDC, or database logs**.

### Common tools

| Tool                   | Incremental approach   | Banking use                        |
| ---------------------- | ---------------------- | ---------------------------------- |
| **Debezium**           | CDC / database logs    | Excellent for Oracle/MySQL → Kafka |
| **Airbyte**            | Incremental + CDC      | Database/SaaS → DWH                |
| **Fivetran**           | Incremental + CDC      | Managed cloud pipelines            |
| **AWS DMS**            | Full load + CDC        | Oracle/MySQL → AWS                 |
| **Azure Data Factory** | Watermark + CDC        | Enterprise ETL                     |
| **Google Dataflow**    | Streaming/incremental  | GCP pipelines                      |
| **Apache NiFi**        | Incremental/flow-based | Enterprise integration             |
| **Informatica**        | Incremental + CDC      | Large banking enterprises          |
| **Talend**             | Incremental/CDC        | Enterprise ETL                     |
| **dbt**                | Incremental models     | DWH transformations                |
| **Apache Spark**       | Incremental/streaming  | Large-scale processing             |
| **Apache Flink**       | Streaming/CDC          | Real-time banking                  |

---

## For your banking example

Suppose:

```text
Oracle FLEXCUBE
      |
      v
CAVW_ENTRIES
500 million records
```

You could build:

```text
                 Oracle
                   |
                   | CDC
                   ↓
               Debezium
                   |
                   ↓
                 Kafka
                   |
                   ↓
              Spark/Flink
                   |
                   ↓
               Data Lake
                   |
                   ↓
                  dbt
                   |
                   ↓
             Data Warehouse
```

Here, **Debezium is doing the incremental capture**.

For example, if this happens:

```text
INSERT
TXN_ID = 10001
AMOUNT = 500
```

Debezium captures:

```text
INSERT → TXN_ID 10001
```

If later:

```text
UPDATE
TXN_ID = 10001
STATUS = POSTED
```

it captures:

```text
UPDATE → TXN_ID 10001
```

So you're not repeatedly doing:

```sql
SELECT * FROM CAVW_ENTRIES;
```

Instead, you're consuming **only changes**.

---

## Another approach: Airflow + SQL

You don't necessarily need a specialized CDC tool.

You could use **Airflow + Python/SQL**:

```text
                 Airflow
                    |
                    v
             Read watermark
                    |
                    v
              Oracle FLEXCUBE
                    |
                    |
         UPDATED_AT > watermark
                    |
                    v
                 Bronze
                    |
                    v
                 Silver
                    |
                    v
                  Gold
                    |
                    v
             Data Warehouse
                    |
                    v
             Update watermark
```

For example:

```sql
SELECT *
FROM CAVW_ENTRIES
WHERE UPDATED_AT > :last_watermark
  AND UPDATED_AT <= :current_watermark;
```

This is **incremental ETL using a watermark**.

---

## Which one should you choose?

For a bank, I'd think about it this way:

### Daily/hourly reporting

```text
Oracle
  ↓
Airflow
  ↓
Incremental SQL
  ↓
Data Lake/DWH
```

Simple and reliable.

### Near-real-time banking analytics

```text
Oracle
  ↓
Debezium
  ↓
Kafka
  ↓
Flink/Spark
  ↓
DWH/Data Lake
```

Better when you need seconds/minutes latency.

### Cloud-managed environment

```text
Oracle
  ↓
AWS DMS / Azure Data Factory / Fivetran
  ↓
Cloud Storage/DWH
```

Less infrastructure to maintain.

### Modern analytics engineering

```text
Source
  ↓
Fivetran/Airbyte/DMS
  ↓
Warehouse/Lakehouse
  ↓
dbt incremental models
  ↓
BI
```

Here **dbt itself can perform incremental transformations**, rather than reprocessing the entire target dataset.

---

### One important distinction

**Airflow is not an incremental ETL engine by itself.**

Airflow is primarily an **orchestrator**:

```text
Airflow
   |
   +--- Run extraction
   +--- Run transformation
   +--- Run validation
   +--- Run loading
   +--- Update watermark
   +--- Alert on failure
```

Whereas:

```text
Debezium → captures database changes
Spark    → processes data
dbt      → transforms warehouse data
Airflow  → orchestrates the pipeline
```

For the banking architecture you've been exploring, a very strong stack would be:

**Oracle/FLEXCUBE → Debezium CDC → Kafka → Spark/Flink → Data Lake → dbt → Data Warehouse → Power BI/Tableau.**


## For a Debezium tool, no watermark?

Correct — **with Debezium CDC, you normally don't use your own `UPDATED_AT` watermark** for incremental capture.

The key difference is **how Debezium knows where to continue**.

### Traditional incremental ETL

You manage a watermark:

```text
Oracle
   |
   | WHERE UPDATED_AT > last_watermark
   v
ETL
   |
   v
DWH
```

Example:

```text
last_watermark = 10:00:00

SELECT *
FROM transactions
WHERE updated_at > '10:00:00';
```

You maintain that timestamp.

---

### Debezium CDC

Debezium reads the database's **change stream/log**, rather than repeatedly querying the table.

For Oracle, this is based on Oracle's redo/archive logs through the appropriate Debezium Oracle connector mechanism.

```text
                 Oracle
                   |
                   | Redo / Change Logs
                   v
               Debezium
                   |
                   v
                  Kafka
                   |
                   v
              Downstream
```

Instead of:

```text
"What is the latest UPDATED_AT?"
```

Debezium effectively tracks:

> **"What is the last database-log position/change event I successfully processed?"**

For Kafka-based deployments, Debezium stores connector progress/offset information so it can resume after restart.

---

### Example

Suppose these changes happen:

```text
10:00:01  INSERT TX100
10:00:05  INSERT TX101
10:00:09  UPDATE TX100
10:00:15  INSERT TX102
```

Debezium emits change events:

```text
TX100 → INSERT
TX101 → INSERT
TX100 → UPDATE
TX102 → INSERT
```

It tracks its position in the change stream.

If Debezium stops after:

```text
10:00:09
```

and restarts, it can continue from its stored position rather than asking:

```sql
WHERE updated_at > '10:00:09'
```

### So think of it as:

| Traditional Incremental ETL              | Debezium CDC                    |
| ---------------------------------------- | ------------------------------- |
| Timestamp watermark                      | Log position / offset           |
| `UPDATED_AT`                             | Database change log             |
| Query source table                       | Read changes                    |
| Usually scheduled                        | Continuous                      |
| You manage watermark                     | Connector manages progress      |
| Can miss some changes if poorly designed | Designed to capture row changes |

### One important caveat

**"No watermark" doesn't mean "no state."**

Debezium absolutely needs to remember **where it is in the change stream**. That state is generally represented by connector offsets/log positions and related CDC metadata, rather than a business-table watermark like `UPDATED_AT`.

For your banking architecture, this distinction is important:

```text
OPTION A — Incremental ETL

Oracle
  ↓
UPDATED_AT
  ↓
Airflow
  ↓
DWH


OPTION B — CDC

Oracle
  ↓
Redo/Archive Logs
  ↓
Debezium
  ↓
Kafka
  ↓
Spark/Flink
  ↓
DWH
```

**Option B is usually much better when you need near-real-time transaction data and reliable capture of INSERT/UPDATE/DELETE changes.**
