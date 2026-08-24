Absolutely. Given your recent work with **Parquet + DuckDB + Apache Arrow + Flight SQL + data lakes**, Iceberg is the natural next topic.

The most important thing to understand is:

> **Apache Iceberg is not a database and not a file format. It is a table format that brings database-like table management, transactions, schema evolution, time travel, and concurrency to data stored in files such as Parquet.**

I'll teach it from **zero → architecture → internals → SQL → production → advanced concepts**, using **banking examples** throughout.

---

# 1. First: Where does Iceberg fit?

Let's start with the bigger picture.

A traditional banking database might look like:

```text
Application
     |
     v
MySQL / Oracle
     |
     v
Tables
```

For example:

```text
CUSTOMER
ACCOUNT
TRANSACTION
LOAN
PAYMENT
```

But imagine your bank has:

```text
10 billion transactions
20 TB of historical data
100 GB of new data every day
```

You don't necessarily want all historical analytical data sitting inside Oracle/MySQL.

So you move analytical data into a **data lake**:

```text
                Banking Systems
                      |
          +-----------+-----------+
          |           |           |
        Oracle      MySQL       Kafka
          |           |           |
          +-----------+-----------+
                      |
                     ETL
                      |
                      v
                 Data Lake
                      |
              +-------+-------+
              |               |
           Parquet          Parquet
              |               |
              +-------+-------+
                      |
                      v
                Spark / DuckDB
                      |
                      v
                    BI/ML
```

This works.

But there is a problem.

---

# 2. The problem with a plain data lake

Suppose you have:

```text
/data/transactions/
```

containing:

```text
part-001.parquet
part-002.parquet
part-003.parquet
...
part-500000.parquet
```

Your data lake is essentially just:

```text
Files + folders
```

There is no real table management layer.

Imagine you run:

```sql
UPDATE transactions
SET status = 'REVERSED'
WHERE transaction_id = 12345;
```

With ordinary Parquet:

**Parquet doesn't provide UPDATE.**

You generally need to:

1. Read affected data.
2. Modify it.
3. Write new Parquet files.
4. Replace old files.
5. Make sure readers don't see an inconsistent state.

This becomes difficult.

Other problems appear too:

### Schema changes

Today:

```text
transaction_id
account_id
amount
transaction_date
```

Tomorrow:

```text
transaction_id
account_id
amount
transaction_date
merchant_id
```

How do you safely manage that across thousands of files?

### Concurrent writes

Two Spark jobs write simultaneously.

```text
Job A ---> files
Job B ---> files
```

How do you know which files belong to which table version?

### Time travel

You want:

> "Show me the transaction table exactly as it existed yesterday at 6 PM."

Plain Parquet doesn't naturally provide this.

### Atomic commits

You want:

```text
Transaction A:
100 files changed
```

Readers should see either:

```text
BEFORE
```

or:

```text
AFTER
```

—not half of each.

This is where **Iceberg** comes in.

---

# 3. What exactly is Apache Iceberg?

Think of Iceberg as a **metadata and transaction layer over your data files**.

Conceptually:

```text
             Apache Iceberg
                   |
        +----------+----------+
        |          |          |
     Metadata   Transactions  Schema
        |          |          |
        +----------+----------+
                   |
                   v
              Data Files
              (Parquet)
```

So:

```text
Iceberg Table
     |
     +---- Metadata
     |
     +---- Snapshots
     |
     +---- Manifests
     |
     +---- Data files
             |
             +---- Parquet
             +---- Parquet
             +---- Parquet
```

This distinction is critical.

### Parquet

Answers:

> How is the data physically stored?

### Iceberg

Answers:

> Which files belong to the table, which version is current, what schema does the table have, and what changed between versions?

### Spark / Trino / Flink / DuckDB

Answer:

> How do I compute/query the data?

So:

```text
          Query Engine
       /       |       \
    Spark    Trino    DuckDB
       \       |       /
        \      |      /
         Apache Iceberg
               |
         +-----+-----+
         |           |
      Metadata    Data Files
                      |
                   Parquet
```

---

# 4. Iceberg is a table format

This is probably the most important definition.

Iceberg is a:

> **High-performance open table format for huge analytic datasets.**

It sits between:

```text
Storage
```

and:

```text
Query engine
```

For example:

```text
             Spark
               |
             Iceberg
               |
       +-------+-------+
       |               |
     S3/GCS         ADLS
       |
    Parquet
```

The storage doesn't have to be AWS S3.

It can be:

* S3
* GCS
* Azure Blob / ADLS
* HDFS
* local filesystem
* compatible object storage

---

# 5. Iceberg vs Parquet

This is where many people get confused.

Imagine:

```text
transactions.parquet
```

Parquet contains the actual rows.

For example:

```text
transaction_id | account_id | amount | date
------------------------------------------------
1001           | A001       | 100    | 2026-08-01
1002           | A002       | 250    | 2026-08-01
1003           | A003       | 500    | 2026-08-02
```

Iceberg doesn't replace Parquet.

Instead:

```text
Iceberg
   |
   +------ Parquet
   +------ Parquet
   +------ Parquet
   +------ Parquet
```

Think:

> **Parquet = data files**

> **Iceberg = table management**

---

# 6. Iceberg vs Delta Lake vs Hudi

These are often compared.

| Technology  | Type         |
| ----------- | ------------ |
| Parquet     | File format  |
| Iceberg     | Table format |
| Delta Lake  | Table format |
| Apache Hudi | Table format |

All solve similar problems around data lakes.

Conceptually:

```text
             Data Lake
                 |
       +---------+---------+
       |         |         |
    Iceberg    Delta     Hudi
       |         |         |
    Parquet   Parquet   Parquet
```

Iceberg is particularly strong in:

* open ecosystem
* schema evolution
* partition evolution
* hidden partitioning
* time travel
* snapshots
* concurrent writes
* engine interoperability

---

# 7. The four layers you need to understand

To master Iceberg, understand these four concepts:

```text
Iceberg Table
     |
     +-- Catalog
     |
     +-- Metadata
     |
     +-- Manifest
     |
     +-- Data files
```

Let's go one by one.

---

# 8. Iceberg Catalog

The **catalog** tells the query engine:

> Where is this table and what is its current metadata?

For example:

```text
banking
   |
   +-- transactions
   +-- customers
   +-- accounts
   +-- loans
```

A catalog maps:

```text
banking.transactions
```

to the Iceberg table metadata.

Different catalog implementations exist.

Examples include:

* REST Catalog
* Hive Catalog
* JDBC Catalog
* AWS Glue Catalog
* Nessie
* Hadoop catalog

Conceptually:

```text
Spark
  |
  | "Give me banking.transactions"
  v
Catalog
  |
  | "Here is the table metadata"
  v
Iceberg Metadata
```

---

# 9. Iceberg metadata

Suppose your table has:

```text
1,000 Parquet files
```

Iceberg doesn't ask Spark to blindly scan the entire directory.

Instead, metadata tells Iceberg:

```text
Current snapshot
       |
       v
Manifest list
       |
       v
Manifests
       |
       v
Data files
```

This hierarchy is extremely important.

---

# 10. The Iceberg architecture

A simplified Iceberg table:

```text
                 Iceberg Table
                       |
                       v
                  Metadata JSON
                       |
                       v
                Current Snapshot
                       |
                       v
                Manifest List
                       |
            +----------+----------+
            |          |          |
        Manifest   Manifest    Manifest
            |          |          |
            v          v          v
        Parquet    Parquet     Parquet
```

There are two major types of metadata files you should remember:

### Manifest list

Describes manifests associated with a snapshot.

### Manifest

Describes data files.

Then:

```text
Manifest
    |
    +-- file1.parquet
    +-- file2.parquet
    +-- file3.parquet
```

---

# 11. Why manifests matter

Imagine:

```text
10 million Parquet files
```

A query:

```sql
SELECT *
FROM transactions
WHERE transaction_date = '2026-08-24';
```

Iceberg can use metadata to eliminate irrelevant files.

Instead of:

```text
Scan 10,000,000 files
```

it may identify:

```text
Relevant manifests
       |
       v
Relevant Parquet files
       |
       v
Read only those files
```

This is called **metadata-based pruning**.

---

# 12. Data file statistics

Iceberg metadata can contain statistics about files.

For example:

```text
File: part-001.parquet

transaction_date:
    min = 2026-08-01
    max = 2026-08-01

amount:
    min = 10
    max = 5000
```

Suppose query:

```sql
WHERE transaction_date = '2026-08-24'
```

Iceberg knows:

```text
part-001:
max date = Aug 1
```

Therefore:

```text
SKIP
```

This is called **data file pruning**.

---

# 13. Snapshots — one of Iceberg's superpowers

Every committed change creates a new **snapshot**.

Imagine:

```text
Snapshot 1
    |
    v
10 Parquet files
```

Then you insert data:

```text
Snapshot 2
    |
    v
12 Parquet files
```

Then update:

```text
Snapshot 3
    |
    v
15 Parquet files
```

The previous snapshots remain available until expired.

Conceptually:

```text
Snapshot 1
     |
     v
Snapshot 2
     |
     v
Snapshot 3
     |
     v
Snapshot 4   <-- CURRENT
```

This enables:

> **Time travel**

---

# 14. Iceberg time travel

Suppose today's table contains:

```text
10,000 transactions
```

Yesterday:

```text
9,500 transactions
```

You can query a historical snapshot.

Conceptually:

```sql
SELECT *
FROM transactions
FOR SYSTEM_TIME AS OF ...
```

Exact SQL syntax depends on the engine.

The important concept is:

```text
Current table
     |
     +---- Snapshot 4
     |
     +---- Snapshot 3
     |
     +---- Snapshot 2
     |
     +---- Snapshot 1
```

You can ask:

> What did the table look like before the bad ETL job?

This is extremely useful in banking.

---

# 15. Banking example: accidental corruption

Suppose:

```text
23:00
```

Your ETL runs.

A bug changes:

```text
100 million transaction records
```

You discover it at:

```text
23:30
```

With Iceberg:

```text
Snapshot 100
    |
    | good
    v
Snapshot 101
    |
    | bad ETL
    v
Snapshot 102
```

You can inspect:

```text
Snapshot 100
```

and potentially roll the table back to a previous snapshot, depending on your operational strategy.

That's much safer than manually reconstructing Parquet files.

---

# 16. ACID transactions

Iceberg provides table-level transactional semantics.

Think:

```text
Transaction
     |
     +---- Write files
     |
     +---- Update metadata
     |
     +---- Commit
```

Readers see:

```text
Snapshot N
```

or:

```text
Snapshot N+1
```

rather than a partially committed state.

This is one of the biggest differences between:

```text
Plain Parquet data lake
```

and:

```text
Iceberg data lake
```

---

# 17. INSERT

Suppose:

```sql
INSERT INTO transactions
SELECT ...
```

Conceptually:

```text
Existing snapshot
       |
       v
Write new Parquet files
       |
       v
Create new metadata
       |
       v
Commit new snapshot
```

The original files aren't necessarily modified.

Instead, Iceberg creates a new table state.

---

# 18. UPDATE

This is where Iceberg becomes very interesting.

Suppose:

```sql
UPDATE transactions
SET amount = 500
WHERE transaction_id = 1001;
```

Parquet isn't naturally an update-oriented format.

Iceberg can implement row-level changes using different mechanisms depending on the engine/table configuration.

One common approach is:

```text
Old file
   |
   | affected rows
   v
New file
```

The metadata changes which files belong to the current snapshot.

So conceptually:

```text
Snapshot 1

file-A.parquet
file-B.parquet
file-C.parquet
```

After update:

```text
Snapshot 2

file-A.parquet
file-B-new.parquet
file-C.parquet
```

The old file can remain available to historical snapshots until cleanup.

---

# 19. DELETE

Similarly:

```sql
DELETE FROM transactions
WHERE transaction_id = 1001;
```

Iceberg can represent the deletion through file replacement or delete files, depending on the operation/table format.

This leads us to an advanced concept:

# Delete files

Iceberg supports delete files such as:

```text
Position deletes
Equality deletes
```

Conceptually:

```text
Data file
   |
   +---- rows
   |
Delete file
   |
   +---- rows to remove
```

This can avoid rewriting the entire data file for certain workloads.

---

# 20. Copy-on-write vs Merge-on-read

This is important for advanced Iceberg.

### Copy-on-write

When you update:

```text
Read old file
      |
Modify rows
      |
Write new file
```

So:

```text
Old data file
     |
     X
     |
New data file
```

Good for:

* read-heavy workloads
* simpler reads

But updates can be expensive.

---

### Merge-on-read

Instead:

```text
Original Parquet
       +
Delete/Update files
```

Reads combine them.

Conceptually:

```text
          Query
            |
       +----+----+
       |         |
   Data files  Delete files
       |         |
       +----+----+
            |
          Result
```

Good for:

* frequent updates
* CDC
* streaming

But reads become more complex.

---

# 21. Iceberg and CDC

This is particularly relevant to your banking architecture.

Imagine:

```text
Oracle / MySQL
       |
    Debezium
       |
      Kafka
       |
     Spark/Flink
       |
   Iceberg Table
```

CDC events:

```text
INSERT
UPDATE
DELETE
```

can be applied to an Iceberg table.

For example:

```text
Core Banking
     |
     v
Debezium
     |
     v
Kafka
     |
     v
Flink
     |
     v
Iceberg
     |
     +---- Current transactions
     |
     +---- Historical snapshots
```

This gives you a powerful lakehouse architecture.

---

# 22. Schema evolution

Traditional relational databases often require careful migrations.

Iceberg supports schema evolution.

Suppose initial schema:

```text
transaction_id
account_id
amount
```

Later:

```text
transaction_id
account_id
amount
merchant_id
```

You can add:

```text
merchant_id
```

without rewriting every historical Parquet file.

Older files simply don't have that column.

Iceberg understands the schema evolution.

---

# 23. Why Iceberg uses column IDs

This is a subtle but extremely important concept.

Suppose:

```text
amount
```

has internal ID:

```text
field-id = 3
```

Later you rename:

```text
amount
```

to:

```text
transaction_amount
```

Iceberg can preserve the field identity.

So:

```text
ID 3
  |
  +---- amount
  |
  +---- transaction_amount
```

The column's identity is not merely its name.

This is one reason Iceberg schema evolution is robust.

---

# 24. Rename column

Suppose:

```sql
amount
```

becomes:

```text
transaction_amount
```

You don't want historical files to suddenly become invalid because they still refer to:

```text
amount
```

Iceberg's field IDs help maintain that logical identity.

This is one of the architectural differences from simplistic "directory of Parquet files" approaches.

---

# 25. Drop column

Suppose:

```text
customer_phone
```

is no longer required.

You can evolve the schema without necessarily rewriting every historical file.

Again:

```text
Schema v1
    |
    v
Schema v2
    |
    v
Schema v3
```

The table's metadata records these changes.

---

# 26. Partitioning

This is another major Iceberg feature.

Suppose your transaction data is:

```text
5 TB
```

You might partition by:

```text
transaction_date
```

Traditional data lake:

```text
transactions/
   year=2026/
      month=08/
         day=24/
```

Iceberg can manage partitioning differently.

For example:

```sql
PARTITIONED BY (days(transaction_date))
```

The query engine doesn't necessarily need to expose physical partition columns to users.

This leads to:

# Hidden partitioning

---

# 27. Hidden partitioning

Traditional Hive-style partitioning:

```text
/year=2026/month=08/day=24/
```

The query may need to understand those partition columns.

Iceberg can define:

```text
days(transaction_timestamp)
```

and users simply query:

```sql
WHERE transaction_timestamp >= ...
```

Iceberg handles partition pruning.

This is called **hidden partitioning**.

---

# 28. Partition evolution

This is one of Iceberg's biggest advantages.

Suppose initially:

```text
Partition by month
```

Then the bank grows.

You now want:

```text
Partition by day
```

Traditional data lakes can make this painful because old and new directory structures become complicated.

Iceberg allows partition evolution.

Conceptually:

```text
Old data
    |
Partition spec v1
    |
month(transaction_date)

New data
    |
Partition spec v2
    |
day(transaction_date)
```

The table can contain both.

```text
Table
 |
 +---- old files → monthly partitioning
 |
 +---- new files → daily partitioning
```

The query engine understands both.

This is a **huge** feature for long-lived data platforms.

---

# 29. Why partition evolution matters in banking

Imagine:

```text
2020:
100 GB/month

2026:
20 TB/month
```

You originally chose:

```text
monthly partitions
```

But now monthly partitions are too large.

With Iceberg:

```text
Old data
monthly

New data
daily
```

You don't have to rewrite the entire historical dataset immediately.

---

# 30. Sorting / clustering

Partitioning alone isn't enough.

Suppose:

```text
transactions
```

has:

```text
10 billion rows
```

and queries frequently filter:

```text
customer_id
```

You can organize files to improve locality.

For example:

```text
partition:
transaction_date

sort/order:
customer_id
```

This improves data skipping and query performance.

Modern lakehouse design often combines:

```text
Partitioning
+
Sorting
+
File statistics
+
Compaction
```

---

# 31. Iceberg and object storage

A very common production architecture is:

```text
                  Applications
                       |
                       v
                    Kafka
                       |
                 Spark/Flink
                       |
                       v
                 Apache Iceberg
                       |
          +------------+------------+
          |            |            |
         S3           S3           S3
          |
       Parquet
```

For AWS:

```text
S3
 |
 +---- Iceberg metadata
 |
 +---- Parquet files
 |
 +---- manifest files
```

The compute engine can be separated from storage.

This is one of the major ideas behind modern cloud data platforms.

---

# 32. Iceberg + Spark

Spark is one of the most common engines used with Iceberg.

Conceptually:

```text
Spark
  |
  v
Iceberg Catalog
  |
  v
Iceberg Table
  |
  v
S3
```

Example:

```sql
SELECT
    branch_id,
    SUM(amount)
FROM banking.transactions
WHERE transaction_date >= '2026-08-01'
  AND transaction_date < '2026-09-01'
GROUP BY branch_id;
```

Spark doesn't need to manually understand:

```text
year=2026/month=08/day=...
```

Iceberg manages the table metadata and partitioning.

---

# 33. Iceberg + Flink

For streaming:

```text
Kafka
   |
   v
Flink
   |
   v
Iceberg
```

This is especially useful for:

```text
real-time transaction analytics
fraud detection
customer 360
operational analytics
CDC
```

For example:

```text
ATM transaction
       |
       v
Kafka
       |
       v
Flink
       |
       v
Iceberg
       |
       +---- fraud analytics
       +---- BI
       +---- ML
```

---

# 34. Iceberg + Trino

Trino can query Iceberg directly.

Architecture:

```text
BI Tool
   |
   v
Trino
   |
   v
Iceberg
   |
   v
S3 / Parquet
```

This gives you SQL access without requiring Spark for every query.

---

# 35. Iceberg + DuckDB

This is particularly relevant to what you've been learning.

DuckDB can work with Iceberg tables, although the exact capabilities depend on the DuckDB version/extensions and Iceberg integration being used.

Conceptually:

```text
DuckDB
   |
   v
Iceberg
   |
   v
Parquet
   |
   v
Object Storage
```

So your earlier architecture:

```text
Flight SQL
    |
    v
DuckDB
    |
    v
Parquet
```

can evolve toward:

```text
Flight SQL
    |
    v
DuckDB
    |
    v
Iceberg
    |
    v
Parquet
```

The important difference is that Iceberg now gives your Parquet data a proper **table layer**.

---

# 36. Iceberg + Apache Arrow

You were recently asking about Arrow.

These technologies solve different problems.

### Apache Arrow

In-memory data representation.

```text
CPU memory
   |
Arrow columns
```

### Parquet

On-disk columnar storage.

```text
Disk/Object Storage
   |
Parquet
```

### Iceberg

Table management.

```text
Table metadata
   |
Snapshots
   |
Manifests
   |
Parquet
```

So:

```text
             Query Engine
                  |
                Arrow
             in memory
                  |
                Iceberg
             table layer
                  |
               Parquet
             storage format
                  |
            S3 / Data Lake
```

They complement each other.

---

# 37. Iceberg vs a Data Warehouse

This distinction is also important.

Iceberg:

```text
Table format
```

Warehouse:

```text
Database/analytical platform
```

For example:

```text
                    Data Platform

                 +----------------+
                 |    Iceberg     |
                 |   Data Lake    |
                 +----------------+
                         |
              +----------+----------+
              |          |          |
            Spark      Trino      Flink
```

You could use Iceberg as the foundation of a **lakehouse**.

---

# 38. Data Lake vs Lakehouse

### Data Lake

```text
S3
 |
 +-- Parquet
 +-- JSON
 +-- CSV
```

Mostly:

```text
Files
```

### Lakehouse

```text
S3
 |
Iceberg
 |
 +-- transactions
 +-- customers
 +-- accounts
 +-- loans
```

Now you have:

* transactions
* schemas
* snapshots
* ACID
* table metadata
* evolution
* SQL
* governance possibilities

This is the **lakehouse** idea.

---

# 39. Banking lakehouse architecture

A realistic architecture for your banking environment could be:

```text
                 CORE BANKING
                      |
              Oracle / MySQL
                      |
                      v
                  Debezium
                      |
                      v
                    Kafka
                      |
          +-----------+-----------+
          |                       |
      Streaming                Batch
       Flink                   Spark
          |                       |
          +-----------+-----------+
                      |
                      v
              Apache Iceberg
                      |
              Object Storage
             S3 / MinIO / etc.
                      |
        +-------------+-------------+
        |             |             |
      Trino         Spark         DuckDB
        |             |             |
        +-------------+-------------+
                      |
          +-----------+-----------+
          |           |           |
         BI          ML       Fraud Detection
```

This is a very powerful modern architecture.

---

# 40. Bronze / Silver / Gold with Iceberg

Your previous questions about Bronze/Silver/Gold connect directly.

You could have:

```text
                    Iceberg Lakehouse

Bronze
  |
  +---- raw_transactions
  +---- raw_customers
  +---- raw_accounts

              ↓

Silver
  |
  +---- transactions_clean
  +---- customers_clean
  +---- accounts_clean

              ↓

Gold
  |
  +---- customer_360
  +---- branch_performance
  +---- daily_transactions
  +---- loan_analytics
  +---- fraud_features
```

Each can be an Iceberg table.

---

# 41. Example: banking transaction table

Imagine:

```text
transactions
```

with:

```text
transaction_id
account_id
customer_id
branch_id
transaction_type
amount
currency
transaction_timestamp
merchant_id
channel
status
```

Initial table:

```text
10 billion records
```

stored as:

```text
Parquet files
```

managed by:

```text
Iceberg
```

---

# 42. Querying it

You might execute:

```sql
SELECT
    branch_id,
    SUM(amount) AS total_amount
FROM transactions
WHERE transaction_timestamp >= TIMESTAMP '2026-08-01 00:00:00'
  AND transaction_timestamp < TIMESTAMP '2026-09-01 00:00:00'
GROUP BY branch_id;
```

The logical flow is:

```text
SQL
 |
 v
Query Engine
 |
 v
Iceberg
 |
 +---- catalog
 |
 +---- snapshot
 |
 +---- manifests
 |
 +---- file statistics
 |
 v
Relevant Parquet files
 |
 v
Arrow / engine memory
 |
 v
Aggregation
 |
 v
Result
```

---

# 43. Query planning

This is where Iceberg becomes really interesting.

Suppose:

```text
10,000 Parquet files
```

Your query:

```sql
WHERE transaction_date = '2026-08-24'
```

Iceberg may perform:

```text
1. Load current snapshot
          ↓
2. Load manifest list
          ↓
3. Identify relevant manifests
          ↓
4. Check file statistics
          ↓
5. Skip irrelevant files
          ↓
6. Read remaining Parquet files
```

So you don't necessarily scan everything.

---

# 44. Snapshot isolation

Suppose:

```text
Query A starts
```

at:

```text
Snapshot 100
```

Then:

```text
ETL job commits Snapshot 101
```

Query A can continue reading a consistent view of:

```text
Snapshot 100
```

while new readers use:

```text
Snapshot 101
```

Conceptually:

```text
              Table
                |
        +-------+-------+
        |               |
   Query A           Query B
        |               |
 Snapshot 100       Snapshot 101
```

This is a major reason Iceberg works well in concurrent environments.

---

# 45. Optimistic concurrency

Iceberg uses optimistic concurrency principles.

Imagine:

```text
Job A reads Snapshot 100
Job B reads Snapshot 100
```

Both perform changes.

Then:

```text
Job A commits
```

creating:

```text
Snapshot 101
```

Job B tries to commit based on old state.

Iceberg checks whether its assumptions are still valid.

If there is a conflict, the commit can fail or be retried/rebased depending on the operation and engine.

This is much safer than:

```text
Both jobs blindly overwrite the directory.
```

---

# 46. Iceberg metadata hierarchy — memorize this

You should remember:

```text
Catalog
   |
   v
Table metadata
   |
   v
Snapshot
   |
   v
Manifest list
   |
   v
Manifest
   |
   v
Data files
   |
   v
Parquet
```

And:

```text
Data file
    |
    +---- statistics
    +---- partition information
    +---- record count
```

If you understand this hierarchy, you've understood a large part of Iceberg's internals.

---

# 47. Metadata cleanup

There is a problem with time travel.

Suppose you create:

```text
1 snapshot/day
```

for:

```text
5 years
```

You could have:

```text
1,825 snapshots
```

Plus old metadata and files.

Therefore Iceberg provides maintenance operations.

Common maintenance concepts include:

```text
Expire snapshots
Remove orphan files
Rewrite manifests
Rewrite data files
Rewrite delete files
```

---

# 48. Compaction

This is very important in production.

Suppose streaming produces:

```text
100,000 tiny Parquet files
```

This is bad.

Why?

Because:

```text
100,000 files
```

means:

* metadata overhead
* object-storage operations
* file-open overhead
* query planning overhead

You want:

```text
many tiny files
       ↓
compaction
       ↓
fewer larger files
```

Conceptually:

```text
file1  \
file2   \
file3    ---> compact ---> large-file-1
file4   /
file5  /
```

This is called **data file compaction / rewrite**.

---

# 49. The small-file problem

Suppose:

```text
Kafka
1000 events/sec
```

and your streaming job writes:

```text
one file every few seconds
```

After a month:

```text
hundreds of thousands of files
```

Your table may still be logically correct.

But performance can degrade.

Therefore production Iceberg architecture often includes:

```text
Streaming ingestion
        |
        v
Iceberg
        |
        v
Compaction
        |
        v
Optimized Iceberg table
```

---

# 50. Metadata compaction

You can also accumulate too many manifests.

For example:

```text
Manifest 1
Manifest 2
Manifest 3
...
Manifest 100,000
```

Iceberg supports rewriting/merging manifests.

So there are two different optimization areas:

```text
Data files
   |
   +---- compact/rewrite data

Metadata files
   |
   +---- rewrite manifests
```

Don't confuse these.

---

# 51. Snapshot expiration

Suppose you want:

```text
Keep snapshots for 30 days
```

Then old snapshots can be expired.

Conceptually:

```text
2026-07-01
2026-07-02
...
2026-07-25
2026-07-26
2026-07-27
...
2026-08-24
```

After expiration:

```text
Keep last 30 days
```

Old metadata and files that are no longer referenced can eventually be cleaned.

This reduces storage cost.

---

# 52. Orphan files

Suppose an ETL job writes:

```text
file-A.parquet
file-B.parquet
```

but crashes before committing metadata.

Those files may no longer be referenced by the table.

These are potentially:

```text
orphan files
```

Maintenance can identify and remove them safely according to your retention policy.

---

# 53. Iceberg REST Catalog

A modern architecture increasingly uses a REST-based catalog.

Conceptually:

```text
Spark
   |
Trino
   |
Flink
   |
DuckDB
   |
   v
REST Catalog
   |
   v
Iceberg
   |
   v
Object Storage
```

The advantage is decoupling catalog clients from a specific catalog implementation.

---

# 54. Nessie

Another interesting technology is Project Nessie.

It provides a Git-like approach to data lake tables.

Conceptually:

```text
Git

main
 |
 +-- commit
 +-- commit
 +-- branch
 +-- merge
```

Nessie brings similar ideas to data/catalog management.

Conceptually:

```text
main
 |
 +-- Iceberg tables
 |
 +-- commit
 |
 +-- branch
 |
 +-- experiment
```

This becomes interesting for:

* data engineering
* experimentation
* dev/test environments
* reproducibility

---

# 55. Iceberg branching and tagging

Modern Iceberg supports concepts such as:

```text
Branches
Tags
```

Think:

```text
main
 |
 +---- snapshot 100
 |
 +---- snapshot 101
 |
 +---- snapshot 102
```

You might create a branch for experimentation.

This is conceptually similar to:

```text
Git branch
```

but for table state.

This can be powerful for:

```text
ML experimentation
ETL testing
data validation
safe deployments
```

---

# 56. Example: ML feature development

Imagine:

```text
transactions
```

You create:

```text
fraud_features
```

You want to test a new transformation.

Instead of modifying production immediately:

```text
production
    |
    +---- branch: fraud-model-v2
```

Test:

```text
new feature logic
```

Validate:

```text
precision
recall
false positives
```

Then promote the data state.

This is one of the more advanced lakehouse patterns.

---

# 57. Iceberg schema evolution vs database migration

Traditional:

```text
ALTER TABLE transactions
ADD merchant_id VARCHAR(50);
```

Iceberg:

```text
Schema evolution
        |
        +---- add column
        +---- rename column
        +---- reorder column
        +---- widen types
        +---- drop column
```

The key difference is that historical files don't necessarily have to be rewritten.

---

# 58. Iceberg partition evolution vs schema evolution

Don't mix them up.

### Schema evolution

Changes:

```text
columns
```

Example:

```text
amount
merchant_id
currency
```

### Partition evolution

Changes:

```text
how data is organized
```

Example:

```text
month(transaction_date)
```

to:

```text
day(transaction_date)
```

Both are important.

---

# 59. Iceberg format versions

Iceberg has evolved through table format versions.

A simplified view:

```text
Format v1
   |
   v
Format v2
```

Format v2 introduced important support around row-level deletes and more advanced mutation semantics.

When building production systems, you need to understand:

```text
table format version
```

because capabilities and compatibility can depend on it.

---

# 60. Iceberg vs Hive tables

Traditional Hive-style table:

```text
transactions/
    year=2026/
        month=08/
            day=24/
```

Metadata is often heavily tied to:

```text
directory structure
```

Iceberg separates:

```text
logical table
```

from:

```text
physical layout
```

This is a fundamental architectural improvement.

---

# 61. Iceberg vs Hive partitioning

Traditional:

```text
WHERE year = 2026
AND month = 8
AND day = 24
```

Iceberg:

```text
WHERE transaction_timestamp >= ...
AND transaction_timestamp < ...
```

The user queries the business column.

Iceberg handles partition transformation.

This is the idea of:

> **Hidden partitioning**

---

# 62. A complete banking pipeline

Let's put everything together.

```text
                CORE BANKING
                     |
             +-------+-------+
             |               |
          Oracle           MySQL
             |               |
             +-------+-------+
                     |
                  Debezium
                     |
                     v
                   Kafka
                     |
             +-------+-------+
             |               |
           Flink            Spark
             |               |
             +-------+-------+
                     |
                     v
              Apache Iceberg
                     |
               Object Storage
                S3 / MinIO
                     |
        +------------+-------------+
        |            |             |
      Trino        Spark         DuckDB
        |            |             |
        +------------+-------------+
                     |
          +----------+----------+
          |          |          |
         BI         ML       Fraud
```

---

# 63. Where Apache Arrow fits

Now add Arrow:

```text
Object Storage
      |
   Iceberg
      |
   Parquet
      |
 Query Engine
      |
    Arrow
      |
    Memory
      |
   Analytics
```

So the technologies you've been studying fit together beautifully:

```text
Kafka
  ↓
Flink/Spark
  ↓
Iceberg
  ↓
Parquet
  ↓
Arrow
  ↓
DuckDB / Trino / Spark
  ↓
BI / ML
```

Each technology has a different job.

---

# 64. Where DuckDB fits

You were asking recently whether DuckDB is a lakehouse database.

The better mental model is:

```text
DuckDB
=
embedded analytical query engine
```

Iceberg:

```text
table format
```

Parquet:

```text
file format
```

Arrow:

```text
in-memory format
```

So:

```text
                DuckDB
             Query Engine
                  |
                  v
              Iceberg
             Table Layer
                  |
                  v
              Parquet
             File Layer
                  |
                  v
           Object Storage
```

This is much cleaner than thinking:

> DuckDB contains the Parquet files.

Usually, it doesn't.

---

# 65. Where Flight SQL fits

Similarly:

```text
Client
   |
Flight SQL
   |
   v
Query Server
   |
   v
DuckDB / Trino
   |
   v
Iceberg
   |
   v
Parquet
```

Flight SQL is a **data access protocol**.

Iceberg is a **table format**.

DuckDB is a **query engine**.

Parquet is a **file format**.

Arrow is an **in-memory representation / interchange format**.

These are complementary layers.

---

# 66. The complete mental model

I recommend memorizing this:

```text
┌───────────────────────────────┐
│           BI / ML             │
└───────────────┬───────────────┘
                │
        Query / Data Access
                │
       Trino / Spark / DuckDB
                │
       ┌────────▼────────┐
       │  Apache Iceberg │
       │   Table Format  │
       └────────┬────────┘
                │
        Metadata / Snapshots
                │
       ┌────────▼────────┐
       │    Parquet      │
       │   File Format   │
       └────────┬────────┘
                │
       Object Storage / HDFS
```

And during processing:

```text
Parquet
   ↓
Arrow
   ↓
CPU Memory
```

---

# 67. What happens when you run a query?

Let's trace one request end-to-end.

Customer analytics application sends:

```sql
SELECT SUM(amount)
FROM transactions
WHERE transaction_date = '2026-08-24';
```

### Step 1

Query engine receives SQL.

```text
SQL
 ↓
Trino / Spark / DuckDB
```

### Step 2

Engine asks Iceberg:

```text
What is the current table?
```

### Step 3

Iceberg catalog returns:

```text
table metadata
```

### Step 4

Iceberg identifies:

```text
current snapshot
```

### Step 5

Snapshot points to:

```text
manifest list
```

### Step 6

Manifest list points to:

```text
manifests
```

### Step 7

Manifests identify:

```text
relevant Parquet files
```

### Step 8

Statistics eliminate irrelevant files.

### Step 9

Query engine reads remaining Parquet files.

### Step 10

Data is processed in memory.

Often using:

```text
columnar execution
```

and potentially Arrow-based representations depending on the engine.

### Step 11

Result:

```text
SUM(amount)
```

is returned.

---

# 68. What happens during an INSERT?

```text
INSERT
  |
  v
Query Engine
  |
  v
Write Parquet
  |
  v
Create/update Iceberg metadata
  |
  v
Commit snapshot
  |
  v
New current table state
```

---

# 69. What happens during UPDATE?

Conceptually:

```text
UPDATE
  |
  v
Identify affected data
  |
  v
Rewrite data and/or create delete files
  |
  v
Create new metadata
  |
  v
Commit
  |
  v
New snapshot
```

---

# 70. What happens if two jobs write simultaneously?

```text
             Snapshot 100
              /        \
             /          \
          Job A        Job B
             |            |
             v            v
        Prepare        Prepare
             |            |
             v            v
          Commit
             |
             v
        Snapshot 101
```

The second commit must validate against the latest table state.

This prevents many forms of corruption that would occur with unmanaged files.

---

# 71. What should you learn to become advanced?

I would divide Iceberg mastery into **six levels**.

### Level 1 — Fundamentals

Learn:

```text
Data Lake
Data Warehouse
Lakehouse
Parquet
Iceberg
Table format
Catalog
```

### Level 2 — Iceberg internals

Learn:

```text
Table metadata
Snapshots
Manifest lists
Manifests
Data files
Delete files
File statistics
```

### Level 3 — Data engineering

Learn:

```text
Partitioning
Hidden partitioning
Partition evolution
Schema evolution
Compaction
Snapshot expiration
Orphan files
```

### Level 4 — Distributed systems

Learn:

```text
ACID
Snapshot isolation
Optimistic concurrency
Atomic commits
Concurrent writers
Conflict detection
```

### Level 5 — Streaming / CDC

Learn:

```text
Kafka
Debezium
Flink
CDC
UPSERT
DELETE
Merge-on-read
Delete files
```

### Level 6 — Production lakehouse

Learn:

```text
REST Catalog
AWS Glue
Nessie
Security
Governance
Catalog HA
Object storage
Maintenance
Monitoring
Cost optimization
Query optimization
```

---

# 72. A practical learning project for you

Instead of learning Iceberg only theoretically, I recommend building this:

```text
                BANKING ICEBERG LAB
                       |
                       v
             MySQL Transactions
                       |
                       v
                   Debezium
                       |
                       v
                     Kafka
                       |
                       v
                    Spark
                       |
                       v
                  Iceberg
                       |
                       v
                  MinIO/S3
                       |
          +------------+------------+
          |            |            |
        Trino        DuckDB       Spark
          |            |            |
          +------------+------------+
                       |
                       v
                 Banking BI
```

Use this table:

```text
transactions
```

with:

```text
transaction_id
account_id
customer_id
branch_id
amount
currency
transaction_type
transaction_timestamp
merchant_id
status
```

Then progressively implement:

```text
Phase 1
Create Iceberg table

Phase 2
Insert 1 million transactions

Phase 3
Query with Spark

Phase 4
Query with DuckDB

Phase 5
Add partitioning

Phase 6
Change schema

Phase 7
Perform UPDATE

Phase 8
Perform DELETE

Phase 9
Time travel

Phase 10
CDC from Kafka

Phase 11
Compaction

Phase 12
Snapshot expiration

Phase 13
Concurrent writers

Phase 14
REST Catalog

Phase 15
Production architecture
```

That would take you from **beginner to genuinely production-capable**.

---

# 73. The key comparison

Keep this table in your notes:

| Technology   | Main responsibility               |
| ------------ | --------------------------------- |
| Kafka        | Event streaming                   |
| Debezium     | CDC                               |
| Spark        | Distributed processing            |
| Flink        | Streaming processing              |
| DuckDB       | Embedded analytical SQL engine    |
| Trino        | Distributed SQL query engine      |
| Arrow        | In-memory columnar representation |
| Parquet      | Columnar file format              |
| Iceberg      | Table format                      |
| S3/MinIO     | Object storage                    |
| Flight SQL   | SQL/data transport protocol       |
| REST Catalog | Iceberg catalog access            |

And the architecture:

```text
             Kafka
               │
          Debezium / CDC
               │
               ▼
        Spark / Flink
               │
               ▼
        ┌──────────────┐
        │   Iceberg    │
        │ Table Format │
        └──────┬───────┘
               │
            Parquet
               │
               ▼
        S3 / MinIO / ADLS
               │
       ┌───────┼────────┐
       │       │        │
    Trino   DuckDB    Spark
       │       │        │
       └───────┼────────┘
               │
             Arrow
               │
               ▼
            BI / ML
```

## The one sentence I want you to remember

> **Apache Iceberg turns a collection of Parquet files in a data lake into a reliable, transactional, evolvable analytical table.**

And the most important hierarchy to memorize is:

```text
Catalog
   ↓
Table Metadata
   ↓
Snapshot
   ↓
Manifest List
   ↓
Manifest
   ↓
Data File
   ↓
Parquet
   ↓
Rows
```

Given what you've already been learning, **the best next step is not another theory section**. We can build a **complete hands-on banking Iceberg lab with Docker: MinIO + Iceberg REST Catalog + Spark + Trino + DuckDB**, create a `transactions` table, load data, perform `INSERT/UPDATE/DELETE`, demonstrate **snapshots/time travel, schema evolution, partition evolution, compaction, and finally connect Kafka/CDC**. That will make the architecture click much faster.
