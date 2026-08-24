## Why can't we use DuckDB instead of iceberg?

You **can use DuckDB without Iceberg**—but they solve different problems.

The simplest way to remember it:

> **DuckDB = query engine.**
> **Iceberg = table/storage-management layer.**

They are not really replacements for each other.

### Think about your banking data

Suppose you have:

```text
S3 / MinIO
   |
   +-- transactions-001.parquet
   +-- transactions-002.parquet
   +-- transactions-003.parquet
   +-- ...
```

You can absolutely use DuckDB:

```python
import duckdb

con = duckdb.connect()

df = con.execute("""
    SELECT
        branch_id,
        SUM(amount)
    FROM read_parquet(
        's3://banking/transactions/*.parquet'
    )
    GROUP BY branch_id
""").df()
```

This is excellent for **querying** the Parquet files.

But now consider what happens when your banking platform becomes large and operationally complex.

---

## 1. DuckDB answers: "How do I query the data?"

DuckDB is an analytical SQL engine.

```text
                 DuckDB
                    |
             SQL execution
                    |
          +---------+---------+
          |                   |
       Parquet              CSV
          |                   |
          +---------+---------+
                    |
                Result
```

It gives you:

* SQL
* joins
* aggregations
* window functions
* vectorized execution
* parallel processing
* Parquet scanning
* analytical performance

For example:

```sql
SELECT
    branch_id,
    SUM(amount)
FROM read_parquet('transactions/*.parquet')
GROUP BY branch_id;
```

Excellent.

---

# 2. Iceberg answers: "What files constitute my table?"

Imagine:

```text
transactions/
    file-001.parquet
    file-002.parquet
    file-003.parquet
    ...
```

Who tells you:

> These 3,000 files are the current version of `banking.transactions`?

That's where Iceberg comes in.

```text
                  Iceberg
                     |
          +----------+----------+
          |                     |
      Metadata               Snapshots
          |                     |
      Manifests             Table versions
          |
      Data files
          |
       Parquet
```

---

# 3. The difference becomes obvious with UPDATE

Suppose you have:

```text
transaction_id = 1001
amount = 100
```

You want:

```text
amount = 500
```

With DuckDB querying Parquet, you don't get Iceberg's table-management semantics simply by using DuckDB.

You need to manage the physical files yourself.

With an Iceberg table:

```sql
UPDATE transactions
SET amount = 500
WHERE transaction_id = 1001;
```

The Iceberg table layer manages the resulting table state, snapshots, metadata and affected files according to the engine/table configuration.

---

# 4. The biggest difference: snapshots

Suppose:

```text
Monday
100 million transactions
```

Then:

```text
Tuesday
ETL bug modifies 20 million records
```

With Iceberg:

```text
Snapshot 100
     |
     | good data
     v
Snapshot 101
     |
     | bad ETL
     v
Current
```

You can query an earlier snapshot.

With a plain collection of Parquet files:

```text
Parquet files
```

you don't automatically get this table-level version history.

---

# 5. Schema evolution

Suppose your original table is:

```text
transaction_id
account_id
amount
```

Six months later:

```text
transaction_id
account_id
amount
merchant_id
channel
```

Iceberg manages this as **table schema evolution**.

```text
Schema v1
   |
   v
Schema v2
   |
   v
Schema v3
```

DuckDB can certainly query changing Parquet schemas, but that is different from providing Iceberg's table-format semantics for managing a long-lived multi-writer table.

---

# 6. Partition evolution

This is another big one.

Today you have:

```text
transactions
    |
    +-- month=2026-01
    +-- month=2026-02
    +-- ...
```

Your bank grows dramatically.

You now want daily partitioning:

```text
day(transaction_date)
```

Iceberg allows the table's partition specification to evolve while retaining older data under the old specification.

Conceptually:

```text
Iceberg table

Old data
   ↓
Partition spec v1
month

New data
   ↓
Partition spec v2
day
```

DuckDB can query the files, but **Iceberg is the layer managing this evolving table definition**.

---

# 7. Concurrent writers

Imagine:

```text
                 transactions
                       |
              +--------+--------+
              |                 |
          Spark Job A       Flink Job B
              |                 |
              +--------+--------+
                       |
                  simultaneous
                     writes
```

Iceberg provides a transaction/commit model for managing concurrent table updates.

This is extremely important when you have:

```text
CDC
+
batch ETL
+
streaming
+
multiple analytical jobs
```

DuckDB is not intended to be your distributed multi-writer lakehouse table-management layer.

---

# 8. This is the critical distinction

Suppose you have:

```text
10 TB of Parquet
```

You can use DuckDB:

```text
DuckDB
   |
   +---- query Parquet
   +---- aggregate
   +---- join
   +---- filter
```

But if you need:

```text
10 TB
+
multiple writers
+
CDC
+
schema evolution
+
partition evolution
+
time travel
+
ACID table commits
+
snapshots
+
data lifecycle management
```

then:

```text
                 Iceberg
                    |
        +-----------+-----------+
        |           |           |
    snapshots   metadata    transactions
        |           |           |
        +-----------+-----------+
                    |
                 Parquet
                    |
              Object Storage
```

is much more appropriate.

---

# 9. But here's the important part

**You don't have to choose DuckDB OR Iceberg.**

You can use:

```text
             DuckDB
           Query Engine
               |
               v
            Iceberg
          Table Format
               |
               v
            Parquet
               |
               v
          S3 / MinIO
```

This is actually a very nice architecture.

For example:

```text
Python
  |
  v
DuckDB
  |
  v
Iceberg table
  |
  v
S3
```

DuckDB does the **computation**.

Iceberg manages the **table**.

Parquet stores the **data**.

---

# 10. Compare them directly

| Capability                                | DuckDB                              | Iceberg          |
| ----------------------------------------- | ----------------------------------- | ---------------- |
| SQL engine                                | ✅                                   | ❌                |
| Query Parquet                             | ✅                                   | ❌                |
| Aggregations                              | ✅                                   | ❌                |
| Joins                                     | ✅                                   | ❌                |
| Vectorized execution                      | ✅                                   | ❌                |
| Table format                              | ❌                                   | ✅                |
| Snapshots                                 | ❌ as its core table-format role     | ✅                |
| Time travel                               | Not equivalent to Iceberg snapshots | ✅                |
| Schema evolution                          | Query-side capabilities             | ✅ table-level    |
| Partition evolution                       | ❌                                   | ✅                |
| Manifest management                       | ❌                                   | ✅                |
| Table metadata                            | ❌ as a lakehouse table format       | ✅                |
| Distributed multi-writer table management | ❌                                   | ✅                |
| Parquet storage                           | Reads/writes it                     | Uses it commonly |
| Object storage lakehouse                  | Can query it                        | Designed for it  |

---

# 11. Your previous architecture makes more sense now

You previously had:

```text
Flight SQL
    |
    v
DuckDB
    |
    v
Parquet
```

That's fine for a relatively simple analytical environment.

But a larger banking lakehouse could become:

```text
                  Banking Systems
                       |
                    Debezium
                       |
                     Kafka
                       |
                Spark / Flink
                       |
                       v
                  ┌─────────┐
                  │ Iceberg │
                  └────┬────┘
                       |
                    Parquet
                       |
                  S3 / MinIO
                       |
          +------------+------------+
          |            |            |
       DuckDB        Trino        Spark
          |            |            |
          +------------+------------+
                       |
                       v
                    BI / ML
```

Now each component has a clear responsibility.

---

# 12. An analogy

Think about a library.

### Parquet

is like the **books**.

### Iceberg

is like the **library catalog + version/history system**.

It knows:

```text
Which books belong to which collection?
Which version is current?
Where is each book?
What changed?
```

### DuckDB

is like the **librarian doing the analysis**:

> Find all books matching these criteria and calculate something from them.

You wouldn't say:

> "Why do I need a library catalog? The librarian can search books."

For a tiny library, perhaps.

For:

```text
10 million books
multiple librarians
constant additions/removals
historical versions
multiple branches
```

you need both.

---

# 13. When I would use DuckDB alone

For example:

```text
100 GB
+
mostly read-only
+
single/few users
+
local analytics
+
Parquet files
```

I'd happily do:

```text
DuckDB
   ↓
Parquet
```

You don't necessarily need Iceberg.

This is especially good for:

* local analytics
* notebooks
* data exploration
* ETL transformations
* small/medium analytical workloads
* ad-hoc queries

---

# 14. When I would introduce Iceberg

If your environment becomes:

```text
10 TB → 100 TB → PB
```

and you have:

```text
multiple writers
CDC
streaming
batch
schema changes
time travel
data governance
long-lived tables
```

then I'd consider:

```text
Iceberg + Parquet
```

and then choose query engines such as:

```text
Spark
Flink
Trino
DuckDB
```

according to the workload.

---

# 15. The most important takeaway

Don't think:

```text
DuckDB vs Iceberg
```

Think:

```text
              Query Layer
         ┌──────┼───────┐
         │      │       │
      DuckDB  Trino   Spark
         │      │       │
         └──────┼───────┘
                ↓
        ┌───────────────┐
        │   Iceberg     │
        │  Table Layer  │
        └───────┬───────┘
                ↓
             Parquet
                ↓
          S3 / MinIO
```

**DuckDB and Iceberg are complementary.**

For the kind of banking lakehouse you're exploring, I would **not replace Iceberg with DuckDB**. I'd use **Iceberg to manage the long-lived banking tables and DuckDB as a lightweight, very fast SQL engine to query them**, while Spark/Flink handle large-scale batch/streaming writes.
