# Lesson 7 — Capstone Walkthrough
## The FinBank Fraud Analytics Platform

> **Mission:** combine every lesson into one working system. You will generate synthetic card transactions, land them as columnar Parquet (L1) via Arrow (L2), register them as an **Apache Iceberg** table with ACID + time travel (L4), analyze them with **DuckDB** (L3), serve results to "branches" through your own **Flight SQL** server (L5), and know exactly which steps scale out with **Spark** (L6).

---

## 0. Architecture Review

```
 pandas generator ──► Arrow Table ──► Parquet lake/            (L1, L2)
                                        │
                        PyIceberg register + append   (L4)
                                        │
              ┌─────────────────────────┼──────────────────────┐
              ▼                         ▼                      ▼
        DuckDB analyst             Spark batch job         Flight SQL server
        workbench (local)          (cluster-scale ETL)     ─► branches/BI/JDBC
              │                         │                      ▲        (L5)
              └────────── alerts ───────┘──────────────────────┘
```

**Setup**

```bash
pip install -r requirements.txt
```

**Run order** (each step is standalone; run them in sequence from the project root):

```bash
python capstone_step1_land.py        # ~30-60 s: 2M rows -> zstd Parquet (~35 MB)
python capstone_step2_iceberg.py     # ~60-90 s: Iceberg table + atomic append
python capstone_step3_duckdb.py      # seconds: SQL over the Iceberg table
# start server in one terminal (L5 code):
python bank_flight_server.py &
# branch client in another:
python capstone_step4_branch_client.py
```

Expected results: Step 1 prints a size well below an equivalent CSV (~10x smaller); Step 2 prints a snapshot id and exact row count; Step 3 shows top-10 alert accounts; the branch client prints typed rows with zero parsing.

---

## 1. Step 1 — Generate & Land Data (L1 + L2)

```python
"""capstone_step1_land.py"""
import numpy as np, pandas as pd, os
import pyarrow as pa, pyarrow.parquet as pq

rng = np.random.default_rng(2026)
N = 2_000_000

df = pd.DataFrame({
    "txn_id":  np.arange(1, N + 1, dtype="int64"),
    "account": rng.choice([f"A{i:06d}" for i in range(60_000)], N),
    "amount":  np.round(rng.lognormal(5.2, 1.3, N), 2),
    "mcc":     rng.choice(["5411", "6011", "5812", "5999"], N),
    "channel": rng.choice(["POS", "ECOM", "ATM", "UPI"], N),
    "txn_ts":  pd.to_datetime("2026-07-01")
               + pd.to_timedelta(rng.integers(0, 30 * 86400, N), unit="s"),
})

# Arrow table (in-memory standard, L2) → optimized Parquet (L1)
table = pa.Table.from_pandas(df, preserve_index=False)

os.makedirs("capstone_lake/txns", exist_ok=True)
pq.write_table(
    table, "capstone_lake/txns/data.parquet",
    compression="zstd", use_dictionary=["account", "mcc", "channel"],
    row_group_size=500_000,
)
print("landed:", table.num_rows, "rows →",
      os.path.getsize("capstone_lake/txns/data.parquet") / 1e6, "MB zstd parquet")
```

> For a real date-partitioned layout use `pq.write_to_dataset(table.add_column(...day...), partition_cols=["day"])` exactly as in Lesson 1 — kept simple here so the capstone runs anywhere.

## 2. Step 2 — Register an Iceberg Table & Commit (L4)

```python
"""capstone_step2_iceberg.py"""
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, LongType, StringType,
                             DoubleType, TimestamptzType)
import pyarrow as pa, pyarrow.parquet as pq, datetime as dt

catalog = load_catalog("finbank_capstone", **{
    "type": "sql",
    "uri": "sqlite:///capstone_iceberg.db",
    "warehouse": "file:///tmp/capstone_warehouse",
})
catalog.create_namespace_if_not_exists("banking")

schema = Schema(
    NestedField(1, "txn_id",  LongType(),        required=True),
    NestedField(2, "account", StringType(),      required=True),
    NestedField(3, "amount",  DoubleType(),      required=False),
    NestedField(4, "mcc",     StringType(),      required=False),
    NestedField(5, "channel", StringType(),      required=False),
    NestedField(6, "txn_ts",  TimestamptzType(), required=True),
)

try:
    tbl = catalog.load_table("banking.txns")
except Exception:
    tbl = catalog.create_table(
        "banking.txns", schema=schema,
        properties={"write.parquet.compression-codec": "zstd"})

data = pq.read_table(
    "capstone_lake/txns/data.parquet",
    columns=["txn_id", "account", "amount", "mcc", "channel", "txn_ts"],
).cast(tbl.schema().as_arrow())

if tbl.scan().to_arrow().num_rows == 0:
    tbl.append(data)                       # atomic commit #1

print("snapshot:", tbl.current_snapshot().snapshot_id,
      "| rows:", tbl.scan().to_arrow().num_rows)
```

## 3. Step 3 — Analyst Layer with DuckDB (L3)

```python
"""capstone_step3_duckdb.py"""
import duckdb

con = duckdb.connect("capstone.duckdb")

# Iceberg table readable directly by DuckDB (same bytes, L4 guarantee)
con.execute("INSTALL iceberg; LOAD iceberg;")

alerts = con.sql("""
    SELECT account,
           count(*)                        AS n_txns_30d,
           round(sum(amount), 2)           AS vol_30d,
           count(*) FILTER (WHERE channel = 'ECOM') AS n_ecom
    FROM iceberg_scan('file:///tmp/capstone_warehouse/banking/txns')
    GROUP BY account
    HAVING count(*) >= 50 AND sum(amount) > 250000
    ORDER BY vol_30d DESC LIMIT 10
""")
alerts.show()
alerts.df().to_parquet("capstone_lake/alerts.parquet")
con.close()
```

## 4. Step 4 — Serve Alerts via Your Flight SQL Server (L5)

Reuse the server from Lesson 5 verbatim (`bank_flight_server.py`) — point it at the capstone database:

```python
DB_PATH = "capstone.duckdb"      # one-line change in bank_flight_server.py
PORT = 8815                      # unchanged
```

Then, from a "branch" machine:

```python
"""capstone_step4_branch_client.py"""
from clients import FinBankClient          # Lesson 5's client class

c = FinBankClient(b"branch-mumbai-token")
tbl = c.sql("""
    SELECT account, vol_30d, n_ecom FROM alerts
    WHERE vol_30d > 300000 ORDER BY vol_30d DESC
""")
print(tbl.to_pandas())                     # typed Arrow → pandas, zero parsing
```

**Scale-out checkpoint (L6):** when N outgrows one machine, Steps 1–3 collapse into the Spark job from Lesson 6 reading/writing the *same* Iceberg table — no consumer changes required. That is the payoff of standardizing on columnar + table format + wire protocol.

## 5. Acceptance Checklist

| ✔ | Requirement | Proof |
|---|---|---|
| ☐ | Data lands columnar & compressed | file size < raw CSV equivalent; zstd parquet (Step 1) |
| ☐ | Commits are atomic | append twice; snapshot ids advance, row counts exact (Step 2) |
| ☐ | History is auditable | `tbl.scan(snapshot_id=...)` returns pre-append world (L4 code) |
| ☐ | Analysts query without ingestion | DuckDB views/scans hit files directly (Step 3) |
| ☐ | Consumers never touch the lake | branches pull via authenticated Flight SQL (Step 4) |
| ☐ | Design survives scale-out | same tables read from Spark (L6 job) |

## 6. Stretch Goals

1. **CDC simulation:** append "late" txns with corrected amounts via PyIceberg `upsert`, then prove time travel shows both worlds.
2. **Row-level security:** extend the Flight server so `branch_mumbai` may only `SELECT` rows for its region.
3. **Streaming ingest:** replace batch generation with a micro-batch loop appending every minute; watch small-file problems appear, then fix with compaction.
4. **Cost model:** estimate monthly S3 spend before/after ZSTD + partition pruning using your measured sizes.

---

🎓 **Course complete!** You have built every layer of a modern analytical stack with your own hands. Return to `00_Course_Overview.md`, or start over and do it faster this time — that's what mastery feels like.
