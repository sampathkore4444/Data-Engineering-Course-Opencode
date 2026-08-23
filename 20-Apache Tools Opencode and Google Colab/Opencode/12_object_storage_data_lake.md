# Lesson 12 — Object Storage: Where the Lake Actually Lives

> **Meridian Trust Bank case study, Part 12**: The capstone lakehouse you built in Lesson 11
> ran on local disk. Monday the platform team pulls the plug: all analytical data moves to the
> bank's **S3-compatible object store** (like AWS S3, MinIO, or Ceph). Suddenly senior engineers
> discover the lake has no folders, no renames, and no `open("file").append()` - and the ETL
> jobs that "just moved files around" start failing. This lesson rebuilds your intuition from
> filesystem to keyspace.

---

## Table of Contents

| Section | Topic |
|---|---|
| [1](#1-concept-a-keyspace-not-a-filesystem) | Concept: a keyspace, not a filesystem |
| [2](#2-banking-scenario-the-400-tb-migration) | Banking scenario: the 400 TB migration |
| [3](#3-end-to-end-example) | End-to-end example |
| [4](#4-what-just-happened-layer-by-layer) | What just happened, layer by layer |
| [5](#5-production-notes-you-will-get-asked-in-interviews) | Production notes |
| [6](#6-exercises) | Exercises |
| [7](#7-cheat-sheet) | Cheat sheet |

---

## 1. Concept: a keyspace, not a filesystem

Object storage exposes exactly three operations per object - PUT, GET, DELETE - plus LIST,
which is not a directory walk but a **lexicographic prefix scan** returning up to 1,000 keys
per page:

```
   FILESYSTEM (POSIX)                    OBJECT STORE (S3 API)
   /lake/txns/2026/07/part.parquet       s3://lake/txns/2026/07/part.parquet
        │                                          │
   tree of inodes                       FLAT map:  key -> bytes + metadata
        │                                          │
   rename()  = O(1) metadata flip       "rename"   = CopyObject + DeleteObject
   append()  = yes                      append     = NO (objects are immutable)
   list dir  = readdir                  list       = prefix scan, paginated
   partial overwrite = yes              overwrite  = whole new object PUT
```

| Property | Filesystem | Object storage |
|---|---|---|
| Namespace | hierarchical directories | flat string keys (prefixes *look* like dirs) |
| Mutability | append/seek/truncate | immutable; change = new version |
| Atomic rename | yes | no - two operations, non-atomic |
| Consistency | immediate | strongly consistent (modern S3) but *list-after-write* still worth testing |
| Cost model | buy capacity | pay per GB-month, per REQUEST, per egress GB |

That last row changes engineering economics: reading one 128 MB Parquet row group costs one
GET plus range requests; listing a 10-million-key prefix costs 10,000 LIST pages. Design for
*few, fat objects* and *metadata-driven planning*.

### Why this kills naive "Hive on S3"

The classic Hive table committed by **renaming** a `_temporary` directory into place. Rename
is atomic on HDFS, so the trick worked there. On S3 there is no rename - so a crash mid-"move"
leaves readers seeing half a table. **Table formats (Iceberg) were invented for exactly this**:
a commit becomes a single atomic PUT of a new metadata JSON file, and old data files are never
rewritten (Lessons 06/07). Your lakehouse stack was object-storage-native all along.

## 2. Banking scenario: the 400 TB migration

Meridian's card-ledger archive - 60 columns, 400 TB - leaves NFS for S3:

1. **Column pruning pays twice.** Fraud scans touch 5 of 60 columns. On disk that saved I/O;
   on S3 it saves *money*: Parquet's footer tells the planner which byte ranges hold those
   columns, and only those ranges are fetched via HTTP range GETs.
2. **Planning without data.** Row counts, schema, min/max stats all live in the footer -
   typically <1% of file size. Query engines plan entire scans before downloading payload.
3. **Same bucket, many engines.** PyArrow writes, DuckDB queries over `httpfs`, Spark reads
   the same keys - the files are the contract (next lesson scales this up).

We prove all three locally with **moto**, a real implementation of the S3 API that runs
in-process - no Docker, no cloud account, no credentials.

## 3. End-to-end example

```python
"""
lesson12_object_storage_lab.py
Real S3-API semantics locally (moto): flat keys, no renames, footer-only reads,
partitioned Parquet on object storage, DuckDB httpfs querying the same bucket.
Deps: pip install pyarrow pandas numpy duckdb boto3 "moto[s3]"
"""
import logging
import os
import threading
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

# ---- 0. boot a REAL S3 API endpoint on localhost (moto = AWS API in-process) ----
os.environ.update(AWS_ACCESS_KEY_ID="testing", AWS_SECRET_ACCESS_KEY="testing",
                  AWS_DEFAULT_REGION="us-east-1")
from moto.server import ThreadedMotoServer

srv = ThreadedMotoServer(ip_address="127.0.0.1", port=4566)
srv.start()
time.sleep(0.5)

import boto3

s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:4566")
BUCKET = "meridian-lake"
s3.create_bucket(Bucket=BUCKET)
print(f"bucket created: s3://{BUCKET}/")

# ---- 1. write HIVE-PARTITIONED parquet straight into the bucket ------------------
rng = np.random.default_rng(2)
N = 300_000
df = pd.DataFrame({
    "txn_id":     np.arange(N),
    "card_id":    rng.integers(300_000, 310_000, N),
    "ts":         pd.date_range("2026-07-01", periods=N, freq="20s"),
    "amount":     np.round(rng.gamma(2, 40, N) + .5, 2),
    "channel":    rng.choice(["POS", "ECOM", "ATM"], N),
})
df["month"] = df.ts.dt.month

fs = pafs.S3FileSystem(endpoint_override="127.0.0.1:4566", scheme="http",
                       access_key="testing", secret_key="testing",
                       region="us-east-1")
table = pa.Table.from_pandas(df, preserve_index=False)   # keep 'month' IN:
pq.write_to_dataset(table, f"{BUCKET}/card_txns", partition_cols=["month"],
                    compression="zstd", filesystem=fs)   # writer moves it to keys

# ---- 2. reality check #1: 'directories' are just key PREFIXES --------------------
logging.getLogger("werkzeug").setLevel(logging.ERROR)

resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="card_txns/")
keys = [o["Key"] for o in resp["Contents"]]
parquets = [k for k in keys if k.endswith(".parquet")]
print(f"\n{len(parquets)} parquet objects ({len(keys)} keys total); the lake has NO folders, only flat string keys:")
for k in keys[:5]:
    print("   ", k)

# ---- 3. reality check #2: rename does not exist -> copy + delete -----------------
src = parquets[0]
dst = src.replace("card_txns/", "card_txns_stage/")
t0 = time.perf_counter()
s3.copy_object(Bucket=BUCKET, Key=dst, CopySource={"Bucket": BUCKET, "Key": src})
s3.delete_object(Bucket=BUCKET, Key=src)
print(f"\n'move' {src} -> {dst}: COPY+DELETE in "
      f"{(time.perf_counter()-t0)*1e3:.1f} ms (two full-object operations!)")
# put it back for the rest of the lab
s3.copy_object(Bucket=BUCKET, Key=src, CopySource={"Bucket": BUCKET, "Key": dst})
s3.delete_object(Bucket=BUCKET, Key=dst)

# ---- 4. footer-only planning: answer questions WITHOUT reading data ---------------
key = parquets[0]
target = f"{BUCKET}/{key}"   # pyarrow wants bucket-prefixed keys
head = s3.head_object(Bucket=BUCKET, Key=key)
size = head["ContentLength"]

f = fs.open_input_file(target)                   # random-access: range GETs
with f:
    f.seek(size - 8)                             # last 8 bytes: len | 'PAR1'
    magic_len = f.read(8)
    footer_len = int.from_bytes(magic_len[:4], "little")
    print(f"\n{target}\n  size={size:,}B | magic={magic_len[4:].decode()!r} "
          f"| footer={footer_len:,}B ({footer_len/size:.1%} of file)")
    f.seek(size - 8 - footer_len)
    _ = f.read(footer_len)                       # <- the ONLY bytes a planner needs

meta = pq.ParquetFile(fs.open_input_file(target)).metadata
print(f"  planned from footer alone: rows={meta.num_rows:,} "
      f"row_groups={meta.num_row_groups}")

# ---- 5. DuckDB queries the SAME bucket over httpfs --------------------------------
import duckdb

con = duckdb.connect()
con.execute("""
    SET s3_endpoint='127.0.0.1:4566'; SET s3_url_style='path';
    SET s3_use_ssl=false;             SET s3_region='us-east-1';
    SET s3_access_key_id='testing';   SET s3_secret_access_key='testing';
""")
out = con.sql("""
    SELECT channel, count(*) n, round(sum(amount), 2) vol
    FROM read_parquet('s3://meridian-lake/card_txns/**/*.parquet',
                      hive_partitioning = true)
    WHERE month = 7 AND amount > 400
    GROUP BY channel ORDER BY vol DESC
""").df()
print("\nDuckDB over the S3 API (no download step):")
print(out.to_string(index=False))
srv.stop()
```

Sample output (abridged):

```
bucket created: s3://meridian-lake/

3 parquet objects (7 keys total); the lake has NO folders, only flat string keys:
    card_txns/
    card_txns/month=7/
    card_txns/month=7/650f1bfe...parquet
    card_txns/month=8/
    card_txns/month=8/650f1bfe...parquet

'move' card_txns/month=7/650f...parquet -> card_txns_stage/month=7/650f...parquet:
COPY+DELETE in 19.4 ms (two full-object operations!)

meridian-lake/card_txns/month=7/650f...parquet
  size=2,035,744B | magic='PAR1' | footer=5,457B (0.3% of file)
  planned from footer alone: rows=133,920 row_groups=5

DuckDB over the S3 API (no download step):
channel     n      vol
    POS    25 11401.28
    ATM    24 10961.41
   ECOM    22 10195.14
```

## 4. What just happened, layer by layer

- **Step 1**: `pq.write_to_dataset(..., partition_cols=["month"])` produced
  `card_txns/month=7/....parquet` keys. The `month` column was removed from the file payloads
  and encoded in the KEY - Hive-style partitioning, now living on flat strings.
- **Step 2**: LIST returned "directories" (`card_txns/`, `card_txns/month=7/`) as zero-byte
  placeholder keys. Tools *render* them as folders; the API sees only sorted prefixes.
- **Step 3**: the "move" copied 2 MB and deleted the original. Scale that to a 50 GB file and
  the anti-pattern is obvious: never stage tables by moving objects - commit metadata instead
  (Iceberg), or write directly to the final key.
- **Step 4**: we hand-parsed the Parquet footer off the wire: seek to `size-8`, read the
  footer length, fetch those bytes only. `0.3% of the file` answered *rows* and *row groups* -
  that is what every query engine's planner does before touching data.
- **Step 5**: DuckDB's `httpfs` extension spoke S3 against the same bucket: predicate
  pushdown (`amount > 400`), partition pruning (`month = 7`), and range GETs for the single
  `channel`/`amount` column chunks. Nothing was ever downloaded wholesale.

## 5. Production notes you will get asked in interviews

- **Small files hurt twice on S3**: per-object request fees and slow LIST planning. Bin-pack
  to 64-512 MB objects (Lesson 10 compaction cadence applies verbatim).
- **Never mix lifecycle deletion with live tables**: an S3 lifecycle rule that expires objects
  after N days will silently corrupt Iceberg tables whose snapshots reference those files.
  Retire data through `expire_snapshots`, then let lifecycle clean the orphans.
- **Checksums are your audit trail**: enable SHA-256/xxHash checksums; combined with Iceberg
  manifests you can prove to a regulator that file content is unchanged since commit.
- **Storage tiers ≠ free archive**: retrieving quarter-end ledgers from Glacier-class storage
  costs retrieval fees and hours of latency. Keep audit-pinned snapshots (Lesson 07 tags!) in
  hot storage; tier only expired snapshots.
- **List-before-read races**: engines plan from metadata (Iceberg manifest lists), not from
  LIST - one more reason "just files + a catalog" beats raw prefix conventions at scale.

## 6. Exercises

1. Add `day` partitioning on top of `month` and rewrite the LIST check to count keys per
   prefix; explain how partition granularity trades LIST cost against prune power.
2. Break the lab on purpose: delete the `month=8` object while DuckDB plans a query covering
   months 7-9, and observe which error surfaces first - planner or reader. Relate to why
   Iceberg validates manifests at commit time.
3. Measure footer size vs file size while varying `row_group_size`; find the point where
   planning overhead stops being negligible.
4. Implement a "fake Iceberg commit": write table state JSON to `table/meta/v{n}.json` and
   publish by copying it to `table/meta/current.json`. Crash between copies - what does a
   reader see? Compare with Iceberg's atomic pointer swap.
5. Point `pafs.S3FileSystem` at a real MinIO container (`docker run minio/minio`) instead of
   moto and re-run unchanged - that portability *is* the S3 contract.

---

## 7. Interview questions: object storage and data lakes

### Concept 1: Object storage fundamentals

**Q1: What's the difference between object storage and file storage?**

A: File storage (NFS, local disk): hierarchical directories, `rename()`, `append()`, `readdir()`. Object storage (S3): flat keyspace, PUT/GET/DELETE only, no rename, no append. Object storage is: (1) scalable (petabytes), (2) durable (11 9s), (3) cheap ($0.02/GB/month), (4) REST API only. The trade-off: no file system semantics, but massive scale.

**Q2: Why can't you rename objects in S3?**

A: S3 is a flat keyspace — keys are immutable identifiers. "Rename" is actually: (1) PUT new object, (2) DELETE old object. This is expensive (2 operations) and non-atomic (crash between puts = duplicate). The key: object storage is append-only by design. Use Iceberg's metadata layer for atomic operations.

**Q3: How does S3 achieve 11 9s durability?**

A: S3 stores each object across 3 Availability Zones (AZs). Each AZ has multiple disks. The math: 1 - (failure probability)^3 = 1 - (10^-11)^3 ≈ 11 9s. The key: redundancy, not backup. S3 is the storage layer — Iceberg provides the metadata layer for consistency.

**Q4: What's the S3 consistency model?**

A: S3 is strongly consistent for PUT-after-PUT and GET-after-PUT (since 2020). This means: if you PUT an object, subsequent GETs see the new version. This is critical for Iceberg: atomic catalog swaps rely on consistent reads. Before 2020, S3 was eventually consistent — Iceberg had to handle this.

**Q5: How do you estimate S3 storage costs for a 100 TB data lake?**

A: S3 Standard: $0.023/GB/month × 100,000 GB = $2,300/month. S3 Intelligent-Tiering: auto-moves data between tiers, saves 30-40%. S3 Glacier: $0.004/GB/month for archival. Plus: PUT/GET requests ($0.005/1000), data transfer ($0.09/GB out). Total: ~$2,500-$3,000/month for 100 TB.

### Concept 2: DuckDB and S3

**Q1: How does DuckDB query Parquet files on S3 without downloading them?**

A: DuckDB uses the `httpfs` extension: `INSTALL httpfs; LOAD httpfs; SELECT count(*) FROM 's3://bucket/txns/**/*.parquet'`. DuckDB fetches only the Parquet pages it needs (HTTP Range requests). No full download — just metadata (footer) and needed data pages. The key: DuckDB does random access via HTTP, not sequential download.

**Q2: What's the performance difference between local Parquet and S3 Parquet?**

A: Local: ~0.1s per file (SSD I/O). S3: ~0.5-2s per file (network latency + S3 request overhead). For 1000 files: local = 100s, S3 = 500-2000s. The key: S3 is slower per file, but parallelism helps (DuckDB reads files concurrently). For filtered queries: S3 is fine (skip most files). For full scans: local is faster.

**Q3: How does DuckDB handle S3 listing (thousands of files)?**

A: DuckDB uses S3 LIST API (prefix scan). For 10,000 files: LIST returns 10,000 keys (paginated, 1000 per page = 10 requests). The key: LIST is expensive on S3 (request fees, latency). Iceberg solves this: catalog stores file list, no LIST needed. DuckDB reads Iceberg metadata, not S3 LIST.

**Q4: How do you optimize DuckDB queries over S3?**

A: (1) Partition pruning: filter on partition columns (skip directories), (2) Predicate pushdown: filter on data columns (skip row groups), (3) Parallelism: read files concurrently, (4) Caching: DuckDB caches Parquet metadata, (5) File size: larger files = fewer requests. The key: minimize S3 requests.

**Q5: How do you handle S3 throttling (503 Slow Down)?**

A: S3 throttles at 5,500 GET requests per second per prefix. Solutions: (1) Spread files across prefixes (hash-based partitioning), (2) Use larger files (fewer requests), (3) Implement retry with exponential backoff, (4) Use S3 Transfer Acceleration. The key: design your key structure to avoid hot prefixes.

### Concept 3: Data lake design

**Q1: How do you organize a data lake on S3?**

A: Pattern: `s3://lake/{database}/{table}/{partition_column}={value}/data.parquet`. Example: `s3://meridian/bank/card_txns/date=2026-07-15/0001.parquet`. Key design: (1) Table-level prefix (isolation), (2) Partition columns (pruning), (3) Consistent naming (predictable paths). The key: organize by access pattern, not by source system.

**Q2: What's the small-file problem on S3, and how do you solve it?**

A: Small files (< 1MB) cause: (1) S3 LIST overhead (thousands of keys), (2) Per-request fees ($0.005/1000), (3) DuckDB planning overhead (many metadata reads). Solution: Iceberg compaction (merge small files into 128-512MB files). The key: object storage is optimized for large objects, not millions of tiny ones.

**Q3: How do you handle schema changes in a data lake?**

A: Iceberg schema evolution: add/rename/drop columns without rewriting data. Field IDs make this safe across engines. Old readers see NULLs for new columns. New readers see all columns. The key: Iceberg's metadata layer handles schema changes — S3 doesn't care about schemas.

**Q4: How do you handle data retention on S3?**

A: (1) Iceberg `expire_snapshots`: remove metadata references to old data files, (2) S3 Lifecycle rules: auto-delete objects after N days (only for orphaned files), (3) Tags: exempt audit-pinned snapshots from expiry. The key: never use S3 Lifecycle on live Iceberg tables — it corrupts metadata.

**Q5: How do you handle cross-region replication for DR?**

A: (1) S3 Cross-Region Replication (CRR): auto-replicate objects to another region, (2) Iceberg catalog replication: sync catalog metadata to DR region, (3) DNS failover: point gateway to DR region. The key: S3 CRR handles data replication; Iceberg metadata is small (replicate separately).

### Concept 4: Production patterns

**Q1: How do you monitor a data lake on S3?**

A: Track: (1) Object count and size per table, (2) File size distribution (detect small files), (3) S3 request rates (detect throttling), (4) Query latency (DuckDB over S3). Alert on: small files > threshold, request rate > 80% of limit, query latency > 2× baseline. The key: S3 metrics + Iceberg metadata.

**Q2: How do you handle S3 costs in production?**

A: (1) S3 Intelligent-Tiering (auto-optimize storage class), (2) Lifecycle rules (delete orphans, archive expired snapshots), (3) Large files (reduce request fees), (4) Compression (Parquet + ZSTD reduces storage), (5) Partitioning (skip irrelevant data). The key: storage is cheap, but requests and transfer add up.

**Q3: How do you handle S3 security?**

A: (1) IAM roles (least privilege), (2) Bucket policies (restrict access), (3) VPC endpoints (no public internet), (4) Encryption at rest (SSE-S3 or SSE-KMS), (5) Encryption in transit (TLS), (6) Access logging (CloudTrail). The key: S3 security is IAM + bucket policies + encryption.

**Q4: How do you handle S3 performance?**

A: (1) Parallel requests (DuckDB reads files concurrently), (2) Larger files (fewer requests), (3) Prefix distribution (avoid hot prefixes), (4) S3 Transfer Acceleration (cross-region), (5) S3 Select (filter at S3, not client). The key: S3 is optimized for throughput, not latency. Design for parallelism.

**Q5: How do you handle S3 failures?**

A: (1) Retry with exponential backoff (transient failures), (2) Checksums (detect corruption), (3) Multi-part upload (resume large uploads), (4) S3 Versioning (recover from deletes), (5) Cross-region replication (DR). The key: S3 is highly available (11 9s durability), but transient failures happen — retry logic is essential.

### Concept 5: Interview preparation

**Q1: How do you explain object storage to a database engineer?**

A: "Object storage is like a key-value store for files. You PUT a blob with a key, GET it back by key, DELETE it. No directories, no rename, no append. It's like a distributed hash table for files — scalable, durable, cheap. The trade-off: no file system semantics, but massive scale."

**Q2: What's the most common mistake when designing a data lake on S3?**

A: (1) Too many small files (creates LIST overhead, request fees), (2) Wrong partitioning (too many partitions = too many directories), (3) No compaction (small files accumulate), (4) Using S3 Lifecycle on live tables (corrupts Iceberg metadata). The key: design for large files and few partitions.

**Q3: How do you justify moving from on-prem HDFS to S3?**

A: (1) Cost: S3 is 10× cheaper than HDFS (no cluster to manage), (2) Durability: S3 has 11 9s (HDFS has 3 replicas), (3) Scalability: S3 is unlimited (HDFS requires capacity planning), (4) Serverless: no ops overhead (HDFS requires admin). The key: S3 is storage-as-a-service — pay for what you use.

**Q4: How do you handle a 400 TB migration from HDFS to S3?**

A: (1) AWS DataSync or S3 Transfer Acceleration (bulk transfer), (2) Iceberg `add_files` (register existing files without copy), (3) Validate: query both, compare results, (4) Cutover: switch readers to S3, (5) Decommission: shut down HDFS. The key: `add_files` avoids rewriting petabytes.

**Q5: What's the future of object storage in data engineering?**

A: (1) S3-compatible storage everywhere (MinIO, Ceph, GCS), (2) Table formats (Iceberg) over object storage, (3) Serverless query (DuckDB, Trino) over S3, (4) Multi-cloud (same data, different clouds). The key: object storage is the foundation — table formats and query engines are the layers on top.

---

## 8. Cheat sheet

| Task | Tool |
|---|---|
| Local S3 API | `moto.server.ThreadedMotoServer(port)` - real AWS semantics in-process |
| List under prefix | `s3.list_objects_v2(Bucket, Prefix)` - paginate with `ContinuationToken` |
| "Rename" | `copy_object` + `delete_object` - never atomic, avoid for commits |
| Object FS handle | `pafs.S3FileSystem(endpoint_override, scheme, access_key, ...)` |
| Footer-only plan | `pq.ParquetFile(fs.open_input_file(key)).metadata` - range GETs only |
| Dataset write to S3 | `pq.write_to_dataset(t, "bucket/path", partition_cols=[...], filesystem=fs)` |
| DuckDB over S3 | `INSTALL httpfs; SET s3_endpoint/url_style/use_ssl/keys...;` then plain `read_parquet('s3://...')` |
| Head metadata | `s3.head_object` - size/etag/checksum without body transfer |

**Next:** Lesson 13 - the lake now lives on object storage, but your laptop's RAM is still the
compute ceiling. Enter Apache Spark: same files, fleet-grade engine.
