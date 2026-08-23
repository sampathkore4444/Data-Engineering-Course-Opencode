# Lesson 12 — Object Storage: Where the Lake Actually Lives

> **Meridian Trust Bank case study, Part 12**: The capstone lakehouse you built in Lesson 11
> ran on local disk. Monday the platform team pulls the plug: all analytical data moves to the
> bank's **S3-compatible object store** (like AWS S3, MinIO, or Ceph). Suddenly senior engineers
> discover the lake has no folders, no renames, and no `open("file").append()` - and the ETL
> jobs that "just moved files around" start failing. This lesson rebuilds your intuition from
> filesystem to keyspace.

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

## 7. Cheat sheet

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
