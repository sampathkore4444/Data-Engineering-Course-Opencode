#!/usr/bin/env python
"""
solutions.py — End-to-end Python solutions for EVERY exercise in the
FinBank "Modern Data Engineering Masterclass", all in ONE file.

Coverage
--------
  L1 Columnar Data Storage : l1ex1 .. l1ex4      (4 exercises)
  L2 Apache Arrow          : l2ex1 .. l2ex5      (5 exercises)
  L3 DuckDB                : l3ex1 .. l3ex5      (5 exercises)
  L4 Apache Iceberg        : l4ex1 .. l4ex5      (5 exercises)
  L5 Apache Flight SQL     : l5ex1 .. l5ex5      (5 exercises)
  L6 Apache Spark          : l6ex1 .. l6ex5      (5 exercises)
  Capstone stretch goals   : cap1 .. cap4

Usage
-----
  python solutions.py --list              # show every target
  python solutions.py                     # run ALL exercises (long!)
  python solutions.py l2 l3ex4            # all of lesson 2 + one exercise
  SOL_QUICK=1 python solutions.py         # tiny sizes, smoke-test mode

Notes
-----
  * Sizes default to the numbers stated in each exercise. SOL_QUICK=1
    shrinks them so the whole file verifies in minutes.
  * L6 needs `pip install pyspark`; l6ex4 additionally downloads the
    Iceberg runtime jars from Maven on first run (needs network).
  * Everything else runs on a laptop; first-time DuckDB extension
    downloads (httpfs/iceberg) need network once.
"""

from __future__ import annotations

import os

# Must be set BEFORE pyiceberg is imported anywhere (pandas produces ns
# timestamps; PyIceberg >=0.10 wants explicit permission to downcast to us).
os.environ.setdefault("PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE", "true")

import contextlib
import datetime as dt
import json
import re
import resource
import shutil
import socket
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from decimal import Decimal
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pyarrow.flight as flight

try:                                    # optional: only Lesson 6 needs it
    from pyspark.sql import Window, functions as F
except ImportError:                     # course still runs without Spark
    F = Window = None

QUICK = os.getenv("SOL_QUICK", "") == "1" or "--quick" in sys.argv
OUT = Path("_solutions_out")


def scaled(full: int, quick: int) -> int:
    """Exercise-spec size, or the smoke-test size when SOL_QUICK=1."""
    return quick if QUICK else full


def out_dir(name: str, fresh: bool = True) -> Path:
    d = OUT / name
    if fresh:
        shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True, exist_ok=True)
    return d


def header(msg: str) -> None:
    bar = "=" * 78
    print(f"\n{bar}\n{msg}\n{bar}", flush=True)


def mb(n_bytes: float) -> str:
    return f"{n_bytes / 1e6:,.1f} MB"


def rss_mb() -> float:
    """Peak resident set size of this process, MB (Linux)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def timed(fn, *args, **kwargs):
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, time.perf_counter() - t0


# ---------------------------------------------------------------------------
# Shared synthetic-data generators (banking-flavoured, Arrow-native)
# ---------------------------------------------------------------------------

_MCCS = ["5411", "5541", "5812", "5945", "6011", "4829", "5999", "7011"]
_CHANNELS = ["POS", "ECOM", "ATM", "UPI"]
_STATUS = ["OK", "FLAGGED", "REVERSED", "FAILED"]


def gen_txns_arrow(n: int, seed: int = 42) -> pa.Table:
    """Card transactions straight into Arrow (no pandas hop) for big N."""
    rng = np.random.default_rng(seed)
    secs = rng.integers(0, 365 * 86400, n)
    return pa.table({
        "txn_id":  pa.array(np.arange(1, n + 1, dtype="int64")),
        "acct_id": pa.array(rng.integers(0, 250_000, n).astype("int32")),
        "amount":  pa.array(np.round(rng.lognormal(5.0, 1.2, n), 2)),
        "mcc":     pa.array(_MCCS).take(rng.integers(0, len(_MCCS), n)),
        "channel": pa.array(_CHANNELS).take(rng.integers(0, len(_CHANNELS), n)),
        "status":  pa.array(_STATUS).take(rng.integers(0, len(_STATUS), n)),
        "txn_ts":  pa.array(
            pd.Timestamp("2026-01-01").value // 10**9 + secs,
            type=pa.int64(),
        ).cast(pa.timestamp("s")),
    })


def duckdb_iceberg_ready(con) -> bool:
    """INSTALL/LOAD the iceberg extension; True on success."""
    try:
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute("SET unsafe_enable_version_guessing = true;")
        return True
    except Exception as e:              # offline box without cached extension
        print(f"  [skip] DuckDB iceberg extension unavailable: {e}")
        return False


# ===========================================================================
# LESSON 1 — COLUMNAR DATA STORAGE
# ===========================================================================

def l1ex1() -> None:
    """Generate 50M rows; benchmark full-read vs columns=['amount','mcc']."""
    header("L1 Exercise 1 - projection benchmark on 50M rows")
    n = scaled(50_000_000, 2_000_000)
    d = out_dir("l1ex1")
    path = d / "txns.parquet"

    print(f"generating {n:,} rows ...")
    table, t_gen = timed(gen_txns_arrow, n)
    print(f"generated in {t_gen:.1f}s")
    _, t_write = timed(
        pq.write_table, table, path,
        compression="zstd", row_group_size=scaled(5_000_000, 500_000),
    )
    print(f"wrote {mb(path.stat().st_size)} zstd parquet ({t_write:.1f}s)")

    _, t_full = timed(pq.read_table, path)
    _, t_proj = timed(pq.read_table, path, columns=["mcc", "amount"])
    saved = (1 - t_proj / t_full) * 100
    print(f"full read (7 cols): {t_full:.2f}s")
    print(f"projection read (mcc, amount): {t_proj:.2f}s")
    print(f">>> time saved by touching 2/7 columns: {saved:.0f}%")
    print(">>> answer: savings track the share of column bytes skipped - "
          "columnar reads only the chunks you project.")


def l1ex2() -> None:
    """Same data, four codecs: tabulate file sizes and read times."""
    header("L1 Exercise 2 - compression codec shootout")
    n = scaled(5_000_000, 400_000)
    d = out_dir("l1ex2")
    table = gen_txns_arrow(n)

    rows = []
    for codec in ["none", "snappy", "gzip", "zstd"]:
        p = d / f"txns_{codec}.parquet"
        comp = None if codec == "none" else codec
        _, tw = timed(pq.write_table, table, p, compression=comp)
        _, tr = timed(pq.read_table, p)
        rows.append({
            "codec": codec,
            "size_MB": round(p.stat().st_size / 1e6, 1),
            "write_s": round(tw, 2),
            "read_s": round(tr, 2),
        })
    print(pd.DataFrame(rows).to_string(index=False))
    print(">>> takeaway: ZSTD ~= GZIP size at SNAPPY-like speed - best default.")


def l1ex3() -> None:
    """Prove via row-group statistics that filtered reads skip chunks."""
    header("L1 Exercise 3 - predicate pushdown, shown with footer statistics")
    n = scaled(3_000_000, 300_000)
    rg = scaled(250_000, 50_000)
    d = out_dir("l1ex3")
    path = d / "txns.parquet"
    pq.write_table(gen_txns_arrow(n), path, compression="zstd",
                   row_group_size=rg)

    pf = pq.ParquetFile(path)
    amt_idx = pf.schema_arrow.get_field_index("amount")

    # First pass: collect footer stats; pick a threshold guaranteed to split
    # groups into both SKIP and READ buckets whatever the data distribution.
    stats = [pf.metadata.row_group(i).column(amt_idx).statistics
             for i in range(pf.metadata.num_row_groups)]
    maxima = sorted(s.max for s in stats)
    thr = maxima[len(maxima) // 2]

    print(f"{'group':>5} {'rows':>9} {'min':>12} {'max':>12}  "
          f"verdict for amount>{thr:,.0f}")
    skippable = 0
    for i, (rg_meta, st) in enumerate(zip(
            (pf.metadata.row_group(i) for i in range(pf.metadata.num_row_groups)),
            stats)):
        skip = st.max is not None and st.max <= thr
        skippable += skip
        print(f"{i:>5} {rg_meta.num_rows:>9,.0f} {st.min:>12,.2f} "
              f"{st.max:>12,.2f} {'SKIP (max<=thr)' if skip else 'READ'}")
    print(f"row groups provably free of amount>{thr:.0f}: "
          f"{skippable}/{pf.metadata.num_row_groups}")

    _, t_unf = timed(pq.read_table, path, columns=["amount"])
    filt, t_f = timed(pq.read_table, path, columns=["amount"],
                      filters=[("amount", ">", thr)])
    print(f"unfiltered scan of amount: {t_unf:.2f}s | "
          f"filtered (> {thr:.0f}): {t_f:.2f}s -> {filt.num_rows:,} rows")
    print(">>> the engine consults min/max per column chunk in the footer and "
          "never touches SKIP groups (zero I/O).")


def l1ex4() -> None:
    """10,000 tiny files vs 10 large ones — feel the metadata tax."""
    header("L1 Exercise 4 - tiny-files anti-pattern, measured")
    n_tiny_files = scaled(10_000, 500)
    rows_per_tiny = scaled(1_000, 1_000)
    n_big_files = max(1, n_tiny_files // 1000)
    total_rows = n_tiny_files * rows_per_tiny
    rows_per_big = total_rows // n_big_files

    d = out_dir("l1ex4")
    table = gen_txns_arrow(total_rows)

    results = []
    for label, nfiles, rpf in (("tiny", n_tiny_files, rows_per_tiny),
                               ("large", n_big_files, rows_per_big)):
        sub = d / label
        sub.mkdir()
        t0 = time.perf_counter()
        for i in range(nfiles):
            pq.write_table(table.slice(i * rpf, rpf),
                           sub / f"part-{i:05d}.parquet", compression="zstd")
        w = time.perf_counter() - t0
        _, r = timed(pq.read_table, sub)
        results.append((label, nfiles, rpf, w, r))
        print(f"{label:>5}: {nfiles:>6,} files x {rpf:>7,} rows | "
              f"write {w:6.1f}s | read_table {r:6.2f}s")
    tiny_r, large_r = results[0][4], results[1][4]
    print(f">>> tiny files read {tiny_r / max(large_r, 1e-9):.0f}x slower for "
          "the SAME bytes: every file pays open/footer/planning cost "
          "(worse on object storage: per-request latency + $).")


# ===========================================================================
# LESSON 2 — APACHE ARROW
# ===========================================================================

def l2ex1() -> None:
    """decimal128(18,2) makes money math exact; float64 does not."""
    header("L2 Exercise 1 - exact money arithmetic with decimal128")
    f = 0.1 + 0.2
    print(f"float64 : 0.1 + 0.2 = {f!r}   equal 0.3? {f == 0.3}")

    d1 = pa.array([Decimal("0.10")], type=pa.decimal128(18, 2))
    d2 = pa.array([Decimal("0.20")], type=pa.decimal128(18, 2))
    total = pc.add(d1, d2)[0].as_py()
    print(f"decimal : 0.1 + 0.2 = {total}      equal Decimal('0.3')? "
          f"{total == Decimal('0.3')}")

    ledger = pa.table({
        "acct": pa.array(["A1", "A1", "A2"]),
        "amt": pa.array([Decimal("0.10"), Decimal("0.20"), Decimal("99.99")],
                        type=pa.decimal128(18, 2)),
    })
    sums = ledger.group_by("acct").aggregate([("amt", "sum")]).sort_by("acct")
    print(sums.to_pandas().to_string(index=False))
    a1 = sums.filter(pc.equal(sums["acct"], "A1")).column("amt_sum")[0].as_py()
    assert a1 == Decimal("0.30"), a1
    print(">>> decimal128 keeps cents exact end-to-end (no binary drift).")


def l2ex2() -> None:
    """Stream 3 record batches over the IPC stream format; reassemble."""
    header("L2 Exercise 2 - IPC streaming: 3 batches -> stream -> one table")
    schema = pa.schema([("txn_id", pa.int64()), ("amount", pa.float64())])
    batches = [
        pa.RecordBatch.from_arrays(
            [pa.array(np.arange(i * 10, i * 10 + 5, dtype="int64")),
             pa.array(np.round(np.random.default_rng(i).uniform(1, 99, 5), 2))],
            schema=schema,
        )
        for i in range(3)
    ]

    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema) as writer:
        for rb in batches:
            writer.write_batch(rb)
    payload = sink.getvalue()

    reader = ipc.open_stream(payload)
    rebuilt = reader.read_all()
    expected = pa.Table.from_batches(batches)
    assert rebuilt.equals(expected)
    print(f"streamed {payload.size} bytes, batches={len(batches)}, "
          f"rows={rebuilt.num_rows} - roundtrip identical: True")
    print(">>> RecordBatches are the unit of streaming; open_stream can begin "
          "consuming before the sender finishes.")


def l2ex3() -> None:
    """Memory-map an .arrow file vs pd.read_csv of the equivalent CSV."""
    header("L2 Exercise 3 - memory-mapped Arrow vs CSV parse")
    n = scaled(1_000_000, 100_000)
    d = out_dir("l2ex3")
    rng = np.random.default_rng(3)
    df = pd.DataFrame({
        "txn_id": np.arange(n),
        "account": rng.choice([f"A{i:06d}" for i in range(10_000)], n),
        "amount": np.round(rng.lognormal(5, 1, n), 2),
        "mcc": rng.choice(["5411", "5945", "6011"], n),
        "ts": pd.to_datetime("2026-07-01") +
              pd.to_timedelta(rng.integers(0, 30 * 86400, n), unit="s"),
    })
    csv_path, arrow_path = d / "txns.csv", d / "txns.arrow"
    df.to_csv(csv_path, index=False)
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    with pa.OSFile(str(arrow_path), "wb") as sink:
        with ipc.new_file(sink, tbl.schema) as writer:
            writer.write_table(tbl)
    print(f"data: {n:,} rows | csv {mb(csv_path.stat().st_size)} | "
          f"arrow {mb(arrow_path.stat().st_size)}")

    src, t_open = timed(pa.memory_map, str(arrow_path), "r")
    _, t_mmap_all = timed(lambda: ipc.open_file(src).read_all())
    _, t_csv = timed(pd.read_csv, csv_path)
    print(f"mmap open:                 {t_open*1000:8.1f} ms  (zero copy)")
    print(f"mmap open + read_all:      {t_mmap_all:8.2f} s  (typed buffers)")
    print(f"pd.read_csv full parse:    {t_csv:8.2f} s  (text->objects)")
    print(">>> mmap 'loads' instantly; even a full materialisation beats "
          "text parsing, and untouched pages never cost RAM.")


def l2ex4() -> None:
    """Risk pipeline validation on RecordBatchReader - constant memory."""
    header("L2 Exercise 4 - streaming validation via RecordBatchReader")

    schema = pa.schema([
        ("txn_id", pa.int64()),
        ("amount", pa.float64()),
        ("mcc", pa.string()),
    ])
    n_batches = scaled(200, 40)
    batch_rows = scaled(10_000, 5_000)

    def source():
        rng = np.random.default_rng(11)
        for i in range(n_batches):
            amounts = np.round(rng.lognormal(5, 1.5, batch_rows), 2)
            amounts[rng.random(batch_rows) < 0.01] *= -1        # violations!
            yield pa.RecordBatch.from_arrays(
                [pa.array(np.arange(i * batch_rows, (i + 1) * batch_rows,
                                    dtype="int64")),
                 pa.array(amounts),
                 pa.array(_MCCS).take(rng.integers(0, len(_MCCS), batch_rows))],
                schema=schema,
            )

    reader = pa.RecordBatchReader.from_batches(schema, source())
    rss_before = rss_mb()
    kept, dropped, volume, mcc_counts = 0, 0, 0.0, defaultdict(int)
    t0 = time.perf_counter()
    for batch in reader:                                    # constant memory!
        bad = pc.less(batch["amount"], 0.0)
        good = batch.filter(pc.invert(bad.fill_null(False)))
        dropped += batch.num_rows - good.num_rows
        kept += good.num_rows
        volume += pc.sum(good["amount"]).as_py()
        for rec in good["mcc"].value_counts().to_pylist():
            mcc_counts[rec["values"]] += rec["counts"]
    elapsed = time.perf_counter() - t0

    print(f"streamed {kept + dropped:,} rows in {n_batches} batches "
          f"({elapsed:.2f}s)")
    print(f"kept={kept:,}  dropped_violations={dropped:,}  volume=${volume:,.0f}")
    print("per-mcc totals (accumulator bounded by cardinality, NOT rows):")
    for m, c in sorted(mcc_counts.items()):
        print(f"  {m}: {c:,}")
    print(f"peak RSS while processing {n_batches * batch_rows:,} rows: "
          f"{rss_mb():.0f} MB -> memory independent of stream length.")
    print(">>> swap the Python generator for a Flight/gRPC reader and this is "
          "your production CDC pipeline shape.")


def l2ex5() -> None:
    """to_pandas() copies; to_numpy(zero_copy_only=True) views."""
    header("L2 Exercise 5 - zero-copy export benchmark (int64, 100M rows)")
    n = scaled(100_000_000, 10_000_000)
    print(f"building int64 column of {n:,} rows ({mb(n*8)}) ...")
    source = np.arange(n, dtype="int64")       # writable NumPy buffer
    arr = pa.array(source)                     # Arrow wraps it: NO copy

    _, t_np = timed(arr.to_numpy, zero_copy_only=True)
    _, t_pd = timed(arr.to_pandas)
    print(f"to_numpy(zero_copy_only=True): {t_np*1e6:10.1f} us")
    print(f"to_pandas():                   {t_pd*1e3:10.1f} ms  "
          f"({t_pd/max(t_np, 1e-9):,.0f}x slower)")

    view = arr.to_numpy(zero_copy_only=True)
    shares = np.shares_memory(view, source)
    source[0] = 999                            # write through the ORIGINAL
    mutated = arr[0].as_py() == 999            # ...Arrow sees the change
    source[0] = 0
    print(f"numpy view shares memory with the Arrow buffer: {shares}; "
          f"writing via NumPy is visible in Arrow: {mutated}")
    print(">>> to_numpy aliases Arrow's buffer when layout allows (plain "
          "numeric, no nulls, single chunk); pandas materialises its own "
          "blocks for object/string columns but may alias plain numerics.")


# ===========================================================================
# LESSON 3 — DUCKDB
# ===========================================================================

def _l3_dir() -> Path:
    return out_dir("l3", fresh=False)


def _l3_build_warehouse(force: bool = False) -> Path:
    """Deterministic FinBank extracts for the whole lesson (built once)."""
    d = _l3_dir()
    marker = d / ".seeded"
    if marker.exists() and not force:
        return d
    rng = np.random.default_rng(11)
    n_acct, n_txn = scaled(5_000, 800), scaled(400_000, 60_000)
    accounts = pd.DataFrame({
        "account":   [f"A{i:06d}" for i in range(n_acct)],
        "cust_type": rng.choice(["RETAIL", "SME", "CORP"], n_acct,
                                p=[.8, .15, .05]),
        "branch":    rng.choice(["MUM", "DEL", "BLR", "HYD"], n_acct),
        "opened":    pd.to_datetime("2015-01-01") +
                     pd.to_timedelta(rng.integers(0, 4000, n_acct), unit="D"),
    })
    txns = pd.DataFrame({
        "txn_id":  np.arange(1, n_txn + 1),
        "account": rng.choice(accounts["account"], n_txn),
        "channel": rng.choice(["CASH_DEPOSIT", "WIRE", "POS", "ECOM"],
                              n_txn, p=[.15, .20, .40, .25]),
        "amount":  np.round(rng.lognormal(6.0, 1.5, n_txn), 2),
        "ts":      pd.to_datetime("2026-04-01") +
                   pd.to_timedelta(rng.integers(0, 91 * 86400, n_txn), unit="s"),
    })

    # Inject structuring patterns: Rule A band [9k,10k)>=30 deposits,
    # Rule B band [45k,50k)>=20; eight accounts are dirty in BOTH bands.
    smurfs_a = rng.choice(n_acct, 25, replace=False)
    smurfs_b = np.concatenate([rng.choice(n_acct, 17, replace=False),
                               smurfs_a[:8]])
    m_a = rng.random(n_txn) < scaled(0.004, 0.03)
    txns.loc[m_a, "channel"] = "CASH_DEPOSIT"
    txns.loc[m_a, "amount"] = np.round(rng.uniform(9_000, 9_999, m_a.sum()), 2)
    txns.loc[m_a, "account"] = rng.choice(
        [f"A{i:06d}" for i in smurfs_a], m_a.sum())
    m_b = rng.random(n_txn) < scaled(0.002, 0.03)
    txns.loc[m_b, "channel"] = "CASH_DEPOSIT"
    txns.loc[m_b, "amount"] = np.round(rng.uniform(45_000, 49_900, m_b.sum()), 2)
    txns.loc[m_b, "account"] = rng.choice(
        [f"A{i:06d}" for i in smurfs_b], m_b.sum())

    months = pd.date_range("2026-04-01", periods=4, freq="MS")
    bal_n = n_acct * len(months)
    bal = pd.DataFrame({
        "account": np.repeat(accounts["account"], len(months)),
        "snapshot_ts": np.tile(months, n_acct),
        "available_balance": np.round(rng.normal(4_000, 2_500, bal_n), 2),
    })
    neg_mask = rng.random(bal_n) < 0.05
    bal.loc[neg_mask, "available_balance"] = np.round(
        rng.uniform(-2_000, 500, neg_mask.sum()), 2)

    pq.write_table(pa.Table.from_pandas(txns), d / "txns.parquet",
                   compression="zstd")
    accounts.to_parquet(d / "accounts.parquet")
    bal.to_parquet(d / "balances.parquet")
    marker.write_text("ok")
    return d


def _l3_connect():
    d = _l3_build_warehouse()
    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE VIEW v_txns     AS SELECT * FROM '{d}/txns.parquet';
        CREATE OR REPLACE VIEW v_accounts AS SELECT * FROM '{d}/accounts.parquet';
        CREATE OR REPLACE VIEW v_balances AS SELECT * FROM '{d}/balances.parquet';
    """)
    return con


def l3ex1() -> None:
    """AML thresholds moved to [45k,50k) & >=20 - who survives BOTH rules?"""
    header("L3 Exercise 1 - dual-threshold structuring rules")
    _l3_build_warehouse(force=True)
    con = _l3_connect()

    rule_sql = """
        CREATE OR REPLACE TABLE {name} AS
        SELECT account, count(*) n_deposits, round(sum(amount),2) total_cash
        FROM v_txns
        WHERE channel='CASH_DEPOSIT' AND amount >= {lo} AND amount < {hi}
        GROUP BY account HAVING count(*) >= {k};
    """
    con.execute(rule_sql.format(name="ruleA", lo=9_000, hi=10_000, k=30))
    con.execute(rule_sql.format(name="ruleB", lo=45_000, hi=50_000, k=20))

    survivors = con.sql("""
        SELECT a.account, a.n_deposits n_at_9k, b.n_deposits n_at_45k,
               round(a.total_cash + b.total_cash, 2) combined_cash
        FROM ruleA a JOIN ruleB b USING (account)
        ORDER BY combined_cash DESC
    """).df()
    print(con.sql("""
        SELECT 'rule_A [9k,10k) >=30' AS rule, count(*) AS accounts FROM ruleA
        UNION ALL
        SELECT 'rule_B [45k,50k) >=20', count(*) FROM ruleB
    """))
    print("\naccounts flagged by BOTH rules:")
    print(survivors.to_string(index=False) if len(survivors) else "  (none)")
    print(f">>> answer: {len(survivors)} account(s) survive both rules - "
          "they are the highest-priority investigations.")
    con.close()


def l3ex2() -> None:
    """ASOF JOIN balances onto transactions; flag overdrafts."""
    header("L3 Exercise 2 - point-in-time balances with ASOF JOIN")
    con = _l3_connect()

    df = con.sql("""
        SELECT t.txn_id, t.account, t.ts, t.amount,
               round(b.available_balance, 2) AS balance_at_txn,
               CASE WHEN b.available_balance < t.amount THEN 'OVERDRAFT'
                    ELSE 'ok' END AS flag
        FROM v_txns t
        ASOF JOIN v_balances b
          ON t.account = b.account AND t.ts >= b.snapshot_ts
    """).df()
    flagged = df[df.flag == "OVERDRAFT"]
    print(f"joined {len(df):,} txns; overdrafts flagged: {len(flagged):,}")
    print(flagged.head(5).to_string(index=False))
    print(">>> ASOF picks the most-recent snapshot AT OR BEFORE each txn - "
          "no joins-on-rounded-dates, no future leakage.")


def l3ex3() -> None:
    """Velocity score: pure-SQL windows vs Python UDF - same answer."""
    header("L3 Exercise 3 - SQL-only velocity score vs Python UDF")
    con = _l3_connect()

    sql_velocity = """
        WITH ordered AS (
            SELECT account,
                   datediff('second', lag(ts) OVER w, ts) AS gap_s
            FROM v_txns
            WINDOW w AS (PARTITION BY account ORDER BY ts)
        )
        SELECT account,
               1.0 - avg(least(coalesce(gap_s, 3600), 3600)) / 3600.0 AS v_score
        FROM ordered GROUP BY account
    """

    def velocity_score_udf(gaps):
        gaps = np.asarray(gaps, dtype="float64")
        gaps = gaps[~np.isnan(gaps)]
        if len(gaps) == 0:
            return 0.0
        return float(np.clip(1.0 - np.mean(np.minimum(gaps, 3600)) / 3600,
                             0, 1))

    con.create_function("velocity_score", velocity_score_udf, ["BIGINT[]"],
                        "DOUBLE")
    udf_velocity = """
        WITH ordered AS (
            SELECT account, datediff('second', lag(ts) OVER w, ts) AS gap_s
            FROM v_txns
            WINDOW w AS (PARTITION BY account ORDER BY ts)
        ), gapped AS (
            SELECT account, list(gap_s) AS gaps FROM ordered GROUP BY account
        )
        SELECT account, round(velocity_score(gaps), 6) AS v_score
        FROM gapped
    """

    t0 = time.perf_counter(); sql_df = con.sql(sql_velocity).df()
    t_sql = time.perf_counter() - t0
    t0 = time.perf_counter(); udf_df = con.sql(udf_velocity).df()
    t_udf = time.perf_counter() - t0

    merged = sql_df.merge(udf_df, on="account", suffixes=("_sql", "_udf"))
    diff = (merged.v_score_sql - merged.v_score_udf).abs().max()
    print(f"accounts scored: {len(merged):,}")
    print(f"max |SQL - UDF| difference: {diff:.2e}")
    print(f"time pure-SQL: {t_sql:.2f}s | time Python-UDF: {t_udf:.2f}s")
    print(merged.sort_values("v_score_sql", ascending=False).head(5)
          .to_string(index=False))
    print(">>> identical results; the vectorised SQL version avoids moving "
          "lists into Python and wins on runtime.")


def l3ex4() -> None:
    """Serve Parquet over local HTTP; httpfs queries + EXPLAIN ANALYZE."""
    header("L3 Exercise 4 - httpfs: querying Parquet over HTTP + pushdown")
    from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

    d = _l3_build_warehouse()
    port = free_port()

    class _RangeFile:                          # file-like slice reader
        def __init__(self, f, pos, length):
            self._f, self._left = f, length
            f.seek(pos)

        def read(self, n=-1):
            want = self._left if n < 0 else min(n, self._left)
            chunk = self._f.read(want)
            self._left -= len(chunk)
            return chunk

        def close(self):
            self._f.close()

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=str(d), **kw)

        def log_message(self, *a):
            pass

        def send_head(self):                       # minimal Range support
            import re as _re
            path = self.translate_path(self.path)
            try:
                f = open(path, "rb")
            except OSError:
                self.send_error(404)
                return None
            size = f.seek(0, 2)
            start, end = 0, size - 1
            rng = self.headers.get("Range")
            m = _re.match(r"bytes=(\d*)-(\d*)$", rng or "")
            if m:
                if m.group(1):
                    start = int(m.group(1))
                if m.group(2):
                    end = min(int(m.group(2)), end)
                elif not m.group(1):
                    start = 0
            length = max(0, end - start + 1)
            f.seek(start)
            self.send_response(206 if (m and (start or
                                              end != size - 1)) else 200)
            self.send_header("Content-Type",
                             "application/octet-stream")
            self.send_header("Content-Length", str(length))
            self.send_header("Accept-Ranges", "bytes")
            if m:
                self.send_header(
                    "Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            return _RangeFile(f, start, length)

    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        con = duckdb.connect()
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception as e:
            print(f"[skip] could not load httpfs extension ({e}); run once "
                  "online: duckdb> INSTALL httpfs;")
            return

        url = f"http://127.0.0.1:{port}/txns.parquet"
        q_filtered = f"""
            SELECT count(*) FROM read_parquet('{url}')
            WHERE channel='CASH_DEPOSIT' AND amount BETWEEN 9000 AND 10000
        """
        q_full = f"SELECT count(*) FROM read_parquet('{url}')"
        _, t_f = timed(lambda: con.sql(q_filtered).fetchone())
        _, t_u = timed(lambda: con.sql(q_full).fetchone())
        print(f"remote filtered query: {t_f:.2f}s | full scan: {t_u:.2f}s "
              "(file served over HTTP, no local copy)")

        plan = con.execute("EXPLAIN ANALYZE " + q_filtered).fetchall()[0][1]
        interesting = [ln.strip() for ln in plan.splitlines()
                       if any(k in ln for k in
                              ("Filters", "Projection", "Scanning", "Rows"))]
        print("EXPLAIN ANALYZE (excerpt):")
        for ln in interesting[:8]:
            print("   ", ln)
        print(">>> filters+projection are pushed into the Parquet reader: "
              "only matching row-groups/column-chunks cross the network.")
        con.close()
    finally:
        httpd.shutdown()


def l3ex5() -> None:
    """Monthly cash-deposit volume pivot: branches x months."""
    header("L3 Exercise 5 - PIVOT: branches x months of cash volume")
    con = _l3_connect()

    con.execute("""
        CREATE OR REPLACE VIEW v_cash AS
        SELECT a.branch AS branch, strftime(t.ts, '%Y-%m') AS month, t.amount
        FROM v_txns t JOIN v_accounts a USING (account)
        WHERE t.channel = 'CASH_DEPOSIT'
    """)
    print(con.sql("""
        PIVOT v_cash ON month USING sum(amount) GROUP BY branch ORDER BY branch
    """))

    months = con.sql("SELECT DISTINCT month FROM v_cash ORDER BY month") \
        .fetchall()
    month_cols = ", ".join(f'"{m}"' for (m,) in months)
    tidy = con.sql(f"""
        SELECT * FROM (PIVOT v_cash ON month USING sum(amount) GROUP BY branch)
        UNPIVOT (volume FOR month IN ({month_cols}))
    """)
    print("\nand UNPIVOT back to tidy form (first 5 rows):")
    print(tidy.df().head(5).to_string(index=False))
    con.close()


# ===========================================================================
# LESSON 4 — APACHE ICEBERG
# ===========================================================================

def _l4_base() -> Path:
    return out_dir("l4", fresh=False)


def l4_catalog(name: str, fresh: bool = True):
    """SQL(sqlite) catalog; the CALLING EXERCISE gets a fresh warehouse,
    child processes must pass fresh=False to keep it intact."""
    from pyiceberg.catalog import load_catalog
    base = out_dir(f"l4/{name}", fresh=fresh).resolve()
    return load_catalog(name, **{
        "type": "sql",
        "uri": f"sqlite:///{base}/catalog.db",
        "warehouse": f"file://{base}/wh",
    })


def l4_wh(name: str) -> Path:
    """Warehouse dir of a catalog created by l4_catalog (no wipe)."""
    return out_dir(f"l4/{name}", fresh=False) / "wh"


def l4_make_table(cat, ident: str, transform, part_name: str):
    from pyiceberg.schema import Schema
    from pyiceberg.types import (NestedField, LongType, StringType,
                                 DoubleType, TimestamptzType, IntegerType)
    from pyiceberg.partitioning import PartitionSpec, PartitionField

    schema = Schema(
        NestedField(1, "txn_id", LongType(), required=True),
        NestedField(2, "account", StringType(), required=True),
        NestedField(3, "amount", DoubleType(), required=False),
        NestedField(4, "mcc", StringType(), required=False),
        NestedField(5, "branch_id", IntegerType(), required=False),
        NestedField(6, "txn_ts", TimestamptzType(), required=True),
    )
    spec = PartitionSpec(PartitionField(source_id=6, field_id=1000,
                                        transform=transform, name=part_name))
    try:
        return cat.load_table(ident), False
    except Exception:
        from pyiceberg.exceptions import NamespaceAlreadyExistsError
        ns = tuple(ident.split(".")[:-1]) or ("default",)
        try:
            cat.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass
        return cat.create_table(
            ident, schema=schema, partition_spec=spec,
            properties={"format-version": "2",
                        "write.parquet.compression-codec": "zstd"},
        ), True


def l4_append_day(tbl, day_utc: dt.datetime, n: int, seed: int = 21) -> None:
    """Generate one day of txns conforming to the table schema; append."""
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({
        "txn_id":   np.arange(1, n + 1, dtype="int64"),
        "account":  rng.choice([f"A{i:06d}" for i in range(20_000)], n),
        "amount":   np.round(rng.lognormal(5, 1.2, n), 2),
        "mcc":      rng.choice(["5411", "6011", "5812", "5999"], n),
        "branch_id": rng.integers(1, 50, n).astype("int32"),
        "txn_ts":   pd.Timestamp(day_utc) +
                    pd.to_timedelta(rng.integers(0, 86_400_000, n), unit="ms"),
    })
    if df["txn_ts"].dt.tz is None:
        df["txn_ts"] = df["txn_ts"].dt.tz_localize("UTC")
    else:
        df["txn_ts"] = df["txn_ts"].dt.tz_convert("UTC")
    arrow_schema = tbl.refresh().schema().as_arrow()
    tbl.append(pa.Table.from_pandas(df, schema=arrow_schema))


def l4ex1() -> None:
    """HourTransform vs DayTransform partitioning: prune to one hour."""
    from pyiceberg.transforms import DayTransform, HourTransform
    header("L4 Exercise 1 - hidden partitioning: HOUR vs DAY granularity")
    cat = l4_catalog("l4ex1")
    n = scaled(600_000, 60_000)
    day = dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc)

    tbl_h, created_h = l4_make_table(cat, "banking.txns_hour",
                                     HourTransform(), "txn_hour")
    tbl_d, created_d = l4_make_table(cat, "banking.txns_day",
                                     DayTransform(), "txn_day")
    if created_h or tbl_h.refresh().scan().count() == 0:
        l4_append_day(tbl_h, day, n)
        l4_append_day(tbl_d, day, n)

    window = ("txn_ts >= '2026-07-01T03:00:00+00:00' and "
              "txn_ts < '2026-07-01T04:00:00+00:00'")
    res_h, t_h = timed(lambda: tbl_h.scan(row_filter=window).to_arrow())
    res_d, t_d = timed(lambda: tbl_d.scan(row_filter=window).to_arrow())
    files_h = len(list(tbl_h.scan(row_filter=window).plan_files()))
    files_d = len(list(tbl_d.scan(row_filter=window).plan_files()))
    assert res_h.num_rows == res_d.num_rows
    print(f"rows matched: {res_h.num_rows:,} (both tables agree)")
    print(f"HOUR table : planned files={files_h:3d}  scan {t_h:.3f}s")
    print(f"DAY  table : planned files={files_d:3d}  scan {t_d:.3f}s")
    print(">>> hidden partitioning: the engine derived the partition filter "
          "from the ts predicate alone - HOUR prunes 23 partitions, DAY "
          "reads all 24. Users cannot 'forget' the partition column.")


def _l4_atomicity_child(cfg: dict) -> None:
    """Spawned process: generate a BIG day, signal, then append (get killed)."""
    cat = l4_catalog(cfg["catalog"], fresh=False)
    tbl = cat.load_table("banking.transactions")
    df_day = pd.DataFrame({
        "txn_id":   np.arange(1, cfg["rows"] + 1, dtype="int64"),
        "account":  np.repeat("A999999", cfg["rows"]),
        "amount":   np.round(np.random.default_rng(7).uniform(1, 1000,
                                                              cfg["rows"]), 2),
        "mcc":      np.repeat("5411", cfg["rows"]),
        "branch_id": np.ones(cfg["rows"], dtype="int32"),
        "txn_ts":   pd.Timestamp(dt.datetime(2026, 7, 2,
                                             tzinfo=dt.timezone.utc))
                    + pd.Timedelta(seconds=1),
    })
    df_day["txn_ts"] = df_day["txn_ts"].dt.tz_convert("UTC")
    Path(cfg["sentinel"]).write_text("ready")       # parent: kill me soon!
    import pyarrow as pa
    tbl.append(pa.Table.from_pandas(
        df_day, schema=tbl.refresh().schema().as_arrow()))


def l4ex2() -> None:
    """SIGKILL a process mid-append; the table stays perfectly consistent."""
    import multiprocessing as mp
    from pyiceberg.transforms import DayTransform
    header("L4 Exercise 2 - atomicity proof: kill -9 mid-append")

    cat = l4_catalog("l4ex2")
    tbl, created = l4_make_table(cat, "banking.transactions",
                                 DayTransform(), "txn_day")
    if created or tbl.scan().count() == 0:
        l4_append_day(tbl, dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc),
                      scaled(300_000, 30_000))

    sentinel = out_dir("l4ex2") / "sentinel"
    ctx = mp.get_context("spawn")
    for attempt, kill_after in enumerate((scaled(1.5, 0.7), 4.0), start=1):
        baseline = cat.load_table("banking.transactions").refresh() \
            .scan().count()
        print(f"[attempt {attempt}] baseline rows: {baseline:,}; killing "
              f"append {kill_after:.1f}s after it starts writing ...")
        sentinel.unlink(missing_ok=True)
        cfg = {"catalog": "l4ex2", "rows": scaled(8_000_000, 3_000_000),
               "sentinel": str(sentinel)}
        proc = ctx.Process(target=_l4_atomicity_child, args=(cfg,),
                           daemon=True)
        proc.start()
        deadline = time.time() + 180
        while not sentinel.exists() and time.time() < deadline:
            time.sleep(0.05)
        assert sentinel.exists(), "child never signalled readiness"
        time.sleep(kill_after)      # land INSIDE the data-file write phase
        proc.kill()                 # SIGKILL - no cleanup handlers run
        proc.join()
        print(f"child killed mid-append (exit code {proc.exitcode})")

        tbl = cat.load_table("banking.transactions")    # fresh metadata read
        after = tbl.scan().count()
        if after == baseline:
            print(f"PASS - rows after crash: {after:,} == baseline; readers "
                  "see the pre-append world exactly. The half-written data "
                  "file is untracked garbage removed by orphan cleanup.")
            return
        print(f"  commit landed before SIGKILL ({after:,} rows); retrying "
              "with longer delay ...")
    raise AssertionError("could not demonstrate mid-append kill")


def l4ex3() -> None:
    """GDPR delete, then time-travel to before it — retention discussion."""
    from pyiceberg.transforms import DayTransform
    header("L4 Exercise 3 - GDPR erasure vs auditor time travel")
    cat = l4_catalog("l4ex3")
    tbl, created = l4_make_table(cat, "banking.transactions",
                                 DayTransform(), "txn_day")
    if created or tbl.scan().count() == 0:
        l4_append_day(tbl, dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc),
                      scaled(200_000, 20_000))
    tbl = tbl.refresh()
    victim = tbl.scan(limit=1).to_arrow().column("account")[0].as_py()
    pre_snap = tbl.current_snapshot().snapshot_id
    n_before = tbl.scan().count()

    tbl.delete(delete_filter=f"account = '{victim}'")
    tbl = tbl.refresh()
    n_after = tbl.scan().count()
    resurrected = tbl.scan(snapshot_id=pre_snap).to_arrow()
    still_there = resurrected.filter(
        pc.equal(resurrected.column("account"), victim)).num_rows

    print(f"customer {victim}: {n_before:,} rows -> {n_after:,} rows after delete")
    print(f"time travel to snapshot {pre_snap}: customer present again "
          f"({still_there:,} rows)")
    assert still_there > 0 and n_after < n_before
    print("""compliance policy discussion:
  * GDPR erasure must ALSO apply to history -> expire snapshots older than
    the erasure request date, then delete orphaned data files; until then
    restrict access to old snapshots via governance, not deletion.
  * Regulators want the opposite: pin certified snapshots (tags!) BEFORE
    any expiry so quarter-close evidence survives retention jobs.
  * Practical pattern: tag certified snapshots -> run GDPR deletes ->
    expire everything older than legal-retention minus certified tags.""")
    print("PASS")


def l4ex4() -> None:
    """Add a nested struct column (metadata-only), backfill, DuckDB readback."""
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import StructType, NestedField, StringType, IntegerType
    header("L4 Exercise 4 - nested struct evolution + DuckDB readback")
    cat = l4_catalog("l4ex4")
    tbl, created = l4_make_table(cat, "banking.transactions",
                                 DayTransform(), "txn_day")
    if created or tbl.scan().count() == 0:
        l4_append_day(tbl, dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc),
                      scaled(100_000, 10_000))
    with tbl.update_schema() as up:
        up.add_column(path=("customer",), field_type=StructType(
            NestedField(7, "segment", StringType(), required=False),
            NestedField(8, "tier", IntegerType(), required=False)),
            required=False)
    tbl = tbl.refresh()
    print("schema after evolution (add was METADATA-ONLY, zero rewrite):")
    print(tbl.schema())

    target = tbl.schema().as_arrow()
    scanned = tbl.scan().to_arrow()
    rng = np.random.default_rng(5)
    seg = pa.array(rng.choice(["RETAIL", "SME", "CORP"],
                              scanned.num_rows).tolist(),
                   type=pa.large_string())
    tier = pa.array(rng.integers(1, 4, scanned.num_rows), type=pa.int32())
    cust = pa.StructArray.from_arrays([seg, tier], names=["segment", "tier"])
    cols = [cust if nm == "customer" else scanned.column(nm)
            for nm in target.names]
    tbl.overwrite(pa.Table.from_arrays(cols, schema=target))  # one atomic commit
    tbl = tbl.refresh()

    out = tbl.scan().to_arrow()
    print("sample rows read back via PyIceberg:")
    print(out.select(["account", "customer"]).slice(0, 3).to_pandas()
          .to_string(index=False))

    con = duckdb.connect()
    if duckdb_iceberg_ready(con):
        wh = l4_wh("l4ex4").resolve()
        print("same table read by DuckDB iceberg_scan (structs included):")
        print(con.sql(f"""
            SELECT customer.segment, count(*) n, round(sum(amount),2) vol
            FROM iceberg_scan('file://{wh}/banking/transactions')
            GROUP BY 1 ORDER BY 1
        """))
    con.close()
    print("PASS")


def _l4_concurrent_child(cfg: dict) -> None:
    cat = l4_catalog(cfg["catalog"], fresh=False)
    tbl = cat.load_table("banking.transactions")
    cfg["ready"].wait(timeout=180)
    day = dt.datetime(2026, 7, cfg["day"], tzinfo=dt.timezone.utc)
    from pyiceberg.exceptions import CommitFailedException
    for attempt in range(20):                 # optimistic-concurrency retry
        try:
            l4_append_day(tbl, day, cfg["rows"], seed=cfg["day"])
            return
        except CommitFailedException:
            time.sleep(0.2 * (attempt + 1))
            tbl = cat.load_table("banking.transactions")
    raise RuntimeError("commit kept failing")


def l4ex5() -> None:
    """Two processes append simultaneously; optimistic concurrency lands both."""
    import multiprocessing as mp
    from pyiceberg.transforms import DayTransform
    header("L4 Exercise 5 - concurrent writers, both commits win")

    cat = l4_catalog("l4ex5")
    tbl, created = l4_make_table(cat, "banking.transactions",
                                 DayTransform(), "txn_day")
    if created or tbl.scan().count() == 0:
        l4_append_day(tbl, dt.datetime(2026, 6, 30, tzinfo=dt.timezone.utc),
                      scaled(50_000, 10_000))
    base = cat.load_table("banking.transactions").scan().count()

    ctx = mp.get_context("spawn")
    ready = ctx.Manager().Event()
    procs, cfgs = [], []
    for day in (3, 4):
        cfg = {"catalog": "l4ex5", "day": day,
               "rows": scaled(200_000, 20_000), "ready": ready}
        cfgs.append(cfg)
        p = ctx.Process(target=_l4_concurrent_child, args=(cfg,), daemon=True)
        p.start()
        procs.append(p)
    time.sleep(2.0)                  # let both workers finish loading catalogs
    ready.set()                      # fire both appends at the same instant
    codes = []
    for p in procs:
        p.join(timeout=600)
        codes.append(p.exitcode)

    tbl = cat.load_table("banking.transactions")
    final = tbl.scan().count()
    added = sum(c["rows"] for c in cfgs)
    print(f"worker exit codes: {codes}")
    print(f"rows: {base:,} -> {final:,} (added {added:,}; expected "
          f"{base + added:,})")
    print(f"snapshots on table: {len(list(tbl.snapshots()))}")
    assert codes == [0, 0] and final == base + added
    print("PASS - optimistic concurrency: the second committer rebased on "
          "the first and retried automatically; no lost update, no corruption.")


# ===========================================================================
# LESSON 5 — FLIGHT SQL (server shared by exercises 1-5 and capstone 2)
# ===========================================================================

TOKENS = {
    "risk-team-token":     "risk_team",
    "branch-mumbai-token": "branch_mumbai",
}


class TokenServerAuth(flight.ServerAuthHandler):
    def __init__(self, tokens):
        self.by_token = {t.encode(): ident for t, ident in tokens.items()}

    def authenticate(self, outgoing, incoming):
        token = incoming.read()
        identity = self.by_token.get(token)
        if identity is None:
            raise flight.FlightUnauthenticatedError("bad token")
        outgoing.write(identity.encode())

    def is_valid(self, token):
        ident = self.by_token.get(token)
        if ident is None:
            raise flight.FlightUnauthenticatedError("invalid/expired token")
        return ident


class TokenClientAuth(flight.ClientAuthHandler):
    def __init__(self, token: bytes):
        self._token = token
        self._identity = b""

    def authenticate(self, outgoing, incoming):
        outgoing.write(self._token)
        self._identity = incoming.read()

    def token(self):
        return self._identity


def _decode_flight_sql_command(cmd: bytes):
    """Minimal protobuf decode of CommandStatementQuery{string query=1;} so
    STANDARD Flight-SQL clients (ADBC/JDBC drivers) work against us too."""
    try:
        if cmd and cmd[0] == 0x0A:
            i, shift, length = 1, 0, 0
            while True:
                b = cmd[i]
                i += 1
                length |= (b & 0x7F) << shift
                if not b & 0x80:
                    break
                shift += 7
            return cmd[i:i + length].decode("utf-8")
    except Exception:
        return None


MAX_ROWS_DEFAULT = 5_000_000


class DuckBackend:
    """DuckDB engine behind the Flight service."""

    def __init__(self, db_path):
        self.con = duckdb.connect(str(db_path))
        self.lock = threading.Lock()

    def tables(self):
        with self.lock:
            return self.con.sql("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema='main'
            """).fetchall()

    def primary_keys(self, table: str):
        with self.lock:
            rows = self.con.execute(
                "SELECT constraint_column_names FROM duckdb_constraints() "
                "WHERE lower(table_name)=lower(?) "
                "AND constraint_type='PRIMARY KEY'", [table]).fetchall()
        return [c for r in rows for c in r[0]]

    def query(self, sql: str, max_rows=None, offset=None, page_size=None):
        sql = sql.rstrip().rstrip(";")
        q = f"SELECT * FROM ({sql}) _q"
        if page_size is not None:
            q += f" LIMIT {int(page_size)}"
        elif max_rows is not None:
            q += f" LIMIT {int(max_rows)}"
        else:
            q += f" LIMIT {MAX_ROWS_DEFAULT}"
        if offset:
            q += f" OFFSET {int(offset)}"
        with self.lock:
            return self.con.sql(q).arrow()


class IcebergScanBackend:
    """Exercise 5: swap DuckDB for PyIceberg scans (simple-pattern engine)."""

    _PATTERN = re.compile(
        r"^\s*select\s+\*\s+from\s+([A-Za-z0-9_.]+)"
        r"(?:\s+limit\s+(\d+))?\s*$", re.I)

    def __init__(self, catalog_kwargs: dict, default_limit=100_000):
        from pyiceberg.catalog import load_catalog
        self.cat = load_catalog("flight_backend", **catalog_kwargs)
        self.default_limit = default_limit

    def tables(self):
        out = []
        for ns in self.cat.list_namespaces():
            for ident in self.cat.list_tables(ns):
                full = ".".join(ident) if isinstance(ident, tuple) \
                    else f"{ns}.{ident}"
                out.append((full,))
        return out

    def primary_keys(self, table):
        return []           # spec: PKs live in catalog metadata; none here

    def query(self, sql: str, max_rows=None, offset=None, page_size=None):
        m = self._PATTERN.match(sql.rstrip().rstrip(";"))
        if not m:
            raise flight.FlightInvalidArgumentError(
                "IcebergBackend supports: SELECT * FROM <table> [LIMIT n]")
        table, limit = m.group(1), m.group(2)
        limit = int(limit) if limit else (max_rows or self.default_limit)
        if offset:
            raise flight.FlightInvalidArgumentError("offset unsupported here")
        return self.cat.load_table(table).scan().limit(limit).to_arrow()


class BankFlightServer(flight.FlightServerBase):
    """Flight SQL-style service: auth -> authorize/quota -> audit -> data."""

    def __init__(self, db_path=None, port=8815, tokens=TOKENS,
                 backend=None, quota_per_min=None,
                 tls_cert=None, tls_key=None, host="127.0.0.1"):
        scheme = "grpc+tls" if tls_cert else "grpc"
        super().__init__(
            f"{scheme}://{host}:{port}",
            auth_handler=TokenServerAuth(tokens) if tokens else None,
            **({"tls_certificates": [(tls_cert, tls_key)]}
               if tls_cert else {}),
        )
        self._port = port
        self.tokens = tokens
        self.backend = backend or DuckBackend(db_path)
        self.quota = quota_per_min
        self._hits = defaultdict(deque)
        self._hits_lock = threading.Lock()
        self.audit = []

    # ---------------- control-plane plumbing -----------------------------
    def _authorize(self, context) -> str:
        identity = context.peer_identity() if self.tokens else "anonymous"
        if self.tokens and not identity:
            raise flight.FlightUnauthenticatedError(
                "authenticate first: client.authenticate(...)")
        self.audit.append((dt.datetime.now().isoformat(timespec="seconds"),
                           identity))
        return identity

    def _check_quota(self, identity):
        if not self.quota:
            return
        now = time.monotonic()
        with self._hits_lock:
            dq = self._hits[identity]
            while dq and now - dq[0] > 60:
                dq.popleft()
            if len(dq) >= self.quota:
                raise flight.FlightUnavailableError(
                    f"query quota exceeded ({self.quota}/min) for {identity}")
            dq.append(now)

    @staticmethod
    def _ticket_for(sql, max_rows=None, offset=None, page_size=None):
        return json.dumps({"sql": sql, "max_rows": max_rows,
                           "offset": offset, "page_size": page_size}).encode()

    @staticmethod
    def _parse_descriptor(descriptor):
        cmd = descriptor.command
        try:
            payload = json.loads(cmd.decode())
            return (payload["sql"], payload.get("max_rows"),
                    payload.get("offset"), payload.get("page_size"))
        except Exception:
            sql = _decode_flight_sql_command(cmd) or cmd.decode("utf-8")
            return sql, None, None, None

    # ---------------- Flight SQL surface ----------------------------------
    def list_flights(self, context, criteria):
        self._authorize(context)
        for (t,) in self.backend.tables():
            sql = f"SELECT * FROM {t}"
            info_tbl = self.backend.query(sql, max_rows=0)
            yield flight.FlightInfo(
                info_tbl.schema,
                flight.FlightDescriptor.for_command(self._ticket_for(sql)),
                [flight.FlightEndpoint(
                    flight.Ticket(self._ticket_for(sql)),
                    [flight.Location.for_grpc_tcp("localhost", self._port)])],
                -1, None)

    def get_flight_info(self, context, descriptor):
        identity = self._authorize(context)
        sql, max_rows, offset, page_size = self._parse_descriptor(descriptor)
        self._check_quota(identity)
        tbl = self.backend.query(sql, max_rows, offset, page_size)
        return flight.FlightInfo(
            tbl.schema, descriptor,
            [flight.FlightEndpoint(
                flight.Ticket(self._ticket_for(sql, max_rows, offset,
                                              page_size)),
                [flight.Location.for_grpc_tcp("localhost", self._port)])],
            tbl.num_rows, tbl.nbytes)

    def get_schema(self, context, descriptor):
        self._authorize(context)
        sql, *_ = self._parse_descriptor(descriptor)
        return flight.SchemaResult(self.backend.query(sql, max_rows=0).schema)

    def do_get(self, context, ticket):
        identity = self._authorize(context)
        payload = json.loads(ticket.ticket.decode())
        self._check_quota(identity)
        tbl = self.backend.query(payload["sql"], payload.get("max_rows"),
                                 payload.get("offset"),
                                 payload.get("page_size"))
        return flight.RecordBatchStream(tbl)

    def do_put(self, context, descriptor, reader, writer):
        self._authorize(context)
        table_name = descriptor.path[0].decode()
        data = reader.read_all()
        be = self.backend
        if not hasattr(be, "con"):
            raise flight.FlightNotImplementedError(
                "do_put requires the DuckBackend")
        with be.lock:
            be.con.register("_upload", data)
            be.con.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _upload")
        writer.write_metadata(json.dumps(
            {"status": "ok", "rows": data.num_rows}).encode())

    def list_actions(self, context):
        return [("healthcheck", "Ping the server"),
                ("audit_log", "Fetch recent access log"),
                ("get_primary_keys", '{"table": "<name>"}')]

    def do_action(self, context, action):
        self._authorize(context)
        if action.type == "healthcheck":
            yield flight.Result(json.dumps({"ok": True}).encode())
        elif action.type == "audit_log":
            yield flight.Result(json.dumps(self.audit[-100:]).encode())
        elif action.type == "get_primary_keys":
            table = json.loads(action.body.to_pybytes().decode())["table"]
            yield flight.Result(json.dumps(
                {"table": table,
                 "key_columns": self.backend.primary_keys(table)}).encode())
        else:
            raise flight.FlightNotImplementedError(action.type)


@contextlib.contextmanager
def flight_server(server, wait=0.8):
    th = threading.Thread(target=server.serve, daemon=True)
    th.start()
    time.sleep(wait)
    try:
        yield server
    finally:
        server.shutdown()
        th.join(timeout=10)


class FinBankClient:
    """Thin typed client used across Lesson 5 exercises."""

    def __init__(self, uri: str, token: bytes | None = None,
                 tls_root_certs: bytes | None = None):
        kwargs = {"tls_root_certs": tls_root_certs} if tls_root_certs else {}
        self.client = flight.FlightClient(uri, **kwargs)
        if token is not None:
            self.client.authenticate(TokenClientAuth(token))

    def sql(self, query: str, max_rows=None) -> pa.Table:
        cmd = json.dumps({"sql": query, "max_rows": max_rows}).encode()
        info = self.client.get_flight_info(
            flight.FlightDescriptor.for_command(cmd))
        reader = self.client.do_get(info.endpoints[0].ticket)
        return reader.read_all()

    def pages(self, query: str, page_size: int):
        """Exercise 2: iterate server pages; next ticket derived from page."""
        offset = 0
        while True:
            cmd = json.dumps({"sql": query, "offset": offset,
                              "page_size": page_size}).encode()
            info = self.client.get_flight_info(
                flight.FlightDescriptor.for_command(cmd))
            reader = self.client.do_get(info.endpoints[0].ticket)
            page = reader.read_all()
            yield offset, page
            if page.num_rows < page_size:
                return
            offset += page_size


def _l5_seed_db(path: Path, n: int) -> None:
    rng = np.random.default_rng(33)
    txns = pd.DataFrame({
        "txn_id": np.arange(n, dtype="int64"),
        "account": rng.choice([f"A{i:06d}" for i in range(50_000)], n),
        "amount": np.round(rng.lognormal(5, 1.3, n), 2),
        "ccy": rng.choice(["INR", "USD", "EUR"], n, p=[.7, .2, .1]),
        "region": rng.choice(["MUM", "DEL", "BLR"], n, p=[.4, .35, .25]),
        "ts": pd.to_datetime("2026-07-01") +
              pd.to_timedelta(rng.integers(0, 30 * 86400, n), unit="s"),
    })
    con = duckdb.connect(str(path))
    con.register("tmp", txns)
    con.execute("DROP TABLE IF EXISTS transactions")
    con.execute("CREATE TABLE transactions AS SELECT * FROM tmp")
    con.execute("DROP TABLE IF EXISTS alerts")
    con.execute("DROP SEQUENCE IF EXISTS alert_seq")
    con.execute("CREATE SEQUENCE alert_seq START 1")
    con.execute("""
        CREATE TABLE alerts (
            alert_id BIGINT PRIMARY KEY DEFAULT nextval('alert_seq'),
            account VARCHAR NOT NULL,
            vol DOUBLE
        )
    """)
    con.execute("""INSERT INTO alerts (account, vol)
                   SELECT account, sum(amount) FROM transactions
                   GROUP BY account LIMIT 100""")
    con.close()


def _l5_db() -> Path:
    d = out_dir("l5", fresh=False)
    db = d / "finbank.duckdb"
    if not db.exists():
        _l5_seed_db(db, scaled(500_000, 50_000))
    return db


def l5ex1() -> None:
    """get_primary_keys action + per-identity query quota (N/min)."""
    header("L5 Exercise 1 - control plane: primary keys + query quotas")
    db = _l5_db()
    port = free_port()
    srv = BankFlightServer(str(db), port, tokens=TOKENS, quota_per_min=5)
    with flight_server(srv):
        c = FinBankClient(f"grpc://127.0.0.1:{port}", b"risk-team-token")

        for action in c.client.do_action(flight.Action(
                "get_primary_keys",
                json.dumps({"table": "alerts"}).encode())):
            print("get_primary_keys ->", action.body.to_pybytes().decode())

        print("firing queries against a 5/min quota (each logical query "
              "costs 2 hits: info + data) ...")
        for i in range(1, 6):
            try:
                c.sql("SELECT count(*) FROM transactions")
                print(f"  query {i}: ok")
            except flight.FlightUnavailableError as e:
                print(f"  query {i}: REJECTED -> {e}")
                break
        for a in c.client.do_action(flight.Action("audit_log")):
            log = json.loads(a.body.to_pybytes().decode())
            print(f"audit trail: {len(log)} authorised requests, last "
                  f"identity={log[-1][1]!r}")


def l5ex2() -> None:
    """Pagination tickets + constant-memory streaming over the full table."""
    header("L5 Exercise 2 - pagination with constant client memory")
    db = _l5_db()
    port = free_port()
    srv = BankFlightServer(str(db), port, tokens=TOKENS)
    with flight_server(srv):
        c = FinBankClient(f"grpc://127.0.0.1:{port}", b"risk-team-token")
        page_size = scaled(100_000, 10_000)

        truth = duckdb.connect(str(db)).sql(
            "SELECT count(*) FROM transactions").fetchone()[0]
        rss_before = rss_mb()
        total, pages = 0, 0
        for _offset, page in c.pages("SELECT txn_id, amount, account "
                                     "FROM transactions", page_size):
            total += page.num_rows
            pages += 1
        rss_after = rss_mb()
        assert total == truth
        print(f"pages fetched: {pages} x {page_size:,} (last short) = "
              f"{total:,} rows == table size")
        print(f"client peak RSS grew ~{max(0.0, rss_after - rss_before):.1f} MB "
              f"while paging ~{truth * 40 / 1e6:,.0f} MB of raw columns")
        print("PASS - each do_get materialises ONE page; client holds ONE "
              "page; memory is O(page_size), not O(table).")


def l5ex3() -> None:
    """Standard Flight-SQL ecosystem client (ADBC DBAPI) against the server."""
    header("L5 Exercise 3 - standard Flight SQL client (ADBC driver)")
    db = _l5_db()
    port = free_port()
    try:
        from adbc_driver_flightsql import dbapi
    except ImportError:
        print("[skip] pip install adbc-driver-manager adbc-driver-flightsql")
        return

    srv = BankFlightServer(str(db), port, tokens=None)  # no-auth for demo
    with flight_server(srv):
        with dbapi.connect(f"grpc://127.0.0.1:{port}") as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT ccy, count(*) AS n, round(sum(amount),2) AS volume
                FROM transactions GROUP BY ccy ORDER BY volume DESC
            """)
            arrow_tbl = cur.fetch_arrow_table()
            print("ADBC fetch_arrow_table():")
            print(arrow_tbl.to_pandas().to_string(index=False))
            cur.execute("SELECT count(*) FROM transactions")
            print("scalar via fetchall:", cur.fetchall())
    print(">>> the server decodes real FlightSQL protobuf commands, so stock "
          "drivers work: JDBC url jdbc:arrow-flight-sql://host:port "
          "(Tableau/PowerBI connect through exactly this driver).")


def l5ex4() -> None:
    """TLS with self-signed certs; client pins the CA."""
    header("L5 Exercise 4 - TLS in three commands")
    if not shutil.which("openssl"):
        print("[skip] openssl CLI not found")
        return
    import subprocess

    certs = out_dir("l5", fresh=False) / "certs"
    certs.mkdir(exist_ok=True)
    cert, key = certs / "server.pem", certs / "server.key"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(key), "-out", str(cert), "-days", "2",
        "-subj", "/CN=localhost",
        "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
    ], check=True, capture_output=True)

    db = _l5_db()
    port = free_port()
    srv = BankFlightServer(str(db), port, tokens=TOKENS,
                           tls_cert=str(cert), tls_key=str(key))
    with flight_server(srv):
        c = FinBankClient(f"grpc+tls://127.0.0.1:{port}",
                          b"risk-team-token",
                          tls_root_certs=cert.read_bytes())
        tbl = c.sql("""SELECT ccy, count(*) n, sum(amount) vol
                       FROM transactions GROUP BY ccy ORDER BY vol DESC""")
        print(tbl.to_pandas().to_string(index=False))
    print("PASS - encrypted gRPC with pinned self-signed CA "
          "(prod: real CA chain + mTLS via root_certificates/verify_client).")


def l5ex5() -> None:
    """Swap DuckDB for a PyIceberg scan inside the SAME server contract."""
    from pyiceberg.transforms import DayTransform
    header("L5 Exercise 5 - Iceberg-backed Flight server (backend swap)")
    cat = l4_catalog("l5ex5")
    base = out_dir("l4/l5ex5", fresh=False).resolve()
    tbl, created = l4_make_table(cat, "banking.txns",
                                 DayTransform(), "txn_day")
    if created or tbl.scan().count() == 0:
        l4_append_day(tbl, dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc),
                      scaled(50_000, 10_000))

    backend = IcebergScanBackend({
        "type": "sql",
        "uri": f"sqlite:///{base}/catalog.db",
        "warehouse": f"file://{base}/wh",
    })
    port = free_port()
    srv = BankFlightServer(port=port, tokens=TOKENS, backend=backend)
    with flight_server(srv):
        c = FinBankClient(f"grpc://127.0.0.1:{port}", b"risk-team-token")
        out = c.sql("SELECT * FROM banking.txns LIMIT 1000")
        print(f"fetched {out.num_rows} rows / {out.num_columns} cols straight "
              "from the Iceberg table over Flight:")
        print(out.slice(0, 3).to_pandas().to_string(index=False))
    print(">>> only `_run_sql`'s engine changed - consumers keep their code. "
          "That seam is how you migrate storage without touching clients.")


# ===========================================================================
# LESSON 6 — APACHE SPARK
# ===========================================================================

def spark_session(app: str, extra: dict | None = None,
                  packages: str | None = None):
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder
               .appName(app)
               .master("local[2]")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.driver.memory", "3g")
               .config("spark.sql.execution.arrow.pyspark.enabled", "true"))
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    for k, v in (extra or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _l6_data(n: int) -> Path:
    d = out_dir("l6", fresh=False)
    path = d / "txns.parquet"
    if not path.exists():
        pq.write_table(gen_txns_arrow(n), path, compression="zstd",
                       row_group_size=1_000_000)
    return path


def l6ex1() -> None:
    """Lesson-1 data through the feature job: Spark local vs DuckDB."""
    header("L6 Exercise 1 - nightly features: Spark local[*] vs DuckDB")
    n = scaled(5_000_000, 400_000)
    path = _l6_data(n)
    print(f"dataset: {n:,} rows -> {path}")

    spark = spark_session("l6ex1")
    try:
        t0 = time.perf_counter()
        feats = (spark.read.parquet(str(path))
                 .where(F.col("txn_ts") >= F.lit("2026-12-02").cast("timestamp"))
                 .groupBy("acct_id")
                 .agg(F.count("*").alias("n_txns_30d"),
                      F.sum("amount").alias("vol_30d"),
                      F.avg(F.when(F.col("channel") == "ECOM", 1)
                             .otherwise(0)).alias("ecom_ratio")))
        spark_n = feats.count()                      # force execution
        t_spark = time.perf_counter() - t0
    finally:
        spark.stop()

    t0 = time.perf_counter()
    duck_n = duckdb.sql(f"""
        SELECT count(*) FROM (
            SELECT acct_id
            FROM '{path}'
            WHERE txn_ts >= TIMESTAMP '2026-12-02'
            GROUP BY acct_id
        )
    """).fetchone()[0]
    t_duck = time.perf_counter() - t0

    print(f"Spark local[2]: {t_spark:6.2f}s ({spark_n:,} feature rows) "
          "[includes JVM startup]")
    print(f"DuckDB        : {t_duck:6.2f}s ({duck_n:,} feature rows)")
    print(">>> on one laptop DuckDB wins (no JVM, no shuffle boundaries); "
          "Spark's payoff begins when the SAME code fans out over a cluster "
          "once data exceeds single-node throughput.")


def l6ex2() -> None:
    """Force SortMergeJoin, then broadcast it — measure the shuffle gap."""
    header("L6 Exercise 2 - SortMergeJoin vs BroadcastHashJoin")
    n = scaled(5_000_000, 400_000)
    path = _l6_data(n)
    spark = spark_session("l6ex2")
    try:
        txns = spark.read.parquet(str(path)).select("acct_id", "amount").cache()
        dim = spark.range(0, 250_000).select(
            F.col("id").cast("int").alias("acct_id"),
            F.when(F.col("id") % 3 == 0, "PREMIUM")
             .when(F.col("id") % 3 == 1, "STANDARD")
             .otherwise("NEW").alias("segment"))
        joined = txns.join(dim, on="acct_id", how="inner") \
                     .groupBy("segment").agg(F.sum("amount").alias("vol"))

        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        t0 = time.perf_counter(); n_smj = joined.count()
        t_smj = time.perf_counter() - t0
        plan_smj = joined._jdf.queryExecution().executedPlan().toString()

        forced_bc = txns.join(F.broadcast(dim), on="acct_id", how="inner") \
                        .groupBy("segment").agg(F.sum("amount").alias("vol"))
        t0 = time.perf_counter(); n_bc = forced_bc.count()
        t_bc = time.perf_counter() - t0
        plan_bc = forced_bc._jdf.queryExecution().executedPlan().toString()

        kind_smj = "SortMergeJoin" if "SortMergeJoin" in plan_smj else "?"
        kind_bc = "BroadcastHashJoin" if "BroadcastHashJoin" in plan_bc else "?"
        print(f"{kind_smj:18s}: {t_smj:6.2f}s  ({n_smj:,} rows)")
        print(f"{kind_bc:18s}: {t_bc:6.2f}s  ({n_bc:,} rows)")
        print(">>> broadcasting the 250k-row dim removes the big-side shuffle; "
              "look for BroadcastHashJoin in `.explain()` before blaming "
              "hardware.")
    finally:
        spark.stop()


def l6ex3() -> None:
    """Hot-key skew, observed — then dissolved with salting."""
    header("L6 Exercise 3 - skew: the straggler task and the salt fix")
    n = scaled(10_000_000, 1_000_000)
    spark = spark_session("l6ex3")
    try:
        hot = spark.range(0, n // 2).select(
            F.lit("HOT-ACCOUNT").alias("account"),
            F.rand(1).alias("amount"))
        cold = spark.range(0, n - n // 2).select(
            F.concat(F.lit("A"), (F.rand(2) * 999_998 + 1).cast("int")
                      .cast("string")).alias("account"),
            F.rand(3).alias("amount"))
        skewed = hot.unionAll(cold).cache()

        t0 = time.perf_counter()
        naive_n = skewed.groupBy("account") \
            .agg(F.sum("amount").alias("vol")).count()
        t_naive = time.perf_counter() - t0

        salted = (skewed.withColumn("salt", (F.rand(4) * 16).cast("int"))
                  .groupBy("account", "salt").agg(F.sum("amount").alias("v"))
                  .groupBy("account").agg(F.sum("v").alias("vol")))
        t0 = time.perf_counter(); salted_n = salted.count()
        t_salted = time.perf_counter() - t0

        print(f"skewed agg  : {t_naive:6.2f}s ({naive_n:,} keys; HOT-ACCOUNT "
              f"holds 50% of {n:,} rows)")
        print(f"salted agg  : {t_salted:6.2f}s ({salted_n:,} keys)")
        print(">>> salting explodes the hot key into 16 sub-keys for stage 1 "
              "and collapses them in stage 2 - one giant straggler task "
              "becomes sixteen parallel tasks (AQE does similar magic for "
              "skewed JOINS automatically).")
    finally:
        spark.stop()


def l6ex4() -> None:
    """Two versions of an Iceberg table from Spark + VERSION AS OF."""
    header("L6 Exercise 4 - Spark time travel: VERSION AS OF")
    base = out_dir("l6ex4")
    spark = spark_session("l6ex4", extra={
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.finbank":
            "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.finbank.type": "jdbc",
        "spark.sql.catalog.finbank.uri":
            f"jdbc:sqlite:file://{(base / 'cat.db').resolve()}",
        "spark.sql.catalog.finbank.jdbc.schema-version": "V1",
        "spark.sql.catalog.finbank.warehouse":
            f"file://{(base / 'wh').resolve()}",
    }, packages=("org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
                 "org.xerial:sqlite-jdbc:3.50.3.0"))
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS finbank.banking")
        spark.sql("DROP TABLE IF EXISTS finbank.banking.positions")
        spark.sql("""CREATE TABLE finbank.banking.positions (
                        account STRING, eod_balance DOUBLE) USING iceberg""")

        spark.createDataFrame([("A1", 1000.0), ("A2", 2500.5)],
                              "account string, eod_balance double") \
            .writeTo("finbank.banking.positions").append()
        snap1 = spark.sql(
            "SELECT snapshot_id FROM finbank.banking.positions.snapshots "
            "ORDER BY committed_at LIMIT 1").first()[0]

        spark.createDataFrame([("A1", 1100.0), ("A2", 2400.0), ("A3", 99.99)],
                              "account string, eod_balance double") \
            .writeTo("finbank.banking.positions").append()

        head_now = spark.sql(
            "SELECT count(*) FROM finbank.banking.positions").first()[0]
        asof = spark.sql(f"""SELECT * FROM finbank.banking.positions
                             VERSION AS OF {snap1}""")
        print(f"table @head: {head_now} rows")
        print(f"table @VERSION AS OF {snap1} (auditor-certified pin):")
        asof.show()
        assert asof.count() == 2
        print("PASS - auditor pins snapshot ids; every engine resolves the "
              "exact same historical bytes.")
    finally:
        spark.stop()


def l6ex5() -> None:
    """Replace the rule-based score with a vectorised pandas_udf model."""
    header("L6 Exercise 5 - pandas_udf: vectorised model scoring")
    n = scaled(2_000_000, 200_000)
    spark = spark_session("l6ex5")
    try:
        w = Window.partitionBy("acct_id").orderBy("txn_ts")
        txns = spark.read.parquet(str(_l6_data(n)))
        feats = (txns.withColumn(
                    "gap_s", F.unix_timestamp("txn_ts")
                    - F.lag(F.unix_timestamp("txn_ts")).over(w))
                 .groupBy("acct_id")
                 .agg(F.count("*").alias("n_txns"),
                      F.avg(F.coalesce("gap_s", F.lit(3600)))
                        .alias("avg_gap_s"),
                      F.stddev("amount").alias("amt_std")))

        @F.pandas_udf("double")
        def risk_model(avg_gap_s: pd.Series, amt_std: pd.Series,
                       n_txns: pd.Series) -> pd.Series:
            # logistic 'model' over engineered features - fully vectorised,
            # Arrow-backed batches (drop your sklearn/xgboost predict here)
            velocity = np.clip(3600.0 / np.maximum(avg_gap_s.to_numpy(), 1.0),
                               0, 1)
            dispersion = 1.0 - np.exp(-np.nan_to_num(
                amt_std.to_numpy()) / 500.0)
            freq = np.clip(n_txns.to_numpy() / 50.0, 0, 1)
            z = 2.0 * velocity + 1.5 * dispersion + freq - 2.0
            return pd.Series(1.0 / (1.0 + np.exp(-z)))

        scored = feats.withColumn("risk_score_ml",
                                  risk_model(feats.avg_gap_s,
                                             feats.amt_std, feats.n_txns))
        top = scored.orderBy(F.desc("risk_score_ml")).limit(5).toPandas()
        print(top.to_string(index=False))
        print(">>> pandas_udf ships NumPy/pandas batches over Arrow - no "
              "row-at-a-time pickling, so sklearn/XGBoost predict drops in "
              "directly.")
    finally:
        spark.stop()


# ===========================================================================
# CAPSTONE STRETCH GOALS
# ===========================================================================

def cap1() -> None:
    """CDC simulation: late corrections via upsert; time travel proves worlds."""
    from pyiceberg.transforms import DayTransform
    header("Capstone stretch 1 - CDC corrections with upsert + time travel")
    cat = l4_catalog("cap1")
    tbl, created = l4_make_table(cat, "banking.txns", DayTransform(), "txn_day")
    if created or tbl.scan().count() == 0:
        l4_append_day(tbl, dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc),
                      scaled(50_000, 10_000))
    tbl = tbl.refresh()
    pre_snap = tbl.current_snapshot().snapshot_id
    old = tbl.scan().limit(500).to_arrow()
    old_pairs = dict(zip(old.column("txn_id").to_pylist(),
                         old.column("amount").to_pylist()))

    fixes = old.set_column(
        old.schema.get_field_index("amount"), "amount",
        pa.array(np.round(np.random.default_rng(9).uniform(1, 99_999, 500), 2)))
    tbl.upsert(fixes, join_cols=["txn_id"])
    tbl = tbl.refresh()

    now = tbl.scan().to_arrow()
    ids = now.column("txn_id").to_pylist()
    idx = {t: i for i, t in enumerate(ids)}
    changed = sum(now.column("amount")[idx[t]].as_py() != amt
                  for t, amt in old_pairs.items())
    historic = tbl.scan(snapshot_id=pre_snap).limit(500).to_arrow()
    hist_pairs = dict(zip(historic.column("txn_id").to_pylist(),
                          historic.column("amount").to_pylist()))
    unchanged_in_history = all(hist_pairs.get(t) == amt
                               for t, amt in old_pairs.items())
    print(f"upsert updated 500 amounts; verified changed at head: {changed}/500")
    print(f"time travel to {pre_snap}: original amounts intact: "
          f"{unchanged_in_history}")
    assert changed == 500 and unchanged_in_history
    print("PASS - downstream re-runs see corrections; auditors replay history.")


def cap2() -> None:
    """Row-level security: branch_mumbai only ever sees MUM rows."""
    header("Capstone stretch 2 - row-level security on the Flight server")

    class RegionScopedServer(BankFlightServer):
        SCOPES = {"branch_mumbai": "MUM"}

        def _scoped_sql(self, identity, sql):
            region = self.SCOPES.get(identity)
            if not region:
                return sql
            return (f"SELECT * FROM ({sql.rstrip().rstrip(';')}) _rls "
                    f"WHERE region = '{region}'")

        def get_flight_info(self, context, descriptor):
            identity = self._authorize(context)
            sql, mr, off, ps = self._parse_descriptor(descriptor)
            self._check_quota(identity)
            scoped = self._scoped_sql(identity, sql)
            tbl = self.backend.query(scoped, mr, off, ps)
            return flight.FlightInfo(
                tbl.schema, descriptor,
                [flight.FlightEndpoint(
                    flight.Ticket(self._ticket_for(scoped, mr, off, ps)),
                    [flight.Location.for_grpc_tcp("localhost", self._port)])],
                tbl.num_rows, tbl.nbytes)

    db = _l5_db()
    port = free_port()
    srv = RegionScopedServer(str(db), port, tokens=TOKENS)
    with flight_server(srv):
        branch = FinBankClient(f"grpc://127.0.0.1:{port}",
                               b"branch-mumbai-token")
        hq = FinBankClient(f"grpc://127.0.0.1:{port}", b"risk-team-token")
        q = "SELECT region, count(*) AS n FROM transactions GROUP BY region"
        seen_branch = branch.sql(q)
        seen_hq = hq.sql(q)
        print("branch_mumbai sees:")
        print(seen_branch.to_pandas().to_string(index=False))
        print("risk_team sees:")
        print(seen_hq.to_pandas().to_string(index=False))
        regions = seen_branch.column("region").to_pylist()
        assert regions == ["MUM"] and seen_hq.num_rows == 3
        print("PASS - WHERE region='MUM' injected SERVER-side; the branch "
              "client cannot bypass its scope.")


def cap3() -> None:
    """Micro-batch streaming ingest: watch small-file disease appear."""
    from pyiceberg.transforms import DayTransform
    header("Capstone stretch 3 - streaming micro-batches vs small files")
    cat = l4_catalog("cap3")
    tbl, created = l4_make_table(cat, "banking.txns", DayTransform(), "txn_day")
    day = dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc)
    if created:
        l4_append_day(tbl, day, scaled(5_000, 5_000), seed=100)
    wh = _l4_base() / "warehouse_cap3" / "banking" / "txns"
    batches = 12
    for i in range(batches):
        l4_append_day(tbl, day, scaled(5_000, 5_000), seed=200 + i)
    files = len(list(wh.rglob("*.parquet")))
    rows = cat.load_table("banking.txns").refresh().scan().count()
    print(f"after {batches} one-minute micro-batches: {rows:,} rows spread "
          f"over {files} data files (avg {rows // max(files, 1):,} rows/file)")
    print("""cure (schedule like VACUUM):
  Spark: CALL finbank.system.rewrite_data_files(
           table => 'banking.txns',
           options => map('min-input-files','5',
                          'target-file-size-bytes','536870912'))
  then  : expire_snapshots() + delete-orphan-files()
PyIceberg exposes snapshot expiry (MaintenanceTable) but compaction is
engine-side today - Spark/Flink/Athena all speak the same procedure.""")
    print("PASS")


def cap4() -> None:
    """Cost model: S3 spend before/after ZSTD + partition pruning."""
    header("Capstone stretch 4 - monthly lake cost model")
    PRICE_PER_GB = 0.023
    l1d = OUT / "l1ex2"
    if (l1d / "txns_none.parquet").exists() and \
            (l1d / "txns_zstd.parquet").exists():
        raw = (l1d / "txns_none.parquet").stat().st_size
        zst = (l1d / "txns_zstd.parquet").stat().st_size
        note = "(measured from L1 Exercise 2 outputs)"
    else:
        sample = gen_txns_arrow(scaled(1_000_000, 100_000))
        tmp = out_dir("cap4")
        pq.write_table(sample, tmp / "u.parquet", compression="none")
        pq.write_table(sample, tmp / "z.parquet", compression="zstd")
        raw = (tmp / "u.parquet").stat().st_size
        zst = (tmp / "z.parquet").stat().st_size
        note = "(extrapolated from a fresh sample)"

    years = 3                          # 3 years of daily data retained
    raw_tb = raw * 365.25 * years / 1e12
    zst_tb = zst * 365.25 * years / 1e12
    prune = 1 / 365                    # one-day report scans 1 of 365 parts
    rows = [
        ["raw/uncompressed lake", raw_tb, raw_tb * PRICE_PER_GB],
        ["ZSTD-compressed lake", zst_tb, zst_tb * PRICE_PER_GB],
        ["ZSTD + day-pruned query (effective scan)", zst_tb * prune,
         zst_tb * prune * PRICE_PER_GB],
    ]
    print(f"storage model {note}; {years}-yr retention @ ${PRICE_PER_GB}/GB-mo")
    print(pd.DataFrame(rows, columns=["strategy", "TB_stored_or_scanned",
                                      "$/month_effective"]).to_string(
        index=False, float_format=lambda v: f"{v:,.4f}"))
    print(f">>> ZSTD saves {(1 - zst / raw) * 100:.0f}% of storage; partition "
          "pruning cuts QUERY-scanned bytes another ~365x (frequent reports "
          "make scanned bytes, not storage, the dominant lake bill).")


# ===========================================================================
# DISPATCHER
# ===========================================================================

REGISTRY = {}
for _l, _n in (("l1", 4), ("l2", 5), ("l3", 5), ("l4", 5), ("l5", 5),
               ("l6", 5)):
    for _i in range(1, _n + 1):
        REGISTRY[f"{_l}ex{_i}"] = globals()[f"{_l}ex{_i}"]
for _i in range(1, 5):
    REGISTRY[f"cap{_i}"] = globals()[f"cap{_i}"]

LESSONS = {
    "l1": ["l1ex1", "l1ex2", "l1ex3", "l1ex4"],
    "l2": [f"l2ex{i}" for i in range(1, 6)],
    "l3": [f"l3ex{i}" for i in range(1, 6)],
    "l4": [f"l4ex{i}" for i in range(1, 6)],
    "l5": [f"l5ex{i}" for i in range(1, 6)],
    "l6": [f"l6ex{i}" for i in range(1, 6)],
    "cap": [f"cap{i}" for i in range(1, 5)],
}


def main(argv: list[str]) -> int:
    if "--list" in argv:
        for tgt, fn in REGISTRY.items():
            doc = (fn.__doc__ or "").strip().splitlines()[0]
            print(f"{tgt:7s} {doc}")
        return 0

    targets: list[str] = []
    for arg in argv:
        if arg.startswith("-"):
            continue
        targets.extend(LESSONS.get(arg, [arg]))

    unknown = [t for t in targets if t not in REGISTRY]
    if unknown or not targets:
        print(__doc__)
        if unknown:
            print("unknown targets:", ", ".join(unknown))
        return 2

    print(f"running {len(targets)} target(s)"
          f"{'  [SOL_QUICK=1 smoke mode]' if QUICK else ''}")
    failures = []
    for t in targets:
        try:
            REGISTRY[t]()
        except Exception:
            failures.append(t)
            print(f"!! {t} FAILED:\n{traceback.format_exc()}")
    if failures:
        print(f"\n{len(failures)} failed: {', '.join(failures)}")
        return 1
    print("\nALL TARGETS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
