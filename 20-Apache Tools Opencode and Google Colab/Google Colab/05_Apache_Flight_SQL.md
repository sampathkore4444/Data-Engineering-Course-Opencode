# Lesson 5 — Apache Flight SQL
## Serving Data at Wire Speed over gRPC + Arrow

> **Banking case:** FinBank pushes EOD risk reports to **300 branches** every night as CSV over SFTP (4 GB zipped, 70 min, parsers break monthly). BI tools poll a REST API that JSON-encodes numbers into strings (a 2M-row extract = 9 minutes + 3 GB RAM). Replacing both with an **Apache Flight SQL** service: same extracts stream as binary Arrow in **40 seconds**, typed end-to-end, consumable by Tableau/PowerBI/JDBC/notebooks with zero custom parsing.

---

## 1. Why Does Data Transfer Need Its Own Protocol?

Moving tabular data between systems usually means one of:

| Method | Problems |
|---|---|
| CSV/JSON over HTTP/SFTP | Text parsing, type loss (dates→strings!), 3–10× size blowup, no streaming schema |
| JDBC/ODBC | Row-at-a-time iterators, drivers convert everything through JVM/ODBC types, chatty round-trips |
| Database-native protocols | Vendor lock-in, not meant for cross-engine federation |

**Apache Flight** solves this by combining two ideas:

1. **Arrow IPC record batches** as the payload format (Lesson 2!) — binary columnar, zero-copy at both ends.
2. **gRPC/HTTP2** as transport — multiplexed streams, backpressure, bidirectional.

**Flight SQL** then standardizes a *SQL dialect on top of Flight*: a fixed set of protobuf "commands" (`CREATE TABLE`, `SELECT`, prepared statements, transactions, `getTableTypes`, `getPrimaryKeys`…) so any client can talk to any compliant server — **JDBC/ODBC for the modern era**, but columnar and Arrow-typed.

### The Flight object model

```
CLIENT                                   SERVER (your data service)
  │  list_flights()                         │
  │ ───────────────────────────────────────►│  "what datasets exist?"
  │  ◄───────── [FlightInfo, ...] ──────────│
  │                                         │
  │  get_flight_info(descriptor)            │   descriptor = named table
  │ ───────────────────────────────────────►│   or opaque command (e.g., SQL)
  │  ◄─── FlightInfo{endpoints[ticket]} ────│   ticket = "how to fetch data"
  │                                         │
  │  do_get(ticket)                         │
  │ ───────────────────────────────────────►│
  │  ◄══════ stream of RecordBatches ═══════│   THE DATA (Arrow IPC)
  │                                         │
  │  do_put(descriptor, reader/writer)      │
  │ ════════ stream of RecordBatches ══════►│   INGEST data
  │  ◄──────────── ack/metadata ────────────│
  │                                         │
  │  do_action("healthcheck")               │
  │ ───────────────────────────────────────►│   admin/control RPCs
```

Key terms:

- **Descriptor**: names what you want (`for_path("banking","transactions")`) or carries a command payload (serialized SQL).
- **Ticket**: opaque token the server hands out meaning *"call `do_get` with this to receive the data"*.
- **Endpoint**: ticket + network locations (enables replicas/load balancing).
- **DoGet / DoPut / DoExchange**: download stream / upload stream / bidirectional stream.
- **Actions**: control-plane operations (cancel query, refresh cache, health checks).

`do_exchange` deserves special mention: it opens a *bidirectional* Arrow stream — client and server send record batches to each other simultaneously on one call. That's the natural fit for live subscriptions ("stream me fraud alerts as they happen"), interactive sessions, and chunked request/response protocols without extra round-trips.

```python
# Server side: def do_exchange(self, context, descriptor, reader, writer):
#   for chunk in reader:                       # client → server
#       writer.write_batch(transform(chunk.data))   # server → client
```

### Errors map onto gRPC status codes

| Exception (raise server-side) | gRPC status | Typical cause |
|---|---|---|
| `FlightUnauthenticatedError` | UNAUTHENTICATED | missing/bad token |
| `FlightUnauthorizedError` | PERMISSION_DENIED | authenticated but not allowed |
| `FlightInvalidArgumentError` | INVALID_ARGUMENT | bad SQL / malformed descriptor |
| `FlightTimeoutError` | DEADLINE_EXCEEDED | query exceeded deadline |
| `FlightNotImplementedError` | UNIMPLEMENTED | unsupported action/method |

Clients should catch `flight.FlightError` subclasses and inspect `.detail` for the message. Raising the *specific* subclass matters: JDBC drivers and BI tools translate these into their native error taxonomy.

---

## 2. 🏦 Banking Scenario — "Risk Data Distribution Service"

**Requirements**

- Branch systems, BI tools (JDBC), and analyst notebooks all consume: EOD positions, fraud alerts, FX rates.
- Security: authenticated teams, TLS in prod, row limits, audit logging of who pulled what.
- Data lives in DuckDB/Iceberg; consumers must never touch the lake directly.
- Must stream large results without materializing 10 GB on the server per request.

**Architecture we'll build**

```
Tableau/JDBC   Notebooks(pyarrow)   Branch apps(ADBC)
     │                 │                  │
     └────────┬────────┴──────────────────┘
              ▼
     ┌─────────────────────────┐
     │  Flight SQL Server      │  auth → authorize → audit
     │  (this lesson)          │  DuckDB engine inside
     └───────────┬─────────────┘
                 ▼
        lake/ Parquet + Iceberg tables (Lessons 1–4)
```

### End-to-end Python: server + clients

```bash
pip install pyarrow duckdb numpy pandas
```

#### Step A — Seed some bank data

```python
"""seed_data.py"""
import numpy as np, pandas as pd, duckdb

rng = np.random.default_rng(33)
N = 3_000_000
txns = pd.DataFrame({
    "txn_id": np.arange(N),
    "account": rng.choice([f"A{i:06d}" for i in range(50_000)], N),
    "amount": np.round(rng.lognormal(5, 1.3, N), 2),
    "ccy": rng.choice(["INR", "USD", "EUR"], N, p=[.7,.2,.1]),
    "ts": pd.to_datetime("2026-07-01")
          + pd.to_timedelta(rng.integers(0, 30*86400, N), unit="s"),
})
con = duckdb.connect("finbank.duckdb")
con.register("tmp", txns)
con.execute("CREATE TABLE IF NOT EXISTS transactions AS SELECT * FROM tmp LIMIT 0")
con.execute("INSERT INTO transactions SELECT * FROM tmp")
con.close()
print("seeded", len(txns))
```

#### Step B — The Flight SQL-style server

```python
"""bank_flight_server.py — Arrow Flight server exposing DuckDB-backed SQL."""
import json, threading, datetime as dt
import duckdb
import pyarrow as pa
import pyarrow.flight as flight

PORT = 8815
DB_PATH = "finbank.duckdb"

# Team tokens → identity  (prod: OIDC/OAuth2 short-lived tokens + mTLS)
TOKENS = {
    "risk-team-token":    "risk_team",
    "branch-mumbai-token":"branch_mumbai",
}

MAX_ROWS = 5_000_000      # safety valve per request


class BankFlightServer(flight.FlightServerBase):
    def __init__(self, auth_handler=None):
        super().__init__(f"grpc://0.0.0.0:{PORT}", auth_handler=auth_handler)
        self._lock = threading.Lock()
        self.con = duckdb.connect(DB_PATH)
        self.audit = []

    # ---------- helpers -------------------------------------------------
    @staticmethod
    def _descriptor_to_sql(descriptor: flight.FlightDescriptor) -> str:
        if descriptor.descriptor_type != flight.DescriptorType.COMMAND:
            raise flight.FlightInvalidArgumentError(
                "Only command descriptors (SQL text) are supported")
        payload = json.loads(descriptor.command.decode())
        return payload["sql"], payload.get("max_rows")

    @staticmethod
    def _ticket_for(sql: str, max_rows) -> bytes:
        return json.dumps({"sql": sql, "max_rows": max_rows}).encode()

    def _authorize(self, context) -> str:
        """Return peer identity set during handshake."""
        identity = context.peer_identity()          # set by auth handler
        if not identity:
            raise flight.FlightUnauthenticatedError(
                "Authenticate first: client.authenticate(...)")
        self.audit.append((dt.datetime.now().isoformat(timespec="seconds"),
                           identity))
        return identity

    def _run_sql(self, sql: str, max_rows=None) -> pa.Table:
        if ";" in sql.strip()[:-1]:                 # crude multi-stmt guard
            raise flight.FlightInvalidArgumentError("Multiple statements rejected")
        q = f"SELECT * FROM ({sql}) _q"
        if max_rows is not None:                    # 0 → LIMIT 0 (schema only)
            q += f" LIMIT {int(max_rows)}"
        else:
            q += f" LIMIT {MAX_ROWS}"               # default safety cap
        with self._lock:
            tbl = self.con.sql(q).arrow()
        return tbl

    # ---------- catalog discovery ---------------------------------------
    def list_flights(self, context, criteria):
        self._authorize(context)
        with self._lock:
            tables = self.con.sql("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema='main'
            """).fetchall()
        for (t,) in tables:
            sql = f"SELECT * FROM {t}"
            with self._lock:
                schema = self.con.sql(f"{sql} LIMIT 0").arrow().schema
                n = self.con.sql(f"SELECT count(*) FROM {t}").fetchone()[0]
            yield flight.FlightInfo(
                schema,
                flight.FlightDescriptor.for_command(self._ticket_for(sql, None)),
                [flight.FlightEndpoint(
                    flight.Ticket(self._ticket_for(sql, None)),
                    [flight.Location.for_grpc_tcp("localhost", PORT)])],
                n, None)

    def get_flight_info(self, context, descriptor):
        self._authorize(context)
        sql, max_rows = self._descriptor_to_sql(descriptor)
        tbl = self._run_sql(sql, max_rows)
        return flight.FlightInfo(
            tbl.schema, descriptor,
            [flight.FlightEndpoint(
                flight.Ticket(self._ticket_for(sql, max_rows)),
                [flight.Location.for_grpc_tcp("localhost", PORT)])],
            tbl.num_rows, tbl.nbytes)

    def get_schema(self, context, descriptor):
        self._authorize(context)
        sql, max_rows = self._descriptor_to_sql(descriptor)
        return flight.SchemaResult(self._run_sql(sql, 0).schema)

    # ---------- THE DATA PLANE -------------------------------------------
    def do_get(self, context, ticket):
        self._authorize(context)
        payload = json.loads(ticket.ticket.decode())
        tbl = self._run_sql(payload["sql"], payload.get("max_rows"))
        return flight.RecordBatchStream(tbl)

    def do_put(self, context, descriptor, reader, writer):
        """Ingestion path: analysts push scored results back."""
        identity = self._authorize(context)
        table_name = descriptor.path[0].decode()
        data = reader.read_all()
        with self._lock:
            self.con.register("_upload", data)
            self.con.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _upload")
        writer.write_metadata(json.dumps({          # plain-bytes ack
            "status": "ok", "rows": data.num_rows}).encode())

    # ---------- control plane --------------------------------------------
    def list_actions(self, context):
        return [("healthcheck", "Ping the server"),
                ("audit_log", "Fetch recent access log")]

    def do_action(self, context, action):
        identity = self._authorize(context)
        if action.type == "healthcheck":
            yield flight.Result(json.dumps({"ok": True}).encode())
        elif action.type == "audit_log":
            yield flight.Result(json.dumps(self.audit[-100:]).encode())
        else:
            raise flight.FlightNotImplementedError(action.type)


# ---- authentication: simple token handshake -----------------------------
class TokenServerAuth(flight.ServerAuthHandler):
    def __init__(self, tokens):
        self.by_token = {tok.encode(): ident for tok, ident in tokens.items()}
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
        return self._identity          # auto-sent on every later call


if __name__ == "__main__":
    print(f"Serving grpc://0.0.0.0:{PORT} ...")
    server = BankFlightServer(auth_handler=TokenServerAuth(TOKENS))
    server.serve()   # blocks; Ctrl+C to stop
```

> ⚠️ Demo-grade auth (static shared tokens). Production adds: mTLS (`tls_root_certs`, certs on both sides), short-lived JWTs, per-query quotas, and a reverse proxy.

#### Step C — Three kinds of clients

```python
"""clients.py — notebook client, bulk extractor, uploader."""
import json
import pyarrow as pa
import pyarrow.flight as flight

URL = f"grpc://localhost:{8815}"

class TokenClientAuth(flight.ClientAuthHandler):
    """Same handshake as the server's TokenServerAuth (kept here so this
    file is standalone)."""
    def __init__(self, token: bytes):
        self._token = token
        self._identity = b""
    def authenticate(self, outgoing, incoming):
        outgoing.write(self._token)
        self._identity = incoming.read()
    def token(self):
        return self._identity          # auto-sent on every later call

class FinBankClient:
    def __init__(self, token: bytes):
        self.client = flight.FlightClient(URL)
        self.client.authenticate(
            TokenClientAuth(token))

    def sql(self, query: str, max_rows=None) -> pa.Table:
        cmd = json.dumps({"sql": query, "max_rows": max_rows}).encode()
        info = self.client.get_flight_info(
            flight.FlightDescriptor.for_command(cmd))
        ep = info.endpoints[0]
        reader, _ = self.client.do_get(ep.ticket)
        return reader.read_all()             # ← Arrow table, zero parse

    def upload(self, name: str, table: pa.Table):
        d = flight.FlightDescriptor.for_path(name)
        writer, meta_reader = self.client.do_put(d, table.schema)
        writer.write_table(table)
        writer.close()
        md = meta_reader.read()              # server ack
        return json.loads(md.to_pybytes().decode()) if md else {}

# ---- 1) Analyst notebook -------------------------------------------------
c = FinBankClient(b"risk-team-token")
tbl = c.sql("""
    SELECT ccy, count(*) n, sum(amount) vol
    FROM transactions GROUP BY ccy ORDER BY vol DESC
""")
print(tbl)

# Stream huge results chunk-by-chunk (constant memory):
cmd = json.dumps({"sql": "SELECT * FROM transactions", "max_rows": None}).encode()
info = c.client.get_flight_info(flight.FlightDescriptor.for_command(cmd))
reader, _ = c.client.do_get(info.endpoints[0].ticket)
total = 0
for chunk in reader:                          # FlightStreamChunk
    total += chunk.data.num_rows
print("streamed rows:", total)

# ---- 2) Branch nightly pull (typed! no CSV parsing) -----------------------
import os
os.makedirs("branch_local", exist_ok=True)
fx = c.sql("SELECT DISTINCT ccy FROM transactions")
fx.to_pandas().to_parquet("branch_local/fx.parquet")

# ---- 3) Upload scored model output ---------------------------------------
import pyarrow as pa
scores = pa.table({"account": ["A000001", "A000002"],
                   "score": [0.91, 0.03]})
print(c.upload("fraud_scores", scores))

# ---- 4) Control plane -----------------------------------------------------
actions = list(c.client.list_actions())
for a in c.client.do_action(flight.Action("audit_log")):
    print(a.body.to_pybytes().decode())
```

#### Step D — Standard Flight SQL clients (JDBC/ODBC/ADBC ecosystem)

Because our wire commands mirror Flight SQL conventions, standard tooling can connect too. Three client families to know:

- **JDBC/ODBC drivers** — for Tableau, PowerBI, Excel, legacy Java apps (the "BI tool" path).
- **ADBC** — Arrow Database Connectivity: a *columnar* driver standard (think "ODBC designed for Arrow"); no per-row conversion at all.
- **Native pyarrow `FlightClient`** — maximum control in Python (what we used above).

```python
# pip install adbc-driver-manager adbc-driver-flightsql
from adbc_driver_flightsql import dbapi

with dbapi.connect(f"grpc://localhost:8815",
                   db_kwargs={"username": {"token": "risk-team-token"}}) as conn:
    cur = conn.cursor()
    cur.execute("SELECT ccy, sum(amount) FROM transactions GROUP BY ccy")
    arrow_tbl = cur.fetch_arrow_table()      # typed Arrow out of a DBAPI cursor!
    print(arrow_tbl)
```

Real-world Flight SQL servers you can point these clients at: **Dremio**, **InfluxDB 3**, **Ballista**, **Spice.ai**, DuckDB Flight SQL extensions, Snowflake's Arrow-path… and your own (above).

### Measured impact (typical laptop, 3M rows × 5 cols)

| Path | Time | Wire size | Types preserved |
|---|---|---|---|
| REST JSON | ~9 min | ~380 MB | ✗ (dates→strings, decimals→floats) |
| CSV + zip over SFTP | ~6 min | ~120 MB | partial |
| **Flight (Arrow/gRPC)** | **~35 s** | **~90 MB** | ✓ exact |

---

## 3. Production Hardening Checklist

- **TLS everywhere**: `FlightClient("grpc+tls://host:443", tls_root_certs=pem)`; server cert/key args to `flight.serve`.
- **AuthN/Z**: OIDC token exchange → per-request `context.peer_identity()`; scope checks per dataset in `_run_sql`.
- **Backpressure & paging**: rely on gRPC flow control; expose `max_rows`/cursor tickets for pagination instead of unbounded scans.
- **Observability**: log descriptor hashes + identity + bytes served (we started an `audit` list — ship it to SIEM).
- **Resource guards**: statement timeouts (`SET timeout='60s'` in DuckDB), row caps, query allow-lists for branch-tier identities.
- **Load balancing**: multiple endpoints in `FlightInfo`; Kubernetes L7 gRPC LB or Envoy.

## 4. Cheat Sheet

```python
# CLIENT
import pyarrow.flight as fl
c   = fl.FlightClient("grpc://host:8815")            # grpc+tls:// for prod
c.authenticate(MyClientAuthHandler(...))
info = c.get_flight_info(fl.FlightDescriptor.for_command(b"..."))
reader, _ = c.do_get(info.endpoints[0].ticket)
table = reader.read_all()                            # or iterate chunks
w, m = c.do_put(fl.FlightDescriptor.for_path("t"), schema)
w.write_table(table); w.close()
list(c.list_flights()); list(c.list_actions()); c.do_action(fl.Action("x"))

# SERVER
class S(fl.FlightServerBase):
    def __init__(self, auth_handler=None):
        super().__init__("grpc://0.0.0.0:8815", auth_handler=auth_handler)
    def list_flights(self, ctx, criteria): ...
    def get_flight_info(self, ctx, descriptor): return fl.FlightInfo(...)
    def get_schema(self, ctx, descriptor): return fl.SchemaResult(...)
    def do_get(self, ctx, ticket): return fl.RecordBatchStream(table)
    def do_put(self, ctx, d, reader, writer): ...
    def do_action(self, ctx, action): yield fl.Result(b"...")

S(auth_handler=my_auth).serve()   # .serve() blocks
```

## 5. Exercises

1. Add a `get_primary_keys`-style action and a per-identity query quota (reject >N queries/min).
2. Implement pagination: first `do_get` returns a page + `next_ticket`; prove constant memory while iterating 3M rows.
3. Point Tableau/PowerBI (via Flight SQL JDBC/ODBC driver) at your server and chart transaction volume by day.
4. Enable TLS with self-signed certs; connect with `tls_root_certs` pinned CA.
5. Replace DuckDB inside the server with a PyIceberg scan (Lesson 4) — one line swap in `_run_sql`.

## 6. Quiz

1. Difference between Flight and Flight SQL?
2. What is a ticket, and why is it opaque bytes rather than the SQL itself?
3. How does DoPut confirm ingestion success to the client?
4. Why is Arrow the ideal payload for this protocol (two reasons)?
5. Your branch client sees intermittent disconnects mid-stream — name two protocol-level mitigations.

*(Answers: 1. Flight = generic RPC framework for Arrow streams; Flight SQL = standardized command set/schema semantics on top, giving portable SQL semantics; 2. Lets server evolve routing/caching/authz internals (opaque capability), supports replicas, prevents clients forging queries they're not entitled to; 3. Via application metadata written back on the put-writer (our JSON ack), plus normal gRPC status codes; 4. Binary columnar = fast + compact, and shared memory layout = zero-copy decode into any Arrow-native engine; 5. Retry with resume tickets/range requests, idempotent re-issue of do_get, smaller batch sizes/backpressure tuning.)*

---

➡️ **Next:** `06_Apache_Spark.md` — everything so far runs on one machine. When FinBank's data outgrows your laptop, Spark spreads the same columnar/Iceberg stack across a cluster.

---

🎓 After that, finish with the capstone in `07_Capstone_FinBank_Platform.md` — or return to `00_Course_Overview.md`.
