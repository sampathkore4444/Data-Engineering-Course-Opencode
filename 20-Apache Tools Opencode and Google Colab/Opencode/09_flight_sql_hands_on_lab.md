# Lesson 09 — Flight SQL Hands-On Lab: A Bank-Grade Query Gateway

> **Meridian Trust Bank case study, Part 9**: Build the thing Lesson 08 described:
> `flight-gateway.meridian.internal` - a real Flight SQL server backed by DuckDB,
> with login handshake, per-role authorization, Arrow ingestion (`do_put`), streaming
> analytics (`do_get`), schema-only contract checks and prepared statements.
> One file, pure PyArrow, runs on your laptop.

---

## 1. What we are building

```
   svc_etl ──do_put(250k rows)──▶┌───────────────────────────────┐
                                 │  Meridian Flight SQL Gateway  │
   risk_analyst ──query─────────▶│  - token handshake            │
      │                          │  - role policy                │◀──▶ Iceberg/Parquet
      │◀═══streamed batches══════│  - JSON command envelopes     │     (Lessons 02-07)
   BI/JDBC (conceptually)       │  - DuckDB engine              │
                                 └───────────────────────────────┘
```

Roles we enforce:

| Principal | Password | May |
|---|---|---|
| `svc_etl` | ingest-me | `do_put` ingestion only |
| `risk_analyst` | quarter-end | query planning + `do_get` reads |

> **Honest engineering note**: the official protocol encodes commands as protobuf
> messages from `flight_sql.proto`, packed into descriptor/ticket/action payloads
> (PyArrow itself only ships the RPC layer - see Lesson 08). We substitute compact
> **JSON envelopes** with identical semantics so everything is runnable with pyarrow
> alone. Production clients (ADBC Flight SQL driver, JDBC) would need the protobuf
> encoding; swapping our envelope for theirs touches exactly three functions.

## 2. End-to-end example

Save as `meridian_flight_gateway.py` and run it - it starts the server in a thread,
then acts as both clients.

```python
"""
meridian_flight_gateway.py
A miniature Flight SQL gateway over DuckDB - Meridian Trust Bank edition.
Server + client in one file; pure PyArrow Flight RPC implementing
Flight-SQL semantics (JSON command envelopes instead of protobuf).

Deps: pip install pyarrow duckdb numpy
Run:  python3 meridian_flight_gateway.py
"""
import json
import os
import shutil
import threading
import time

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.flight as fl

USERS = {                       # principal -> password (demo-grade!)
    "risk_analyst": "quarter-end",
    "svc_etl":      "ingest-me",
}
_TOKENS = {pw: user for user, pw in USERS.items()}
_PORT = 31350


# --------------------------------------------------------------- envelopes ---
# Real Flight SQL packs protobuf messages from flight_sql.proto into
# descriptors/tickets/actions. We use JSON envelopes with the same shape so
# everything runs on pyarrow alone.
def enc(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()

def dec(raw) -> dict:
    return json.loads(bytes(raw).decode())


class TokenHandler(fl.ServerAuthHandler):
    """Handshake 'user:password' -> token; every later call carries the token."""

    def authenticate(self, outgoing, incoming):
        user, _, pw = incoming.read().decode().partition(":")
        if USERS.get(user) != pw:
            raise fl.FlightUnauthenticatedError(f"unknown principal {user!r}")
        outgoing.write(enc({"user": user, "token": pw}))

    def is_valid(self, token):
        try:
            payload = dec(token)
            if _TOKENS.get(payload["token"]) != payload["user"]:
                raise KeyError
        except Exception:
            raise fl.FlightUnauthenticatedError("stale/invalid token")
        return payload["user"].encode()


class Login(fl.ClientAuthHandler):
    def __init__(self, user, password):
        self._creds = f"{user}:{password}".encode()
        self.token = b""

    def authenticate(self, outgoing, incoming):
        outgoing.write(self._creds)
        self.token = incoming.read()

    def get_token(self):                    # auto-attached to every call
        return self.token


class MeridianFlightSqlServer(fl.FlightServerBase):

    def __init__(self, location, auth_handler, warehouse="/tmp/opencode/wh"):
        super().__init__(location, auth_handler=auth_handler)
        self._location = location
        os.makedirs(warehouse, exist_ok=True)
        self.con = duckdb.connect(f"{warehouse}/meridian.duckdb")
        self.prepared = {}                  # handle(str) -> sql(str)

    # ---- policy: who may read / who may ingest --------------------------------
    def _require(self, ctx, verb):
        allowed = {"read": {"risk_analyst"}, "ingest": {"svc_etl"}}[verb]
        user = ctx.peer_identity().decode() or "<anonymous>"
        if user not in allowed:
            raise fl.FlightUnauthorizedError(f"{user!r} may not {verb}")

    def _resolve(self, cmd):
        if cmd.get("handle"):               # prepared statement path
            return self.prepared[cmd["handle"]]
        return cmd["sql"]

    def _endpoint(self, ticket):
        return [fl.FlightEndpoint(ticket, [self._location])]

    # ---- CommandStatementQuery / CommandPreparedStatementQuery -----------------
    def get_flight_info(self, context, descriptor):
        sql = self._resolve(dec(descriptor.command))
        self._require(context, "read")
        schema = self.con.sql(sql).arrow().schema   # plan only - no data moved
        ticket = fl.Ticket(enc({"sql": sql}))
        return fl.FlightInfo(schema, descriptor,
                             self._endpoint(ticket), -1, -1)

    def get_schema(self, context, descriptor):
        """Schema-only contract check: regulators love it, bandwidth hates it."""
        sql = self._resolve(dec(descriptor.command))
        self._require(context, "read")
        return fl.SchemaResult(self.con.sql(sql).arrow().schema)

    # ---- DoGet: stream the result set batch by batch ----------------------------
    def do_get(self, context, ticket):
        sql = dec(ticket.ticket)["sql"]
        self._require(context, "read")
        reader = self.con.execute(sql).to_arrow_reader(4096)
        return fl.GeneratorStream(reader.schema, self._batches(reader))

    @staticmethod
    def _batches(reader):
        while True:
            try:
                yield reader.read_next_batch()
            except StopIteration:
                return

    # ---- DoPut: Arrow ingestion path ---------------------------------------------
    def do_put(self, context, descriptor, reader, writer):
        table_name = b"/".join(descriptor.path).decode()
        self._require(context, "ingest")
        arrow_table = reader.read_all()
        self.con.register("incoming", arrow_table)
        self.con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM incoming")
        writer.write(enc({"rows_written": arrow_table.num_rows}))

    # ---- actions = prepared statements & ops ---------------------------------------
    def list_actions(self, context):
        return [("CreatePreparedStatement", "compile SQL -> opaque handle"),
                ("ClosePreparedStatement",  "release a handle"),
                ("healthcheck",             "liveness probe")]

    def do_action(self, context, action):
        if action.type == "CreatePreparedStatement":
            sql = dec(action.body)["sql"]
            handle = f"stmt_{abs(hash(sql)) % 10**8}"
            self.prepared[handle] = sql
            yield fl.Result(handle.encode())
        elif action.type == "ClosePreparedStatement":
            self.prepared.pop(dec(action.body)["handle"], None)
        elif action.type == "healthcheck":
            yield fl.Result(b"ok")


# ============================== CLIENT DEMO ===================================
def make_card_txns(n=250_000):
    rng = np.random.default_rng(7)
    return pa.Table.from_pydict({
        "txn_id":   np.arange(n, dtype="int64"),
        "card_id":  rng.integers(300_000, 310_000, n),
        "amount":   np.round(rng.gamma(2.0, 45.0, n), 2),
        "currency": rng.choice(["EUR", "GBP"], n),
        "flag":     rng.random(n) < 0.01,
    })


def main():
    shutil.rmtree("/tmp/opencode/wh", ignore_errors=True)

    server = MeridianFlightSqlServer(f"grpc://127.0.0.1:{_PORT}", TokenHandler())
    threading.Thread(target=server.serve, daemon=True).start()
    time.sleep(0.5)

    client = fl.FlightClient(f"grpc://127.0.0.1:{_PORT}")

    # -- 1. ETL principal ingests 250k card transactions --------------------------
    client.authenticate(Login("svc_etl", "ingest-me"))
    txns = make_card_txns()
    writer, meta = client.do_put(
        fl.FlightDescriptor.for_path("card_txns"), txns.schema)
    writer.write_table(txns, max_chunksize=32_768)     # ~8 streamed batches
    writer.done_writing()                              # MUST precede ack read
    print("ingest ack:", dec(meta.read()))
    writer.close()

    # -- 2. analyst logs in; role check blocks ETL principal from reading ----------
    client.authenticate(Login("risk_analyst", "quarter-end"))
    etl_client = fl.FlightClient(f"grpc://127.0.0.1:{_PORT}")
    etl_client.authenticate(Login("svc_etl", "ingest-me"))
    try:
        etl_client.get_flight_info(fl.FlightDescriptor.for_command(
            enc({"sql": "SELECT 1"})))
    except fl.FlightUnauthorizedError:
        print("policy enforced: svc_etl may not read")

    # -- 3. two-phase query: plan, then stream -------------------------------------
    query = ("SELECT currency, count(*) AS n, round(sum(amount), 2) AS total "
             "FROM card_txns GROUP BY currency ORDER BY currency")
    info = client.get_flight_info(
        fl.FlightDescriptor.for_command(enc({"sql": query})))
    print("planned schema:", [f.name for f in info.schema])

    big_query = "SELECT txn_id, card_id, amount FROM card_txns WHERE amount > 100"
    big_info = client.get_flight_info(
        fl.FlightDescriptor.for_command(enc({"sql": big_query})))
    reader = client.do_get(big_info.endpoints[0].ticket)
    batches = []
    while True:                                    # batch-by-batch arrival
        try:
            batches.append(reader.read_chunk().data)
        except StopIteration:
            break
    flagged = pa.Table.from_batches(batches)
    print(f"streamed {len(batches)} chunks -> {flagged.num_rows} rows")

    table = client.do_get(info.endpoints[0].ticket).read_all()
    print(table.to_pandas().to_string(index=False))

    # -- 4. schema-only round trip (contract testing, zero data) --------------------
    sr = client.get_schema(
        fl.FlightDescriptor.for_command(enc({"sql": query})))
    assert sr.schema.names == ["currency", "n", "total"]
    print("schema-only OK:", sr.schema.names)

    # -- 5. prepared statement lifecycle ----------------------------------------------
    stmt = ("SELECT card_id, count(*) AS hits FROM card_txns "
            "WHERE flag GROUP BY card_id ORDER BY hits DESC LIMIT 3")
    handle = next(client.do_action(fl.Action(
        "CreatePreparedStatement", enc({"sql": stmt})))).body.to_pybytes()
    pinfo = client.get_flight_info(
        fl.FlightDescriptor.for_command(enc({"handle": handle.decode()})))
    top_cards = client.do_get(pinfo.endpoints[0].ticket).read_all()
    print("top flagged cards:", top_cards.to_pydict())
    client.do_action(fl.Action("ClosePreparedStatement",
                               enc({"handle": handle.decode()})))


if __name__ == "__main__":
    main()
```

Output (yours will vary slightly - random data):

```
ingest ack: {'rows_written': 250000}
policy enforced: svc_etl may not read
planned schema: ['currency', 'n', 'total']
streamed 22 chunks -> 87364 rows
currency      n       total
     EUR 125399 11296950.57
     GBP 124601 11223976.87
schema-only OK: ['currency', 'n', 'total']
top flagged cards: {'card_id': [300325, ...], 'hits': [3, 3, 3]}
```

## 3. Walkthrough: the five moves that matter

### 3.1 Auth handshake → token → identity

`authenticate()` runs ONCE: credentials go in, a token comes back. From then on the
client attaches the token automatically (`get_token()`) and the server re-validates on
**every** call via `is_valid()`, which returns the peer identity used by `_require`.
Anonymous or stale calls raise `FlightUnauthenticatedError` before any handler runs.

### 3.2 Command envelope ↔ protocol mapping

| Our envelope | Official Flight SQL message |
|---|---|
| `{"sql": "..."}` in descriptor | `CommandStatementQuery` |
| `{"handle": "stmt_..."}` | `CommandPreparedStatementQuery` |
| action `CreatePreparedStatement` | same name; handle returned in `Result` |
| `for_path("card_txns")` in `do_put` | `CommandStatementIngest`-style upload target |
| ticket `{"sql": ...}` | server-chosen opaque ticket |

### 3.3 Two-phase execution

`get_flight_info` resolves SQL to a schema via `duckdb.sql(...).arrow().schema`
(planning only - zero rows moved). Clients can render empty grids, validate contracts,
or fan out to endpoints before any data flows. `do_get` then streams 4096-row batches
straight out of DuckDB's columnar result - watch it arrive as **22 chunks**, not one blob.

### 3.4 The `do_put` ordering trap

```python
writer.write_table(txns)
writer.done_writing()      # <- half-close the upload FIRST...
ack = meta.read()          # ...THEN read the app-metadata ack
writer.close()
```

Read the ack before `done_writing()` and you deadlock or get `None`: the server's
`reader.read_all()` only finishes once the client half-closes. This bites everyone once.

### 3.5 Role policy

`ctx.peer_identity()` carries the authenticated principal into every handler;
`_require(ctx, verb)` turns it into allow-lists. In production this becomes a policy
engine call (row/column filters live in views or the engine - Lesson 10).

## 4. Exercises

1. Add `list_flights`: keep `{name: sql}` registry so analysts can discover datasets
   (`client.list_flights()` shows schemas + queries available).
2. Implement parameter binding: extend `CreatePreparedStatement` to accept an Arrow
   parameters table via `do_put` (like the protocol's bind phase), then execute with
   `con.execute(sql, params)`.
3. Add a `cancel` action: flip a shared flag that stops `_batches` mid-stream; verify
   the client receives a truncated stream instead of hanging.
4. Swap JSON envelopes for the real protobuf encoding using
   `pyarrow.flight.sql`-compatible wire format from the ADBC project docs - then connect
   with `adbc-driver-flightsql` and prove interop.
5. Run TWO gateway instances behind one catalog (Lesson 06 SQLite catalog works);
   confirm either serves identical results because storage truth stays in Iceberg.

## 5. Cheat sheet

| Task | Code |
|---|---|
| Start server | `MyServer(location, auth_handler=...).serve()` |
| Login | `client.authenticate(MyClientAuthHandler(...))` |
| Plan query | `client.get_flight_info(FlightDescriptor.for_command(cmd))` |
| Schema only | `client.get_schema(descriptor)` → `SchemaResult` |
| Stream read | `client.do_get(info.endpoints[0].ticket)` → `read_all()` / `read_chunk()` loop |
| Upload | `w, m = client.do_put(desc, schema)`; write; `done_writing()`; `m.read()` |
| Actions | `client.list_actions()`, `client.do_action(Action(name, body))` |
| Identity on server | `context.peer_identity()` after valid token |
| Errors | `FlightUnauthenticatedError`, `FlightUnauthorizedError`, `FlightServerError` |
| Engine hookup | `duckdb.con.sql(q).arrow().schema` (plan); `.execute(q).to_arrow_reader(N)` (stream) |

**Next:** Lesson 10 zooms out from one gateway to the whole lakehouse: medallion
zones, catalogs, compaction cadence, governance and how all five technologies click
together in production.
