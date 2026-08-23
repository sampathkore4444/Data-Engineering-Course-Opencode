# Lesson 08 — Apache Arrow Flight SQL: The Wire Protocol

> **Meridian Trust Bank case study, Part 8**: Twelve teams consume Meridian's data:
> risk runs R, fraud uses Python notebooks, treasury insists on JDBC, the regulator's
> tooling speaks ODBC. Today each team scrapes its own CSV/SFTP export - twelve copies,
> twelve freshness levels, zero governance. Flight SQL replaces all of it: **one protocol,
> any language, Arrow end-to-end** - with sub-second scans of the Parquet/Iceberg tables
> from Lessons 02-07.

---

## 1. Concept: why a *protocol*, not another database

Your data lives in Parquet files and Iceberg tables (storage truth - Lessons 02, 06).
Consumers still need a way to *ask for* slices of it over the network. Historically:

| Approach | Problem |
|---|---|
| ODBC/JDBC to a warehouse | row-oriented serialization, driver hell, CPU burn converting to/from columnar |
| REST + JSON | 10-100x payload inflation, no schema, no streaming |
| Per-team exports | N copies, N freshness SLAs, no access control story |

**Arrow Flight** is an RPC framework built *on top of gRPC* where the payload is
**Arrow RecordBatches**. **Flight SQL** is a thin standardization layer on Flight that
defines *what the bytes mean*: a conventional set of commands ("run this statement",
"prepare this", "list catalogs") so that any client can talk to any compliant server.

The one-sentence pitch:

```
Flight      = "gRPC whose data plane streams Arrow record batches"
Flight SQL  = "a standard vocabulary of commands & result types on top of Flight"
```

Why this matters at a bank: one governed endpoint, columnar all the way from disk
(Parquet) through engine (DuckDB) to network (Flight) into pandas/R/Java - **zero
serialization cost** anywhere on the path.

## 2. Foundation: gRPC in five sentences

1. gRPC = remote method calls defined in a `.proto` file (IDL), running over HTTP/2.
2. HTTP/2 gives multiplexed streams: many concurrent calls, one TCP connection.
3. Messages are protobuf: compact binary, typed, forward/backward compatible.
4. Four call shapes: unary, server-streaming, client-streaming, bidirectional.
5. Interceptors ("middleware") add auth/tracing/logging transparently.

Flight reuses all of this and adds exactly one thing: the message body format is
**the Arrow IPC encapsulated format** - i.e., the same bytes Arrow uses in memory
and in files, streamed without conversion.

### 2.1 Where the time goes (why JSON/ODBC lose)

```
Warehouse -> JSON  : convert columns->rows->strings, 20-50x size, parse on other side
Warehouse -> ODBC  : row-by-row fetch callback per record
Parquet   -> DuckDB -> Arrow batches -> gRPC frames -> pandas/numpy
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             columnar buffers copied or even only referenced (zero-copy)
```

## 3. The Flight data model

Five primitives you must know:

| Primitive | Role |
|---|---|
| `FlightDescriptor` | what you want: PATH (a dataset name) or COMMAND (opaque cmd bytes) |
| `FlightInfo` | answer to "what would I get": schema, total rows/bytes, list of endpoints |
| `FlightEndpoint` | where+how to read: a **Ticket** (opaque read handle) + list of Locations |
| `Ticket` | opaque token handed to `do_get` to stream the actual data |
| `RecordBatchStream` | the data itself: Arrow schema + sequence of record batches |

```
   CLIENT                                SERVER
     │  get_flight_info(descriptor)         │   plan query, do NOT run it yet
     │──────────────────────────────────▶   │
     │◀───── FlightInfo {schema,            │
     │        [Endpoint{ticket, loc}]} ─    │
     │                                      │
     │  do_get(ticket)                      │   execute; stream results
     │──────────────────────────────────▶   │
     │◀═══ batch,batch,batch,... ═══════    │   Arrow IPC frames (HTTP/2 DATA)
```

Key insight: **two-phase**. `get_flight_info` is cheap metadata planning (schema before
data!); `do_get` streams. A client can show a preview from the schema alone, fan out to
multiple endpoints (parallel reads from different locations!), and cancel by just
closing the stream.

### 3.1 The five RPC methods

| Method | Shape | Purpose |
|---|---|---|
| `handshake` | bidi | exchange credentials → session token |
| `list_flights` / `get_flight_info` / `get_schema` | unary/streaming | discover datasets, plan queries |
| `do_get` | server-streaming | read a dataset (by ticket) |
| `do_put` | client-streaming | write/upload a dataset (+app-metadata ack back) |
| `do_action` / `list_actions` | streaming | RPC verbs outside CRUD: healthcheck, cancel, admin ops |

## 4. Flight SQL: the standardized vocabulary

Flight alone leaves "what goes inside descriptor.command?" undefined. Flight SQL fixes
exactly that - it specifies protobuf messages (`flight_sql.proto`) that servers pack
into descriptors/tickets/actions:

| Operation | Command message | Result |
|---|---|---|
| Run a query | `CommandStatementQuery` | endpoint(s) → `DoGet` stream |
| Update data | `CommandStatementUpdate` | affected-row count |
| Prepare | action `CreatePreparedStatement` | prepared-statement **handle** |
| Execute prepared | `CommandPreparedStatementQuery` | endpoint(s) → `DoGet` |
| Bind params | `DoPut` with `CommandPreparedStatementQuery` | bind ack |
| Catalogs/schemas/tables | `GetCatalogs`, `GetTables`, ... | metadata result sets |
| Transaction scope | `BeginTransaction` / `Commit` / ... | transaction handles |

So a compliant client (ADBC Flight SQL driver, JDBC driver, `flightsql-dbapi`) works
against Dremio, InfluxDB 3, Spark Connect-style gateways... **or against the server we
build in Lesson 09**.

> **PyArrow reality check**: PyArrow ships Flight *RPC* primitives only
> (`pyarrow.flight`). There is no `pyarrow.flight.sql` module. Production clients use
> the **ADBC Flight SQL driver** (`adbc-driver-flightsql`) or `flightsql-dbapi`;
> servers either wrap a native C++/Go/Java implementation or implement the convention
> themselves on `pyarrow.flight.FlightServerBase`. Lesson 09 does the latter with
> JSON-encoded command envelopes - same semantics as the protobuf spec, runnable
> with pyarrow alone.

### 4.1 Two-phase execution & prepared statements

```sql
-- 1. prepare once: server parses/plans, returns an opaque handle
CreatePreparedStatement("SELECT amount FROM card_txns WHERE card_id = ?") -> H
-- 2. bind parameters via DoPut (typed Arrow values!)
DoPut(H, parameters={card_id: 300_042})
-- 3. execute & stream
get_flight_info(CommandPreparedStatementQuery{H}) -> ticket
do_get(ticket) -> batches...
-- 4. close
ClosePreparedStatement(H)
```

Prepared statements buy: parse-on-run-many, injection safety, and parameter typing
enforced end-to-end by Arrow schemas rather than string coercion.

## 5. Authentication & security

Three layers stack neatly:

```
TLS            grpc:// vs grpc+tls:// ; client verifies server cert
Handshake      ServerAuthHandler/ClientAuthHandler: credentials -> token
Per-call token ClientAuthHandler.get_token() attached to EVERY subsequent call
Authorization  server code checks identity (ctx.peer_identity()) per method/query
```

```python
# the whole dance, PyArrow edition
class TokenHandler(fl.ServerAuthHandler):
    def authenticate(self, outgoing, incoming):
        user, _, pw = incoming.read().decode().partition(":")
        if USERS.get(user) != pw:
            raise fl.FlightUnauthenticatedError("no such principal")
        outgoing.write(issue_jwt(user))            # <- token
    def is_valid(self, token):                     # called on every call
        return validate_jwt(token)                 # -> peer identity

class Login(fl.ClientAuthHandler):
    def authenticate(self, outgoing, incoming):
        outgoing.write(b"risk_analyst:quarter-end")
        self.token = incoming.read()
    def get_token(self): return self.token         # auto-sent per call

client.authenticate(Login())
# later: ctx.peer_identity() == b"risk_analyst" on the server
```

Bank pattern: handshake issues a short-lived JWT; middleware validates it; a policy
engine decides which SQL verbs/principals are allowed (row/column filtering happens
in the engine or via views - Lesson 10).

## 6. Streaming & partitioned results

- Results stream as **many small record batches**, not one blob: first row latency is
  milliseconds after planning; backpressure is natural (gRPC flow control).
- `FlightInfo.endpoints` is a LIST: a smart server splits a big scan across locations
  (S3-east, S3-west) and clients fetch tickets **in parallel**.
- `do_put` is the write path: clients stream ingest batches while the server can send
  application-metadata acks mid-flight (e.g., running row counts).
- Cancellation = close the call; the server's generator gets GC'd/closed.
- Timeouts/deadlines via `FlightCallOptions(timeout=...)`.

## 7. Banking scenario walkthrough

Meridian deploys ONE service: `flight-gateway.meridian.internal:31337`.

- Behind it: DuckDB engines reading Iceberg tables (Lessons 05-07) - the gateway owns
  catalog credentials, consumers never touch S3 paths.
- Fraud analysts: Python + pyarrow, stream flagged transactions live during an attack.
- Treasury: JDBC Flight SQL driver inside existing BI - same governed endpoint.
- Regulator feed: monthly `do_put` of reference data + `get_schema` contract tests -
  schema drift fails CI, not the overnight batch.
- Result: 12 pipelines → 1 endpoint; CSV exports die; audit logs capture principal,
  query, bytes served.

## 8. End-to-end example: raw Flight in 60 lines

A complete (if minimal) Flight server + client proving the mechanics: command
descriptors, two-phase get_info→do_get, do_put with app-metadata ack, and actions.

```python
"""
lesson08_flight_minimal.py
Raw Arrow Flight round trip: info -> stream, put -> ack, actions.
Deps: pip install pyarrow
"""
import threading, time
import pyarrow as pa
import pyarrow.flight as fl

DATA = pa.table({"id": [1, 2, 3], "amount": [10.5, 42.0, 7.25]})

class Server(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self._location = location

    def get_flight_info(self, context, descriptor):
        query = descriptor.command                  # opaque command bytes
        ticket = fl.Ticket(query)                   # echo as the read handle
        return fl.FlightInfo(
            DATA.schema, descriptor,
            [fl.FlightEndpoint(ticket, [self._location])],
            DATA.num_rows, DATA.nbytes)

    def get_schema(self, context, descriptor):
        return fl.SchemaResult(DATA.schema)          # schema without data!

    def do_get(self, context, ticket):
        return fl.RecordBatchStream(DATA)            # stream batches

    def do_put(self, context, descriptor, reader, writer):
        received = reader.read_all()                 # drain upload
        writer.write(pa.py_buffer(
            f"rows={received.num_rows}".encode()))   # app-metadata ack

    def do_action(self, context, action):
        if action.type == "healthcheck":
            yield fl.Result(b"ok")

    def list_actions(self, context):
        return [("healthcheck", "liveness probe")]

server = Server("grpc://127.0.0.1:31337")
threading.Thread(target=server.serve, daemon=True).start()
time.sleep(0.5)

client = fl.FlightClient("grpc://127.0.0.1:31337")

info = client.get_flight_info(
    fl.FlightDescriptor.for_command(b"SELECT * FROM txns"))
print("planned:", info.schema.names, info.total_records)

table = client.do_get(info.endpoints[0].ticket).read_all()
print(table.to_pydict())

w, m = client.do_put(fl.FlightDescriptor.for_path("txns"), DATA.schema)
w.write_table(DATA)
w.done_writing()                       # signal end-of-upload BEFORE reading ack
print("ack:", m.read().to_pybytes())
w.close()

print("healthcheck:", [r.body.to_pybytes()
      for r in client.do_action(fl.Action("healthcheck", b""))])
```

Output:

```
planned: ['id', 'amount'] 3
{'id': [1, 2, 3], 'amount': [10.5, 42.0, 7.25]}
ack: b'rows=3'
healthcheck: [b'ok']
```

## 9. Exercises

1. Add `list_flights` to the Lesson 08 server: keep a registry `{name: table}` and let
   clients discover available datasets instead of knowing query strings.
2. Split DATA into 3 record batches and verify the client receives them incrementally
   (`reader.iter_chunks()` prints as they arrive) - observe streaming, not one blob.
3. Implement `do_action("cancel", ticket)` semantics: make `do_get` stop mid-stream
   when a flag flips; confirm the client sees a truncated but valid stream.
4. Wrap the minimal server with `ServerAuthHandler`/`ClientAuthHandler`; prove an
   unauthenticated `do_get` raises `FlightUnauthenticatedError`.
5. Benchmark JSON-over-HTTP vs Flight for 1M rows x 5 float columns on localhost;
   compare wall-clock and bytes transferred. (Expect ~an order of magnitude.)

## 10. Cheat sheet

| Concept | Fact |
|---|---|
| Flight | gRPC-based RPC framework; data plane = Arrow record batches |
| Flight SQL | standard protobuf commands layered ON Flight (descriptors/tickets/actions) |
| Descriptor | PATH (named dataset) or COMMAND (encoded request) |
| Two-phase | `get_flight_info` plans (schema!) → `do_get(ticket)` streams |
| Endpoint | ticket + locations; multiple endpoints ⇒ parallel reads |
| `do_put` | client-streaming writes; server sends app-metadata acks; `done_writing()` before reading ack |
| Actions | extensible RPC verbs: prepare/close statements, healthcheck, cancel |
| Auth | TLS + handshake→token + per-call validation (`peer_identity`) |
| PyArrow | has `pyarrow.flight` (RPC) only; Flight-SQL clients come via ADBC drivers |
| Why fast | columnar bytes stay columnar end-to-end; HTTP/2 streaming; no row conversion |

**Next:** Lesson 09 wires this protocol to a real DuckDB engine - a miniature
production Flight SQL gateway with logins, roles, ingestion and streaming analytics.
