# Apache Arrow vs Row-Based Storage: Banking Visual Guide

> **Understanding why Apache Arrow makes Dremio 10-100x faster for banking analytics**

---

## 1. THE PROBLEM: Bank Has 100 Million Card Transactions

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BANKING DATA CHALLENGE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Bank: MegaBank Vietnam                                                     │
│  Data: 100 MILLION card transactions                                        │
│  Size: 500 GB on disk                                                       │
│  Question: "What is total spend by merchant category for last 30 days?"    │
│                                                                             │
│  Goal: Get answer in SECONDS, not HOURS                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. ROW-BASED STORAGE (Traditional Format)

### How Traditional Databases Store Data

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ROW-BASED STORAGE (CSV, JSON, MySQL)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Each ROW is stored together:                                               │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ ROW 1: [txn_001 | CUST_001 | 2025-01-15 | 15000 | ELECTRONICS     │   │
│  │         | Amazon | Mumbai | UPI | POSTED | ... 40 more columns ...] │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │ ROW 2: [txn_002 | CUST_002 | 2025-01-15 | 800   | FOOD            │   │
│  │         | Zomato  | Delhi  | CARD | POSTED | ... 40 more columns ...] │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │ ROW 3: [txn_003 | CUST_003 | 2025-01-15 | 45000 | TRAVEL          │   │
│  │         | MakeMyTrip | Bengaluru | NET | POSTED | ... 40 more ...]  │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │ ...                                                                  │   │
│  │ ROW 100,000,000: [txn_100M | CUST_999 | 2025-01-30 | ... ]         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Total Columns: 50 (txn_id, card_id, date, amount, category, name, ...)   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### The Query: Filter by merchant_category

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                ROW-BASED QUERY EXECUTION                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SQL Query:                                                                 │
│  SELECT merchant_category, SUM(amount)                                      │
│  FROM transactions                                                          │
│  WHERE transaction_date >= '2025-01-01'                                     │
│  GROUP BY merchant_category;                                                │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    HOW ROW-BASED READS DATA                         │   │
│  │                                                                     │   │
│  │  To get merchant_category + amount, database must read:             │   │
│  │                                                                     │   │
│  │  Row 1: [txn_id, card_id, date, amount, category, name,           │   │
│  │          location, channel, status, ...]  ← READ ALL 50 COLUMNS!  │   │
│  │                                                                     │   │
│  │  Row 2: [txn_id, card_id, date, amount, category, name,           │   │
│  │          location, channel, status, ...]  ← READ ALL 50 COLUMNS!  │   │
│  │                                                                     │   │
│  │  Row 3: [txn_id, card_id, date, amount, category, name,           │   │
│  │          location, channel, status, ...]  ← READ ALL 50 COLUMNS!  │   │
│  │                                                                     │   │
│  │  ... repeat 100,000,000 times ...                                  │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ❌ PROBLEM: Reading 48 UNNECESSARY columns for every row!                 │
│                                                                             │
│  Data Read from Disk:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  100M rows × 50 columns = 5,000,000,000 values                      │   │
│  │  Actual data needed: 100M × 2 columns = 200,000,000 values         │   │
│  │                                                                     │   │
│  │  WASTE: 96% of data read is UNNECESSARY!                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Row-Based Performance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ROW-BASED PERFORMANCE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Operation                    │ Time        │ Disk I/O              │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  Scan 100M rows               │ 45 minutes  │ 500 GB                │   │
│  │  Read all columns             │ +30 minutes │ +300 GB               │   │
│  │  Filter by category           │ +5 minutes  │ +50 GB                │   │
│  │  Aggregate (SUM)              │ +2 minutes  │ +10 GB                │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  TOTAL                        │ ~82 minutes │ ~860 GB               │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ⏱️  Result: 82 MINUTES for a simple aggregation query!                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. COLUMNAR STORAGE (Apache Arrow)

### How Apache Arrow Stores Data

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    COLUMNAR STORAGE (Apache Arrow)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Each COLUMN is stored together (not rows!):                                │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ COLUMN: transaction_id                                              │   │
│  │ [txn_001, txn_002, txn_003, ..., txn_100M]                         │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │ COLUMN: customer_id                                                 │   │
│  │ [CUST_001, CUST_002, CUST_003, ..., CUST_999]                      │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │ COLUMN: transaction_date                                            │   │
│  │ [2025-01-15, 2025-01-15, 2025-01-15, ..., 2025-01-30]             │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │ COLUMN: amount                    ← ONLY READ THIS!                │   │
│  │ [15000, 800, 45000, 12000, 85000, ..., 25000]                      │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │ COLUMN: merchant_category        ← ONLY READ THIS!                │   │
│  │ [ELECTRONICS, FOOD, TRAVEL, RETAIL, ELECTRONICS, ..., FOOD]        │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │ COLUMN: merchant_name                                                │   │
│  │ [Amazon, Zomato, MakeMyTrip, Flipkart, Croma, ..., Swiggy]         │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │ ... (45 more columns)                                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Total Columns: 50 (same data, different organization)                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### The Query: Filter by merchant_category

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                COLUMNAR QUERY EXECUTION (Apache Arrow)                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SQL Query:                                                                 │
│  SELECT merchant_category, SUM(amount)                                      │
│  FROM transactions                                                          │
│  WHERE transaction_date >= '2025-01-01'                                     │
│  GROUP BY merchant_category;                                                │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    HOW ARROW READS DATA                             │   │
│  │                                                                     │   │
│  │  Arrow knows exactly where each column is stored:                   │   │
│  │                                                                     │   │
│  │  Step 1: Read ONLY transaction_date column (for filter)            │   │
│  │          [2025-01-15, 2025-01-15, ..., 2025-01-30]                 │   │
│  │          Size: 800 MB (100M dates × 8 bytes each)                  │   │
│  │                                                                     │   │
│  │  Step 2: Read ONLY merchant_category column                        │   │
│  │          [ELECTRONICS, FOOD, TRAVEL, RETAIL, ...]                   │   │
│  │          Size: 1.2 GB (100M categories × 12 bytes avg)             │   │
│  │                                                                     │   │
│  │  Step 3: Read ONLY amount column                                   │   │
│  │          [15000, 800, 45000, 12000, ...]                           │   │
│  │          Size: 800 MB (100M amounts × 8 bytes each)                │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ✅ BENEFIT: Reading only 3 columns out of 50!                             │
│                                                                             │
│  Data Read from Disk:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Columns read: 3 (transaction_date, merchant_category, amount)      │   │
│  │  Data read: 100M × 3 columns = 300,000,000 values                  │   │
│  │                                                                     │   │
│  │  SAVINGS: 94% less data read!                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Apache Arrow Performance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    APACHE ARROW PERFORMANCE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Operation                    │ Time        │ Disk I/O              │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  Scan 3 columns only          │ 2 seconds   │ 2.8 GB                │   │
│  │  In-memory filtering          │ +0.5 sec    │ 0 (CPU only)          │   │
│  │  SIMD aggregation             │ +0.3 sec    │ 0 (CPU only)          │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  TOTAL                        │ ~3 seconds  │ ~2.8 GB               │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ⚡ Result: 3 SECONDS for the same query!                                  │
│                                                                             │
│  SPEEDUP: 82 minutes ÷ 3 seconds = 1,640x FASTER!                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. SIDE-BY-SIDE COMPARISON

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ROW-BASED vs APACHE ARROW                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ROW-BASED STORAGE (Traditional)         │  APACHE ARROW (Columnar)        │
│  ═══════════════════════════════════════  │  ══════════════════════════════  │
│                                          │                                  │
│  ┌─────────────────────────────────┐     │  ┌─────────────────────────┐     │
│  │ ROW 1: [A1, B1, C1, D1, E1, F1]│     │  │ COL A: [A1, A2, A3, ...]│     │
│  │ ROW 2: [A2, B2, C2, D2, E2, F2]│     │  │ COL B: [B1, B2, B3, ...]│     │
│  │ ROW 3: [A3, B3, C3, D3, E3, F3]│     │  │ COL C: [C1, C2, C3, ...]│     │
│  │ ROW 4: [A4, B4, C4, D4, E4, F4]│     │  │ COL D: [D1, D2, D3, ...]│     │
│  │ ROW 5: [A5, B5, C5, D5, E5, F5]│     │  │ COL E: [E1, E2, E3, ...]│     │
│  └─────────────────────────────────┘     │  │ COL F: [F1, F2, F3, ...]│     │
│                                          │  └─────────────────────────┘     │
│  To read columns A and C:                │                                  │
│  Must read ALL rows (A,B,C,D,E,F)       │  To read columns A and C:        │
│  ❌ Reads 6 columns per row              │  ✅ Reads ONLY columns A and C   │
│                                          │                                  │
├──────────────────────────────────────────┼──────────────────────────────────┤
│  PERFORMANCE METRICS                     │                                  │
├──────────────────────────────────────────┼──────────────────────────────────┤
│                                          │                                  │
│  100M rows × 50 columns                 │  100M rows × 3 columns          │
│  = 5 billion values                      │  = 300 million values            │
│                                          │                                  │
│  Disk I/O: 500 GB                        │  Disk I/O: 2.8 GB               │
│  Time: 82 minutes                        │  Time: 3 seconds                 │
│  CPU: Waiting for disk                   │  CPU: Processing in memory       │
│                                          │                                  │
│  ❌ SLOW (I/O bound)                     │  ✅ FAST (CPU bound)            │
│                                          │                                  │
├──────────────────────────────────────────┼──────────────────────────────────┤
│  BANKING USE CASE                        │                                  │
├──────────────────────────────────────────┼──────────────────────────────────┤
│                                          │                                  │
│  "What is total spend by category?"      │                                  │
│                                          │                                  │
│  Row-Based: 82 minutes                   │  Arrow: 3 seconds               │
│  (CEO waits over an hour!)               │  (CEO gets answer instantly!)    │
│                                          │                                  │
└──────────────────────────────────────────┴──────────────────────────────────┘
```

---

## 5. APACHE ARROW ADDITIONAL OPTIMIZATIONS

### 5.1 SIMD (Single Instruction, Multiple Data)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SIMD PROCESSING (CPU Optimization)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Traditional (Row-Based):                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  CPU Core 1: Process Row 1 → Process Row 2 → Process Row 3 → ...  │   │
│  │                                                                     │   │
│  │  Time: 100 million sequential operations                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Apache Arrow (SIMD):                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  CPU Core 1: Process [Row 1, Row 2, Row 3, Row 4, Row 5, Row 6,    │   │
│  │               Row 7, Row 8] simultaneously                          │   │
│  │                                                                     │   │
│  │  CPU Core 2: Process [Row 9, Row 10, Row 11, Row 12, Row 13,       │   │
│  │               Row 14, Row 15, Row 16] simultaneously               │   │
│  │                                                                     │   │
│  │  ... (8+ cores working in parallel)                                 │   │
│  │                                                                     │   │
│  │  Time: 100M ÷ 8 cores = 12.5M operations per core                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  SPEEDUP: 8x additional (on 8-core CPU)                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Zero-Copy Data Sharing

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ZERO-COPY DATA SHARING                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  TRADITIONAL APPROACH (Data Copying):                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  System A (Oracle)    System B (MySQL)    System C (PostgreSQL)    │   │
│  │  ┌──────────┐         ┌──────────┐        ┌──────────┐            │   │
│  │  │ Data     │ ──ETL──►│ Copy 1   │ ──ETL──►│ Copy 2   │            │   │
│  │  │ (500 GB) │         │ (500 GB) │        │ (500 GB) │            │   │
│  │  └──────────┘         └──────────┘        └──────────┘            │   │
│  │                                                                     │   │
│  │  Total Storage: 1.5 TB (3 copies of same data!)                    │   │
│  │  Total Time: Hours (ETL pipelines)                                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  APACHE ARROW APPROACH (Zero-Copy):                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  System A (Oracle)    System B (MySQL)    System C (PostgreSQL)    │   │
│  │  ┌──────────┐         ┌──────────┐        ┌──────────┐            │   │
│  │  │ Data     │ ◄─────►│ Arrow    │ ◄─────►│ Arrow    │            │   │
│  │  │ (500 GB) │ Memory │ Shared   │ Memory │ Shared   │            │   │
│  │  │          │ Share  │ Format   │ Share  │ Format   │            │   │
│  │  └──────────┘         └──────────┘        └──────────┘            │   │
│  │                                                                     │   │
│  │  Total Storage: 500 GB (1 copy, shared in memory!)                 │   │
│  │  Total Time: Milliseconds (memory sharing)                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  STORAGE SAVINGS: 66% less storage                                         │
│  TIME SAVINGS: 1,000x faster                                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. BANKING SCENARIO: REAL-TIME FRAUD DETECTION

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FRAUD DETECTION: ROW-BASED vs ARROW                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Scenario: Detect fraudulent card transactions in real-time                 │
│  Data: 100 million transactions from last 24 hours                         │
│  Goal: Find suspicious patterns in SECONDS                                  │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  ROW-BASED APPROACH:                                                        │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Step 1: Scan all 100M rows (read all 50 columns)         → 45 minutes    │
│  Step 2: Filter by transaction_date (today)                → 5 minutes     │
│  Step 3: Calculate velocity per card                       → 10 minutes    │
│  Step 4: Detect anomalies                                  → 15 minutes    │
│  ─────────────────────────────────────────────────────────────────────────  │
│  TOTAL: 75 MINUTES                                                          │
│                                                                             │
│  ❌ FRAUD ALREADY HAPPENED BY THE TIME DETECTED!                           │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  APACHE ARROW APPROACH:                                                     │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Step 1: Read only 5 needed columns (Arrow columnar)      → 2 seconds     │
│  Step 2: In-memory filter by date (SIMD)                   → 0.3 seconds   │
│  Step 3: Window function for velocity (parallel)           → 0.5 seconds   │
│  Step 4: Anomaly detection (CPU bound)                     → 0.2 seconds   │
│  ─────────────────────────────────────────────────────────────────────────  │
│  TOTAL: 3 SECONDS                                                           │
│                                                                             │
│  ✅ FRAUD DETECTED IN REAL-TIME - BLOCK TRANSACTION!                       │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  IMPACT:                                                                    │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  • Prevent $X million in fraud losses per year                              │
│  • Meet regulatory requirements for real-time monitoring                    │
│  • Reduce false positives (better customer experience)                      │
│  • Faster investigation and resolution                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. BANKING SCENARIO: CEO DASHBOARD

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CEO DASHBOARD: ROW-BASED vs ARROW                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Scenario: CEO opens morning dashboard for real-time KPIs                   │
│  Data: Customer 360° view across all banking products                      │
│  Goal: Instant access to total relationship value                          │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  ROW-BASED APPROACH:                                                        │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Query joins 4 tables:                                                      │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌────────────┐ │
│  │ Customers    │ + │ Accounts     │ + │ Cards        │ + │ Loans      │ │
│  │ (1M rows)    │   │ (2M rows)    │   │ (500K rows)  │   │ (1M rows)  │ │
│  └──────────────┘   └──────────────┘   └──────────────┘   └────────────┘ │
│                                                                             │
│  Step 1: Scan all 4 tables (read all columns)            → 30 minutes    │
│  Step 2: Join tables (hash join)                         → 10 minutes    │
│  Step 3: Aggregate and calculate                         → 5 minutes     │
│  ─────────────────────────────────────────────────────────────────────────  │
│  TOTAL: 45 MINUTES                                                          │
│                                                                             │
│  ❌ CEO: "This dashboard is too slow! I'll check again later."            │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  APACHE ARROW APPROACH (with Reflections):                                  │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Dremio creates pre-aggregated Arrow reflections:                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  REFLECTION: customer_360_summary                                   │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │ customer_id │ total_accounts │ total_balance │ total_cards │   │   │
│  │  ├─────────────────────────────────────────────────────────────┤   │   │
│  │  │ CUST_001    │ 3              │ 2,500,000    │ 2           │   │   │
│  │  │ CUST_002    │ 1              │ 850,000      │ 1           │   │   │
│  │  │ ... (1M rows pre-aggregated)                               │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  │  Size: 50 MB (instead of 5 GB)                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Step 1: Read pre-aggregated reflection (50 MB)          → 0.1 seconds   │
│  Step 2: Calculate total relationship value               → 0.05 seconds  │
│  ─────────────────────────────────────────────────────────────────────────  │
│  TOTAL: 50 MILLISECONDS                                                     │
│                                                                             │
│  ✅ CEO: "Wow, instant! Let me check another metric."                      │
│                                                                             │
│  SPEEDUP: 45 minutes ÷ 0.05 seconds = 54,000x FASTER!                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. SUMMARY: WHY ARROW MATTERS FOR BANKING

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    APACHE ARROW: BANKING IMPACT                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                     │   │
│  │  Banking Use Case              │ Row-Based    │ Arrow (Dremio)      │   │
│  │  ═══════════════════════════════════════════════════════════════════│   │
│  │  Fraud Detection               │ 75 minutes   │ 3 seconds          │   │
│  │  CEO Dashboard                 │ 45 minutes   │ 50 milliseconds    │   │
│  │  Regulatory Report (Basel III) │ 45 minutes   │ 3.5 seconds        │   │
│  │  Customer 360° View            │ 30 minutes   │ 20 milliseconds    │   │
│  │  NPA Tracking                  │ 20 minutes   │ 1 second           │   │
│  │  Merchant Analytics            │ 60 minutes   │ 4 seconds          │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  KEY TAKEAWAYS:                                                             │
│                                                                             │
│  ✅ 10-100x FASTER queries on banking data                                │
│  ✅ REAL-TIME fraud detection (prevent losses)                            │
│  ✅ INSTANT dashboards (CEO gets answers in ms)                           │
│  ✅ FAST regulatory reports (meet compliance deadlines)                   │
│  ✅ LESS storage (columnar compression)                                   │
│  ✅ LOWER costs (less disk I/O, less compute)                            │
│                                                                             │
│  HOW TO USE IN DREMIO:                                                     │
│                                                                             │
│  1. Store data in columnar formats (Parquet, Iceberg, Delta)              │
│  2. Create reflections for frequently accessed queries                    │
│  3. Use Dremio's Arrow-based query engine                                 │
│  4. Enable zero-copy cloning for test environments                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. VISUAL: DATA READ COMPARISON

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    VISUAL: WHAT DATA IS READ                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Query: SELECT merchant_category, SUM(amount) FROM transactions;           │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  ROW-BASED (Traditional):                                                   │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Columns on Disk:                                                           │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐          │
│  │ txn │card │date │ amt │ cat │name │loc  │chann│stat │ ... │          │
│  │ id  │ id  │     │     │     │     │     │ el  │ us  │     │          │
│  ├─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤          │
│  │█████│█████│█████│█████│█████│█████│█████│█████│█████│█████│ 50 cols  │
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘          │
│                                                                             │
│  Data Read: ██████████████████████████████████████████████████████         │
│             (ALL 50 COLUMNS = 500 GB)                                      │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│  APACHE ARROW (Columnar):                                                   │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  Columns on Disk:                                                           │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐          │
│  │ txn │card │date │ amt │ cat │name │loc  │chann│stat │ ... │          │
│  │ id  │ id  │     │     │     │     │     │ el  │ us  │     │          │
│  ├─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤          │
│  │     │     │     │█████│█████│     │     │     │     │     │ 3 cols   │
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘          │
│                                                                             │
│  Data Read: ██████                                                           │
│             (ONLY 3 COLUMNS = 2.8 GB)                                      │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════   │
│                                                                             │
│  SAVINGS: 94% less disk I/O = 94% faster query!                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

*Created for: Banking Data Platform - Lakehouse Architecture*
*Learn more: `03-dremio-sql/05-apache-arrow-banking.sql`*