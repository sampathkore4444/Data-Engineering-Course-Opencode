# Banking Terms Glossary

## Data Architecture Terms

| Term | Definition | Example |
|------|------------|---------|
| **Data Lakehouse** | Architecture combining data lake and data warehouse | Dremio + MinIO |
| **Medallion Architecture** | Three-layer data organization (Bronze-Silver-Gold) | Bronze → Silver → Gold |
| **Data Mesh** | Decentralized data architecture with domain ownership | Each team owns their data |
| **Data Fabric** | AI-driven metadata layer across hybrid environments | Automated data discovery |
| **Data Virtualization** | Query data without moving it | Dremio queries multiple sources |
| **CDC (Change Data Capture)** | Track and capture data changes | Debezium captures DB changes |
| **ETL** | Extract, Transform, Load | Traditional data integration |
| **ELT** | Extract, Load, Transform | Modern data integration |

## Banking Domain Terms

| Term | Definition | Example |
|------|------------|---------|
| **Core Banking** | Central system managing accounts and transactions | T24, Flexcube |
| **NPL (Non-Performing Loan)** | Loan where borrower stops paying | Loan overdue > 90 days |
| **CAR (Capital Adequacy Ratio)** | Bank's capital relative to risk | CAR = (Tier1 + Tier2) / RWA |
| **LCR (Liquidity Coverage Ratio)** | High-quality liquid assets / net cash outflows | LCR > 100% |
| **NIM (Net Interest Margin)** | Net interest income / average earning assets | NIM = 2.5% |
| **AML (Anti-Money Laundering)** | Detect and prevent money laundering | Suspicious transaction reports |
| **KYC (Know Your Customer)** | Verify customer identity | PAN, passport verification |
| **Basel III** | International banking regulation framework | Capital requirements |
| **SBV (State Bank of Vietnam)** | Central bank of Vietnam | Regulatory authority |

## Data Layer Terms

| Term | Definition | Purpose |
|------|------------|---------|
| **Bronze Layer** | Raw data from source systems | Preserve original format |
| **Silver Layer** | Cleaned and validated data | Remove duplicates, validate |
| **Gold Layer** | Business-ready aggregations | Star schemas, materialized views |
| **Quarantine** | Failed records requiring attention | Data quality management |
| **Reflection** | Dremio's materialized acceleration | Query performance optimization |

## Technology Terms

| Term | Definition | Usage |
|------|------------|-------|
| **Apache Arrow** | In-memory columnar format | Dremio's processing engine |
| **Parquet** | Columnar file format | Data storage in data lake |
| **Delta Lake** | ACID transactions for data lakes | Databricks ecosystem |
| **Iceberg** | Open table format for data lakes | Snowflake ecosystem |
| **Kafka** | Distributed event streaming platform | Real-time data pipeline |
| **Debezium** | CDC platform for databases | Capturing DB changes |
| **dbt** | Data build tool for SQL transformations | Analytics engineering |
| **Airflow** | Workflow orchestration platform | Pipeline scheduling |
| **Prometheus** | Monitoring and alerting toolkit | Metrics collection |
| **Grafana** | Analytics and monitoring platform | Dashboard visualization |

## Compliance Terms

| Term | Definition | Requirement |
|------|------------|-------------|
| **Circular 39/2014** | SBV regulation on data security | Data encryption, access control |
| **Circular 23/2014** | SBV regulation on reporting | Daily/monthly reports |
| **Decision 1168/QD-NHNN** | SBV AML regulation | Suspicious transaction reporting |
| **Data Localization** | Data must be stored in Vietnam | SBV requirement |
| **PII (Personally Identifiable Information)** | Sensitive customer data | Name, ID, phone, email |
| **PHI (Protected Health Information)** | Health-related data | Medical records |
| **RBAC (Role-Based Access Control)** | Access based on user roles | Role → Permission mapping |
| **RLS (Row-Level Security)** | Access based on row attributes | Branch-level access |

## Performance Terms

| Term | Definition | Target |
|------|------------|--------|
| **QPS (Queries Per Second)** | Number of queries executed per second | > 1000 QPS |
| **Latency** | Time to execute a query | < 2 seconds |
| **Throughput** | Amount of data processed per unit time | > 1 GB/second |
| **Partition Pruning** | Skipping irrelevant partitions | Reduce data scanned |
| **Reflection Hit Rate** | Percentage of queries using reflection | > 80% |
| **Data Freshness** | How recent the data is | < 1 hour |

## Business Terms

| Term | Definition | Example |
|------|------------|---------|
| **Customer 360°** | Complete view of customer across all products | Accounts, cards, loans |
| **Relationship Value** | Total value of customer relationship | Assets - Liabilities |
| **Risk Classification** | Loan risk categorization | Standard, Special Mention, NPA |
| **Provision Coverage** | Reserves for loan losses | Provisions / NPLs |
| **Fraud Score** | Risk score for transactions | 0-100 scale |
| **CTR (Currency Transaction Report)** | Report for cash transactions > threshold | VND 500M+ |
| **STR (Suspicious Transaction Report)** | Report for suspicious activity | AML compliance |
| **PEP (Politically Exposed Person)** | Public official or their family | Enhanced due diligence |
| **EDD (Enhanced Due Diligence)** | Additional verification for high-risk customers | PEP, high-value customers |

## Metric Definitions

| Metric | Formula | Target |
|--------|---------|--------|
| **NPL Ratio** | NPLs / Total Loans × 100 | < 3% |
| **CAR** | (Tier1 + Tier2) / RWA × 100 | > 10% |
| **LCR** | HQLA / Net Cash Outflows × 100 | > 100% |
| **NIM** | Net Interest Income / Average Earning Assets × 100 | > 2.5% |
| **ROA** | Net Income / Total Assets × 100 | > 1% |
| **ROE** | Net Income / Shareholder Equity × 100 | > 15% |
| **Cost-to-Income** | Operating Expenses / Operating Income × 100 | < 50% |
| **Card Utilization** | Credit Used / Credit Limit × 100 | 30-70% |
| **Payment Success Rate** | Successful Payments / Total Payments × 100 | > 99% |
| **Digital Transaction %** | Digital Transactions / Total Transactions × 100 | > 70% |

## Abbreviations

| Abbreviation | Full Form |
|--------------|-----------|
| **BI** | Business Intelligence |
| **ML** | Machine Learning |
| **AI** | Artificial Intelligence |
| **ETL** | Extract, Transform, Load |
| **ELT** | Extract, Load, Transform |
| **CDC** | Change Data Capture |
| **DWH** | Data Warehouse |
| **OLAP** | Online Analytical Processing |
| **OLTP** | Online Transaction Processing |
| **RWA** | Risk-Weighted Assets |
| **CET1** | Common Equity Tier 1 |
| **AT1** | Additional Tier 1 |
| **DPD** | Days Past Due |
| **SLA** | Service Level Agreement |
| **RTO** | Recovery Time Objective |
| **RPO** | Recovery Point Objective |
