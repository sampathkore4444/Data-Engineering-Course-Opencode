# ADR-001: Dremio Selection as Data Platform

## Status
Accepted

## Date
2024-01-15

## Context

We need to select a data platform for our banking data lakehouse that can:
1. Query data across multiple source systems (Oracle, Mainframe, SQL Server)
2. Provide real-time analytics for fraud detection and customer 360°
3. Support regulatory reporting (SBV compliance)
4. Handle both structured and semi-structured data
5. Scale to handle growing data volumes (20% monthly growth)

## Decision

We will use **Dremio** as our primary data platform for the following reasons:

### 1. Apache Arrow Performance
- Dremio uses Apache Arrow for in-memory processing
- 10x faster than traditional query engines
- Real-time queries for fraud detection (< 500ms)

### 2. Data Virtualization
- Query data without moving it
- Connect to Oracle, Mainframe, SQL Server, MinIO
- Single SQL interface across all sources

### 3. Reflections (Materialized Views)
- Pre-compute and store query results
- Automatic query acceleration
- Reduces query time from seconds to milliseconds

### 4. Open Source Foundation
- Built on Apache Arrow and Parquet
- No vendor lock-in
- Active open-source community

### 5. Cost Effective
- Open-source core (free)
- Commercial enterprise features available
- Lower TCO than proprietary solutions

## Alternatives Considered

### 1. Snowflake
**Pros:**
- Fully managed service
- Easy to use
- Good performance

**Cons:**
- Vendor lock-in
- Higher cost at scale
- Limited data virtualization

### 2. Databricks
**Pros:**
- Strong ML capabilities
- Delta Lake support
- Good for data engineering

**Cons:**
- Higher cost
- More complex setup
- Less focus on SQL analytics

### 3. Starburst (Trino)
**Pros:**
- Open source
- Good for federated queries
- Strong connector ecosystem

**Cons:**
- No built-in reflections
- Requires more configuration
- Less mature enterprise features

### 4. Presto
**Pros:**
- Open source
- Good for interactive queries
- Facebook-scale proven

**Cons:**
- No built-in reflections
- Requires significant configuration
- Less enterprise-ready

## Consequences

### Positive
1. **Real-time analytics** for fraud detection and customer 360°
2. **Cost savings** from data virtualization (no data movement)
3. **Faster regulatory reporting** (automated SBV reports)
4. **Scalability** for growing data volumes
5. **Flexibility** to query across multiple sources

### Negative
1. **Learning curve** for team (new technology)
2. **Enterprise features** require commercial license
3. **Reflection maintenance** requires monitoring
4. **Connector configuration** for each source system

### Risks
1. **Team adoption** - Mitigate with training and documentation
2. **Performance tuning** - Mitigate with reflections and query optimization
3. **Source system changes** - Mitigate with flexible schema evolution

## Implementation Plan

### Phase 1: Foundation (Weeks 1-4)
- [ ] Set up Dremio cluster
- [ ] Configure source connections
- [ ] Create basic spaces and views
- [ ] Train core team

### Phase 2: Core Use Cases (Weeks 5-8)
- [ ] Implement Customer 360° view
- [ ] Implement fraud detection queries
- [ ] Implement regulatory reports
- [ ] Enable reflections

### Phase 3: Advanced Features (Weeks 9-12)
- [ ] Implement row-level security
- [ ] Set up monitoring dashboards
- [ ] Optimize query performance
- [ ] Document best practices

## Review Date
2024-07-15 (6 months after implementation)
