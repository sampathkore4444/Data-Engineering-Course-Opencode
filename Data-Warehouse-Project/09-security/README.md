# 09 - Security & Access Control

## Overview

Security is critical for banking data. This folder contains RBAC, encryption, and audit logging configurations.

---

## Table of Contents

1. [Security in Banking](#1-security-in-banking)
2. [Architecture](#2-architecture)
3. [RBAC (Role-Based Access Control)](#3-rbac)
4. [Row-Level Security](#4-row-level-security)
5. [Encryption](#5-encryption)
6. [Audit Logging](#6-audit-logging)

---

## 1. Security in Banking

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SECURITY REQUIREMENTS (SBV)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  🔐 WHAT BANKS MUST PROTECT:                                                │
│  • Customer PII (name, email, phone)                                       │
│  • Account balances                                                        │
│  • Transaction history                                                     │
│  • Loan information                                                        │
│                                                                             │
│  📋 REGULATORY REQUIREMENTS:                                               │
│  • SBV (State Bank of Vietnam) compliance                                  │
│  • PCI DSS (for card data)                                                 │
│  • Data masking for non-production                                         │
│  • Audit trail for all access                                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SECURITY ARCHITECTURE                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Layer 1: Authentication                                            │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  • Username/Password (development)                                 │   │
│  │  • LDAP/Active Directory (production)                              │   │
│  │  • OAuth2/JWT (API access)                                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Layer 2: Authorization (RBAC)                                      │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  • Roles: admin, analyst, viewer                                   │   │
│  │  • Permissions: SELECT, INSERT, UPDATE, DELETE                     │   │
│  │  • Object-level: table, column, row                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Layer 3: Encryption                                                │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  • At rest: PostgreSQL TDE                                         │   │
│  │  • In transit: TLS/SSL                                             │   │
│  │  • Column-level: PII encryption                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Layer 4: Audit Logging                                             │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  • Who accessed what                                               │   │
│  │  • When and from where                                             │   │
│  │  • What changes were made                                          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. RBAC (Role-Based Access Control)

### Roles

| Role | Description | Permissions |
|------|-------------|-------------|
| **dw_admin** | Database administrator | Full access |
| **dw_etl** | ETL service account | INSERT, UPDATE on all tables |
| **dw_analyst** | Data analyst | SELECT on gold tables |
| **dw_viewer** | Read-only access | SELECT on specific tables |
| **dw_auditor** | Compliance auditor | SELECT + audit logs |

### SQL Setup

```sql
-- Create roles
CREATE ROLE dw_admin WITH LOGIN PASSWORD 'secure_password';
CREATE ROLE dw_etl WITH LOGIN PASSWORD 'secure_password';
CREATE ROLE dw_analyst WITH LOGIN PASSWORD 'secure_password';
CREATE ROLE dw_viewer WITH LOGIN PASSWORD 'secure_password';
CREATE ROLE dw_auditor WITH LOGIN PASSWORD 'secure_password';

-- Grant permissions
-- Admin: Full access
GRANT ALL PRIVILEGES ON DATABASE banking_dw TO dw_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO dw_admin;

-- ETL: Write access
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA staging TO dw_etl;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA gold TO dw_etl;

-- Analyst: Read access to gold
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dw_analyst;

-- Viewer: Read access to specific tables
GRANT SELECT ON gold.dim_customer TO dw_viewer;
GRANT SELECT ON gold.fact_transactions TO dw_viewer;

-- Auditor: Read access + audit logs
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dw_auditor;
GRANT SELECT ON audit.log_table TO dw_auditor;
```

---

## 4. Row-Level Security

### Branch-Based Access

```sql
-- Each branch manager sees only their branch data
CREATE POLICY branch_isolation ON gold.fact_transactions
    FOR SELECT
    USING (branch_id = current_setting('app.current_branch')::int);

-- Enable RLS
ALTER TABLE gold.fact_transactions ENABLE ROW LEVEL SECURITY;
```

### Customer-Based Access

```sql
-- Customer service sees only their assigned customers
CREATE POLICY customer_isolation ON gold.dim_customer
    FOR SELECT
    USING (customer_id IN (
        SELECT customer_id FROM customer_assignments 
        WHERE agent_id = current_setting('app.current_user')
    ));
```

---

## 5. Encryption

### Column-Level Encryption (PII)

```sql
-- Create extension
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Encrypt sensitive columns
CREATE OR REPLACE FUNCTION encrypt_pii(data text) 
RETURNS text AS $$
BEGIN
    RETURN pgp_sym_encrypt(data, current_setting('app.encryption_key'));
END;
$$ LANGUAGE plpgsql;

-- Use in table
ALTER TABLE gold.dim_customer 
    ALTER COLUMN email TYPE bytea USING encrypt_pii(email);
```

### TLS/SSL Configuration

```sql
-- postgresql.conf
ssl = on
ssl_cert_file = '/path/to/server.crt'
ssl_key_file = '/path/to/server.key'
ssl_ca_file = '/path/to/ca.crt'
```

---

## 6. Audit Logging

### Audit Table

```sql
CREATE TABLE audit.access_log (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    operation VARCHAR(10),  -- SELECT, INSERT, UPDATE, DELETE
    user_name VARCHAR(100),
    client_ip INET,
    query_text TEXT,
    row_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create trigger function
CREATE OR REPLACE FUNCTION audit_log_trigger()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit.access_log (table_name, operation, user_name, query_text)
    VALUES (TG_TABLE_NAME, TG_OP, current_user, current_query());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach to tables
CREATE TRIGGER audit_dim_customer
AFTER INSERT OR UPDATE OR DELETE ON gold.dim_customer
FOR EACH ROW EXECUTE FUNCTION audit_log_trigger();
```

### Audit Reports

```sql
-- Who accessed customer data today?
SELECT user_name, operation, COUNT(*) as access_count
FROM audit.access_log
WHERE table_name = 'dim_customer'
  AND created_at >= CURRENT_DATE
GROUP BY user_name, operation;

-- Recent suspicious activity
SELECT * FROM audit.access_log
WHERE operation = 'DELETE'
  AND created_at >= CURRENT_DATE - INTERVAL '1 day'
ORDER BY created_at DESC;
```

---

## Summary

| Layer | Purpose | Tool |
|-------|---------|------|
| **Authentication** | Verify identity | LDAP/OAuth2 |
| **Authorization** | Control access | RBAC |
| **Encryption** | Protect data | pgcrypto, TLS |
| **Audit** | Track access | Triggers |

**Security = Customer Trust + Regulatory Compliance**

---

*Back to: [Main README](../README.md)*
