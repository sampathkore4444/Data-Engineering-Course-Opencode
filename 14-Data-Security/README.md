# 14 - Data Security

## Table of Contents
1. [Security Fundamentals](#1-security-fundamentals)
2. [Encryption](#2-encryption)
3. [Access Control](#3-access-control)
4. [Compliance Regulations](#4-compliance-regulations)
5. [Real-World Scenarios](#5-real-world-scenarios)
6. [Hands-On Exercises](#6-hands-on-exercises)
7. [Interview Questions](#7-interview-questions)

---

## 1. Security Fundamentals

### Data Security Layers

```
+--------------------------------------------------+
|              PHYSICAL SECURITY                    |
|  Data centers, hardware, network                 |
+--------------------------------------------------+
                        |
+--------------------------------------------------+
|              NETWORK SECURITY                    |
|  Firewalls, VPNs, DDoS protection               |
+--------------------------------------------------+
                        |
+--------------------------------------------------+
|              APPLICATION SECURITY                |
|  Authentication, authorization, session mgmt     |
+--------------------------------------------------+
                        |
+--------------------------------------------------+
|              DATA SECURITY                       |
|  Encryption, masking, tokenization, access ctrl  |
+--------------------------------------------------+
```

### Security Principles

| Principle | Description |
|-----------|-------------|
| **Least Privilege** | Grant minimum permissions needed |
| **Defense in Depth** | Multiple security layers |
| **Separation of Duties** | No single person controls entire process |
| **Need to Know** | Access only what's necessary |
| **Zero Trust** | Never trust, always verify |

### Data Security Tools

| Category | Tools | Description |
|----------|-------|-------------|
| **Key Management** | AWS KMS, Azure Key Vault, Google Cloud KMS | Centralized key management |
| **Secret Management** | HashiCorp Vault, AWS Secrets Manager, CyberArk | Secure secrets storage |
| **Data Masking** | Informatica, Delphix, ARX | Static and dynamic masking |
| **SIEM** | Splunk, IBM QRadar, Azure Sentinel | Security event monitoring |
| **Identity** | Okta, Auth0, Azure AD | Identity and access management |
| **DLP** | Google DLP, AWS Macie, Azure Purview | Data loss prevention |

---

## 2. Encryption

### Encryption at Rest

```sql
-- PostgreSQL: pgcrypto extension
CREATE EXTENSION pgcrypto;

-- Encrypt column
INSERT INTO customers (ssn_encrypted) 
VALUES (pgp_sym_encrypt('123-45-6789', 'secret_key'));

-- Decrypt
SELECT pgp_sym_decrypt(ssn_encrypted, 'secret_key') FROM customers;

-- Redshift: Automatic encryption
CREATE TABLE customers (
    customer_id INT,
    name VARCHAR(100),
    ssn VARCHAR(20)
) ENCODE AUTO;

-- BigQuery: Column-level encryption
SELECT 
    customer_id,
    DECRYPT_COLUMN(name, 'name', 'projects/key-ring/cryptoKeys/key') as decrypted_name
FROM customers;
```

### Encryption in Transit

```python
# SSL/TLS for database connections
import psycopg2

conn = psycopg2.connect(
    host="warehouse.redshift.amazonaws.com",
    port=5439,
    dbname="analytics",
    user="admin",
    password="secret",
    sslmode="require",
    sslrootcert="/path/to/ca-certificate.crt"
)

# Kafka SSL
properties = {
    'bootstrap.servers': 'kafka:9093',
    'security.protocol': 'SSL',
    'ssl.truststore.location': '/path/to/truststore.jks',
    'ssl.keystore.location': '/path/to/keystore.jks'
}
```

### Key Management Platforms

| Platform | Type | Key Features |
|----------|------|--------------|
| **AWS KMS** | Cloud | Integrated with AWS services |
| **Azure Key Vault** | Cloud | HSM-backed, Azure integration |
| **Google Cloud KMS** | Cloud | Cloud HSM, key rotation |
| **HashiCorp Vault** | Self-hosted | Dynamic secrets, encryption |
| **CyberArk** | Enterprise | Privileged access management |

### Key Management

```python
# AWS KMS
import boto3

kms = boto3.client('kms')

# Create key
response = kms.create_key(
    Description='Data warehouse encryption key',
    Tags=[{'TagKey': 'Environment', 'TagValue': 'Production'}]
)

# Encrypt
encrypted = kms.encrypt(
    KeyId=response['KeyMetadata']['KeyId'],
    Plaintext='sensitive-data'
)

# Decrypt
decrypted = kms.decrypt(CiphertextBlob=encrypted['CiphertextBlob'])
```

---

## 3. Access Control

### Role-Based Access Control (RBAC)

```sql
-- PostgreSQL roles
CREATE ROLE data_analyst;
CREATE ROLE data_engineer;
CREATE ROLE data_admin;

-- Grant permissions
GRANT USAGE ON SCHEMA analytics TO data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO data_analyst;

GRANT ALL PRIVILEGES ON SCHEMA staging TO data_engineer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA staging TO data_engineer;

GRANT ALL PRIVILEGES ON DATABASE warehouse TO data_admin;

-- Create user and assign role
CREATE USER john WITH PASSWORD 'secure_password';
GRANT data_analyst TO john;

-- Row-level security
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;

CREATE POLICY customer_region_policy ON customers
    USING (region = current_setting('app.user_region'));

-- Column-level security
GRANT SELECT ON customers (customer_id, name, email) TO data_analyst;
-- data_analyst cannot see: ssn, phone, address
```

### Access Control Tools

| Tool | Type | Description |
|------|------|-------------|
| **Okta** | IAM | Identity and access management |
| **Auth0** | IAM | Authentication as a service |
| **AWS IAM** | Cloud | AWS access management |
| **Google Cloud IAM** | Cloud | GCP access management |
| **Azure AD** | Cloud | Microsoft identity platform |
| **OpenLDAP** | Self-hosted | Open source directory service |

### Attribute-Based Access Control (ABAC)

```python
# Python implementation
def check_access(user, resource, action):
    """ABAC access control"""
    # Check user attributes
    if user.department not in resource.allowed_departments:
        return False
    
    # Check resource classification
    if resource.classification == 'RESTRICTED' and user.clearance_level < 3:
        return False
    
    # Check time-based restrictions
    if resource.time_restricted and not is_business_hours():
        return False
    
    # Check location
    if resource.location_restricted and user.country not in resource.allowed_countries:
        return False
    
    return True
```

---

## 4. Compliance Regulations

### GDPR (General Data Protection Regulation)

```sql
-- Right to be forgotten
DELETE FROM customers WHERE customer_id = 'C001';
-- Also need to delete from all downstream tables

-- Data portability
SELECT * FROM customers WHERE customer_id = 'C001' 
FOR JSON PATH;

-- Consent management
CREATE TABLE customer_consents (
    customer_id INT,
    consent_type VARCHAR(50),
    consent_date TIMESTAMP,
    expiry_date TIMESTAMP,
    is_active BOOLEAN
);
```

### HIPAA (Health Insurance Portability and Accountability Act)

```sql
-- PHI (Protected Health Information) handling
CREATE TABLE patient_records (
    patient_id INT,
    name VARCHAR(100),
    ssn VARCHAR(20),
    diagnosis_code VARCHAR(10),
    treatment_date DATE,
    provider VARCHAR(100)
) ENCODE AUTO;  -- Redshift automatic encryption

-- Audit logging
CREATE TABLE access_log (
    access_id SERIAL PRIMARY KEY,
    user_id INT,
    patient_id INT,
    access_type VARCHAR(20),
    access_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ip_address INET
);

-- Create audit trigger
CREATE OR REPLACE FUNCTION log_patient_access()
RETURNS TRIGGER AS 
BEGIN
    INSERT INTO access_log (user_id, patient_id, access_type)
    VALUES (current_setting('app.user_id')::INT, NEW.patient_id, TG_OP);
    RETURN NEW;
END;
 LANGUAGE plpgsql;

CREATE TRIGGER trg_patient_access
AFTER SELECT ON patient_records
FOR EACH ROW EXECUTE FUNCTION log_patient_access();
```

### Data Masking

```sql
-- Static masking (permanent)
UPDATE customers SET 
    ssn = CONCAT('XXX-XX-', RIGHT(ssn, 4)),
    email = CONCAT('user', customer_id, '@masked.com');

-- Dynamic masking (at query time)
CREATE MASKING POLICY ssn_mask ON customers
    AS (val VARCHAR) RETURN VARCHAR
    CASE 
        WHEN current_setting('app.user_role') = 'admin' THEN val
        ELSE CONCAT('XXX-XX-', RIGHT(val, 4))
    END;

-- Tokenization
CREATE TABLE tokenization_map (
    token VARCHAR(50) PRIMARY KEY,
    original_value VARCHAR(200),
    column_name VARCHAR(50),
    created_at TIMESTAMP
);
```

---

## 5. Real-World Scenarios

### Scenario 1: Healthcare Data Security (HIPAA)

```
Security Architecture:

+------------------+     +------------------+     +------------------+
| Network Layer    |     | Application Layer|     | Data Layer       |
| - VPC isolation  |     | - MFA/SSO        |     | - AES-256 at rest|
| - Private subnet |     | - RBAC + ABAC    |     | - TLS 1.3 transit|
| - WAF            |     | - Audit logging  |     | - Column-level   |
| - VPN for remote |     | - Session mgmt   |     |   encryption     |
+------------------+     +------------------+     +------------------+
                              |                           |
                              v                           v
                    +------------------+         +------------------+
                    | Monitoring       |         | Compliance       |
                    | - SIEM           |         | - HIPAA controls |
                    | - Anomaly detect |         | - Audit reports  |
                    | - Alerting       |         | - Risk assessment|
                    +------------------+         +------------------+
```

### Scenario 2: Financial Data Security (PCI DSS)

```
Cardholder Data Environment:

1. Network Segmentation
   - Isolate CDE from other networks
   - Firewall rules restrict access

2. Access Control
   - MFA for all access
   - Least privilege principle
   - Regular access reviews

3. Encryption
   - AES-256 for stored card data
   - TLS 1.3 for transmission
   - Key rotation every 90 days

4. Monitoring
   - Real-time fraud detection
   - Audit logging all access
   - Quarterly vulnerability scans
```

---

## 6. Hands-On Exercises

### Exercise 1: Column-Level Encryption
```sql
-- Task: Implement column-level encryption for PII

-- Create extension
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create table with encrypted columns
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    ssn_encrypted BYTEA,  -- Encrypted
    phone_encrypted BYTEA, -- Encrypted
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert with encryption
INSERT INTO customers (name, email, ssn_encrypted, phone_encrypted)
VALUES (
    'John Doe',
    'john@example.com',
    pgp_sym_encrypt('123-45-6789', 'secret_key'),
    pgp_sym_encrypt('555-123-4567', 'secret_key')
);

-- Query with decryption
SELECT 
    customer_id,
    name,
    email,
    pgp_sym_decrypt(ssn_encrypted, 'secret_key') as ssn,
    pgp_sym_decrypt(phone_encrypted, 'secret_key') as phone
FROM customers;

-- Create view for authorized users
CREATE VIEW customers_masked AS
SELECT 
    customer_id,
    name,
    email,
    CONCAT('XXX-XX-', RIGHT(pgp_sym_decrypt(ssn_encrypted, 'secret_key'), 4)) as ssn_masked
FROM customers;

-- Test
SELECT * FROM customers_masked;
```

### Exercise 2: Role-Based Access Control
```sql
-- Task: Implement RBAC with row-level security

-- Create roles
CREATE ROLE sales_analyst;
CREATE ROLE finance_analyst;
CREATE ROLE admin;

-- Create table with region data
CREATE TABLE sales_data (
    sale_id SERIAL PRIMARY KEY,
    region VARCHAR(20),
    amount DECIMAL(10,2),
    sale_date DATE
);

-- Insert sample data
INSERT INTO sales_data (region, amount, sale_date) VALUES
('North', 1000, '2024-01-15'),
('South', 1500, '2024-01-16'),
('North', 2000, '2024-01-17'),
('East', 1200, '2024-01-18');

-- Enable row-level security
ALTER TABLE sales_data ENABLE ROW LEVEL SECURITY;

-- Create policies
CREATE POLICY sales_analyst_policy ON sales_data
    FOR SELECT
    TO sales_analyst
    USING (region = current_setting('app.user_region'));

CREATE POLICY finance_analyst_policy ON sales_data
    FOR SELECT
    TO finance_analyst
    USING (true);  -- Finance can see all regions

-- Grant permissions
GRANT SELECT ON sales_data TO sales_analyst;
GRANT SELECT ON sales_data TO finance_analyst;

-- Test as sales_analyst (North region)
SET ROLE sales_analyst;
SET app.user_region = 'North';
SELECT * FROM sales_data;  -- Only North region visible
RESET ROLE;

-- Test as finance_analyst (all regions)
SET ROLE finance_analyst;
SELECT * FROM sales_data;  -- All regions visible
RESET ROLE;
```

### Exercise 3: Data Masking Implementation
```python
# Task: Implement data masking for non-production environments

import re
from typing import Any


class DataMasker:
    def __init__(self):
        self.masking_rules = {
            'ssn': self.mask_ssn,
            'email': self.mask_email,
            'phone': self.mask_phone,
            'credit_card': self.mask_credit_card,
            'name': self.mask_name
        }
    
    def mask_ssn(self, value: str) -> str:
        """Mask SSN except last 4 digits."""
        if len(value) == 11:  # XXX-XX-XXXX
            return f"XXX-XX-{value[-4:]}"
        return "XXX-XX-XXXX"
    
    def mask_email(self, value: str) -> str:
        """Mask email username."""
        if '@' in value:
            username, domain = value.split('@')
            return f"{username[0]}***@{domain}"
        return "***@***.com"
    
    def mask_phone(self, value: str) -> str:
        """Mask phone number except last 4 digits."""
        digits = re.sub(r'\D', '', value)
        if len(digits) >= 4:
            return f"(***) ***-{digits[-4:]}"
        return "(***) ***-****"
    
    def mask_credit_card(self, value: str) -> str:
        """Mask credit card except last 4 digits."""
        digits = re.sub(r'\D', '', value)
        if len(digits) >= 4:
            return f"****-****-****-{digits[-4:]}"
        return "****-****-****-****"
    
    def mask_name(self, value: str) -> str:
        """Mask name except first letter."""
        if len(value) > 1:
            return f"{value[0]}{'*' * (len(value) - 1)}"
        return value
    
    def mask_record(self, record: dict, columns_to_mask: list) -> dict:
        """Mask specified columns in a record."""
        masked = record.copy()
        for col in columns_to_mask:
            if col in masked and masked[col]:
                # Determine masking function based on column name
                for key, func in self.masking_rules.items():
                    if key in col.lower():
                        masked[col] = func(str(masked[col]))
                        break
        return masked


# Test masking
def test_masking():
    masker = DataMasker()
    
    record = {
        'customer_id': 123,
        'name': 'John Doe',
        'email': 'john.doe@example.com',
        'ssn': '123-45-6789',
        'phone': '555-123-4567',
        'credit_card': '4111-1111-1111-1234'
    }
    
    masked = masker.mask_record(record, ['name', 'email', 'ssn', 'phone', 'credit_card'])
    
    print("Original:")
    for k, v in record.items():
        print(f"  {k}: {v}")
    
    print("\nMasked:")
    for k, v in masked.items():
        print(f"  {k}: {v}")

test_masking()
```

### Exercise 4: Audit Logging
```sql
-- Task: Implement comprehensive audit logging

-- Create audit log table
CREATE TABLE audit_log (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    record_id INT,
    action VARCHAR(10),  -- INSERT, UPDATE, DELETE
    old_values JSONB,
    new_values JSONB,
    user_name VARCHAR(100),
    user_ip INET,
    action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create audit function
CREATE OR REPLACE FUNCTION audit_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log (table_name, record_id, action, new_values, user_name)
        VALUES (TG_TABLE_NAME, NEW.id, 'INSERT', to_jsonb(NEW), current_user);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, user_name)
        VALUES (TG_TABLE_NAME, NEW.id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW), current_user);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values, user_name)
        VALUES (TG_TABLE_NAME, OLD.id, 'DELETE', to_jsonb(OLD), current_user);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create audit trigger on sensitive table
CREATE TRIGGER audit_customers
    AFTER INSERT OR UPDATE OR DELETE ON customers
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();

-- Query audit log
SELECT * FROM audit_log 
WHERE table_name = 'customers' 
ORDER BY action_timestamp DESC 
LIMIT 10;

-- Find all changes to a specific record
SELECT * FROM audit_log 
WHERE table_name = 'customers' AND record_id = 123
ORDER BY action_timestamp;
```

---

## 7. Interview Questions

### Q1: Explain encryption at rest vs encryption in transit.

**Answer:** 

**Encryption at rest** protects data stored on disk (databases, files, backups). Uses algorithms like AES-256. Protects against physical theft or unauthorized disk access. 

**Encryption in transit** protects data moving between systems (client to database, service to service). Uses TLS/SSL. Protects against network sniffing and man-in-the-middle attacks. Both are required for comprehensive security. Never assume one covers the other - implement both.

### Q2: What is the difference between RBAC and ABAC?

**Answer:** 

**RBAC (Role-Based):** Access based on assigned roles (analyst, engineer, admin). Simple to implement and manage. Good for stable organizational structures. 

**ABAC (Attribute-Based):** Access based on attributes of user, resource, environment (department, classification, time, location). More flexible and fine-grained. Better for complex policies. Most modern systems combine both: RBAC for base access, ABAC for additional constraints. Example: Analyst role (RBAC) can only access their region's data (ABAC).

### Q3: How do you implement GDPR compliance in a data warehouse?

**Answer:** 

Key requirements: 

1) **Data minimization:** Only collect necessary data. 

2) **Consent tracking:** Store consent records with timestamps. 

3) **Right to erasure:** Implement cascading deletes across all tables/systems. 

4) **Data portability:** Export customer data in standard format (JSON/CSV). 

5) **Anonymization:** Mask PII for analytics. 

6) **Audit logging:** Track all data access. 

7) **Data retention:** Automate deletion after retention period. 

8) **Privacy by design:** Build privacy into data models from start.

### Q4: Describe a data security architecture for a financial institution.

**Answer:** 

Multi-layer approach: 

1) **Network:** VPC isolation, firewalls, VPN for remote access. 

2) **Authentication:** MFA, SSO via SAML/OIDC, service accounts for applications. 

3) **Authorization:** RBAC + ABAC, least privilege, separation of duties. 

4) **Encryption:** AES-256 at rest, TLS 1.3 in transit, KMS for key management. 

5) **Data protection:** Column-level encryption for PII, dynamic data masking, tokenization. 

6) **Monitoring:** Audit logging, SIEM integration, anomaly detection. 

7) **Compliance:** GDPR, SOX, PCI DSS controls, regular audits.

### Q5: How do you handle data masking in a data warehouse?

**Answer:** 

Several approaches: 

1) **Static masking:** Permanently transform data (for non-production environments). 

2) **Dynamic masking:** Apply masking at query time based on user role (production). 

3) **Tokenization:** Replace sensitive values with tokens (reversible with key). 

4) **Pseudonymization:** Replace identifiers with pseudonyms (can be reversed). 

5) **Aggregation:** Show only aggregated values (never individual records). 

6) **Nulling:** Replace with NULL for unauthorized users. 

Tools: Redshift column masking, BigQuery column-level security, custom views with CASE logic.

### Q6: What is Zero Trust architecture and how does it apply to data security?

**Answer:** Zero Trust assumes no implicit trust - every access request must be verified. Key principles:
1. **Verify explicitly:** Authenticate and authorize every request
2. **Least privilege access:** Minimal permissions needed
3. **Assume breach:** Design for compromise

Data implementation:
- Micro-segmentation of data stores
- Identity-based access (not network-based)
- Continuous monitoring and validation
- Encryption everywhere (at rest and in transit)
- Regular access reviews and cleanup

### Q7: How do you secure a data lake?

**Answer:** Multi-layer approach:
1. **Storage:** Encryption at rest (S3 SSE-KMS), versioning enabled
2. **Access:** IAM policies, bucket policies, VPC endpoints
3. **Network:** Private subnets, no public access, VPN for remote
4. **Data:** Column-level encryption for PII, data classification
5. **Monitoring:** CloudTrail logging, access alerts, anomaly detection
6. **Governance:** Data catalog with classification tags, retention policies
7. **Compliance:** Regular audits, automated compliance checks

---

## Summary Checklist

### Security Fundamentals
- [ ] Understand security layers (Physical, Network, Application, Data)
- [ ] Know security principles (Least Privilege, Defense in Depth, Zero Trust)

### Encryption
- [ ] Implement encryption at rest (AES-256)
- [ ] Configure encryption in transit (TLS/SSL)
- [ ] Use key management systems (KMS, Vault)

### Access Control
- [ ] Implement RBAC for role-based access
- [ ] Configure ABAC for fine-grained control
- [ ] Enable row-level and column-level security

### Compliance
- [ ] Understand GDPR requirements (consent, erasure, portability)
- [ ] Implement HIPAA controls (PHI protection, audit logging)
- [ ] Apply data masking for non-production environments

### Practical Skills
- [ ] Implement column-level encryption
- [ ] Build audit logging systems
- [ ] Design secure data architectures
- [ ] Handle PII and sensitive data properly

---

*Next Section: [15 - Monitoring](../15-Monitoring/README.md)*
