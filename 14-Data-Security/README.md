# 14 - Data Security

## Table of Contents
1. [Security Fundamentals](#1-security-fundamentals)
2. [Encryption](#2-encryption)
3. [Access Control](#3-access-control)
4. [Compliance Regulations](#4-compliance-regulations)
5. [Interview Questions](#5-interview-questions)

---

## 1. Security Fundamentals

### Data Security Layers

`
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
`

### Security Principles

| Principle | Description |
|-----------|-------------|
| **Least Privilege** | Grant minimum permissions needed |
| **Defense in Depth** | Multiple security layers |
| **Separation of Duties** | No single person controls entire process |
| **Need to Know** | Access only what's necessary |
| **Zero Trust** | Never trust, always verify |

---

## 2. Encryption

### Encryption at Rest

`sql
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
`

### Encryption in Transit

`python
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
`

### Key Management

`python
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
`

---

## 3. Access Control

### Role-Based Access Control (RBAC)

`sql
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
`

### Attribute-Based Access Control (ABAC)

`python
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
`

---

## 4. Compliance Regulations

### GDPR (General Data Protection Regulation)

`sql
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
`

### HIPAA (Health Insurance Portability and Accountability Act)

`sql
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
`

### Data Masking

`sql
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
`

---

## 5. Interview Questions

### Q1: Explain encryption at rest vs encryption in transit.

**Answer:** **Encryption at rest** protects data stored on disk (databases, files, backups). Uses algorithms like AES-256. Protects against physical theft or unauthorized disk access. **Encryption in transit** protects data moving between systems (client to database, service to service). Uses TLS/SSL. Protects against network sniffing and man-in-the-middle attacks. Both are required for comprehensive security. Never assume one covers the other - implement both.

### Q2: What is the difference between RBAC and ABAC?

**Answer:** **RBAC (Role-Based):** Access based on assigned roles (analyst, engineer, admin). Simple to implement and manage. Good for stable organizational structures. **ABAC (Attribute-Based):** Access based on attributes of user, resource, environment (department, classification, time, location). More flexible and fine-grained. Better for complex policies. Most modern systems combine both: RBAC for base access, ABAC for additional constraints. Example: Analyst role (RBAC) can only access their region's data (ABAC).

### Q3: How do you implement GDPR compliance in a data warehouse?

**Answer:** Key requirements: 1) **Data minimization:** Only collect necessary data. 2) **Consent tracking:** Store consent records with timestamps. 3) **Right to erasure:** Implement cascading deletes across all tables/systems. 4) **Data portability:** Export customer data in standard format (JSON/CSV). 5) **Anonymization:** Mask PII for analytics. 6) **Audit logging:** Track all data access. 7) **Data retention:** Automate deletion after retention period. 8) **Privacy by design:** Build privacy into data models from start.

### Q4: Describe a data security architecture for a financial institution.

**Answer:** Multi-layer approach: 1) **Network:** VPC isolation, firewalls, VPN for remote access. 2) **Authentication:** MFA, SSO via SAML/OIDC, service accounts for applications. 3) **Authorization:** RBAC + ABAC, least privilege, separation of duties. 4) **Encryption:** AES-256 at rest, TLS 1.3 in transit, KMS for key management. 5) **Data protection:** Column-level encryption for PII, dynamic data masking, tokenization. 6) **Monitoring:** Audit logging, SIEM integration, anomaly detection. 7) **Compliance:** GDPR, SOX, PCI DSS controls, regular audits.

### Q5: How do you handle data masking in a data warehouse?

**Answer:** Several approaches: 1) **Static masking:** Permanently transform data (for non-production environments). 2) **Dynamic masking:** Apply masking at query time based on user role (production). 3) **Tokenization:** Replace sensitive values with tokens (reversible with key). 4) **Pseudonymization:** Replace identifiers with pseudonyms (can be reversed). 5) **Aggregation:** Show only aggregated values (never individual records). 6) **Nulling:** Replace with NULL for unauthorized users. Tools: Redshift column masking, BigQuery column-level security, custom views with CASE logic.

---

*Next Section: [15 - Monitoring](../15-Monitoring/README.md)*
