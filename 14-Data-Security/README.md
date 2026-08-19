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

### What is Encryption?

**Encryption** is the process of converting data into a coded format that can only be read with the correct decryption key. It protects data from unauthorized access even if the storage介质 or network is compromised.

### Encryption at Rest

**Encryption at Rest** protects data stored on disk (databases, files, backups, data lakes). The data is encrypted when written and decrypted when read.

```
How Encryption at Rest Works:

Plaintext Data  -->  Encryption Algorithm  -->  Ciphertext (Encrypted)
                         + Key                     Stored on Disk

Ciphertext  -->  Decryption Algorithm  -->  Plaintext Data
   (Read)            + Key                   (Used by Application)
```

#### Why Encryption at Rest?

| Threat | Protection |
|--------|------------|
| **Physical theft** | Stolen hard drive is unreadable without key |
| **Unauthorized access** | Database admin cannot read encrypted columns |
| **Backup exposure** | Backups are encrypted even if copied |
| **Compliance** | Meets GDPR, HIPAA, PCI DSS requirements |

#### Banking Example - Account Data at Rest

```sql
-- PostgreSQL: Encrypt sensitive account data
CREATE EXTENSION pgcrypto;

-- Bank account table with encrypted columns
CREATE TABLE bank_accounts (
    account_id SERIAL PRIMARY KEY,
    account_number_encrypted BYTEA,  -- Encrypted account number
    ssn_encrypted BYTEA,             -- Encrypted SSN
    balance DECIMAL(15,2),           -- Plain (for queries)
    customer_name VARCHAR(100),      -- Plain (non-sensitive)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert with encryption
INSERT INTO bank_accounts (account_number_encrypted, ssn_encrypted, balance, customer_name)
VALUES (
    pgp_sym_encrypt('1234567890', 'bank_master_key'),  -- Account number
    pgp_sym_encrypt('123-45-6789', 'bank_master_key'),  -- SSN
    50000.00,
    'John Smith'
);

-- Query with decryption (authorized users only)
SELECT 
    account_id,
    pgp_sym_decrypt(account_number_encrypted, 'bank_master_key') as account_number,
    pgp_sym_decrypt(ssn_encrypted, 'bank_master_key') as ssn,
    balance,
    customer_name
FROM bank_accounts;
```

#### AWS S3 Encryption Example

```python
import boto3

s3 = boto3.client('s3')

# Upload with server-side encryption
s3.put_object(
    Bucket='bank-data-lake',
    Key='accounts/2024/01/transactions.parquet',
    Body=parquet_data,
    ServerSideEncryption='aws:kms',  # Use AWS KMS
    SSEKMSKeyId='alias/bank-key'     # KMS key alias
)

# Download (decryption is automatic)
response = s3.get_object(
    Bucket='bank-data-lake',
    Key='accounts/2024/01/transactions.parquet'
)
plaintext_data = response['Body'].read()  # Automatically decrypted
```

---

### Encryption in Transit

**Encryption in Transit** protects data moving between systems (client to database, service to service, application to API). Uses TLS/SSL protocols.

```
How Encryption in Transit Works:

Client  -->  TLS Handshake  -->  Encrypted Channel  -->  Server
              (Negotiate)        (Secure Tunnel)      (Decrypt)

Data flows through encrypted tunnel
Protection: Network sniffing, Man-in-the-middle attacks
```

#### Why Encryption in Transit?

| Threat | Protection |
|--------|------------|
| **Network sniffing** | Intercepted packets are unreadable |
| **Man-in-the-middle** | Cannot intercept or modify data |
| **Public WiFi** | Safe to use untrusted networks |
| **Compliance** | PCI DSS requires TLS for card data |

#### Banking Example - Secure Database Connection

```python
# Python: Connect to bank database with TLS
import psycopg2

# Secure connection to bank database
conn = psycopg2.connect(
    host="bank-database.example.com",
    port=5432,
    dbname="banking",
    user="app_service",
    password="secure_password",
    sslmode='verify-full',  # Require TLS + verify certificate
    sslrootcert='/path/to/bank-ca-certificate.crt',
    sslcert='/path/to/client-certificate.crt',
    sslkey='/path/to/client-private-key.pem'
)

# All queries are now encrypted in transit
cursor = conn.cursor()
cursor.execute("SELECT balance FROM accounts WHERE account_id = %s", (12345,))
```

#### Banking API Example

```python
# HTTPS API call to banking service
import requests

# Secure API call (TLS 1.3)
response = requests.get(
    'https://api.bank.com/accounts/12345/balance',
    headers={
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIs...',
        'Content-Type': 'application/json'
    },
    verify='/path/to/bank-ca-certificate.crt'  # Verify server certificate
)

# Response is decrypted automatically
account_balance = response.json()
```

---

### Encryption at Rest vs Encryption in Transit: Comparison

| Aspect | Encryption at Rest | Encryption in Transit |
|--------|-------------------|----------------------|
| **When** | Data stored on disk | Data moving between systems |
| **Protects Against** | Physical theft, unauthorized disk access | Network sniffing, MITM attacks |
| **Technology** | AES-256, envelope encryption | TLS 1.2/1.3, SSL |
| **Performance** | Minimal impact (encrypt/decrypt once) | Minimal impact (hardware acceleration) |
| **Compliance** | GDPR, HIPAA, PCI DSS | PCI DSS, HIPAA |
| **Example** | Encrypt database column | HTTPS connection to API |

### Banking: Both Are Required

```
Complete Banking Security:

User's Phone  -->  [TLS 1.3]  -->  Bank API  -->  [TLS 1.3]  -->  Database
   (App)           (Encrypted        (Server)      (Encrypted      (AES-256
                    in Transit)                     in Transit)     at Rest)

1. User opens banking app
2. App connects to bank API via HTTPS (TLS 1.3)
3. API queries database (TLS 1.3)
4. Database reads encrypted data (AES-256 at rest)
5. Data flows back through encrypted channels

Result: Data is protected at every stage
```

### Key Management Platforms

| Platform | Type | Key Features |
|----------|------|--------------|
| **AWS KMS** | Cloud | Integrated with AWS services |
| **Azure Key Vault** | Cloud | HSM-backed, Azure integration |
| **Google Cloud KMS** | Cloud | Cloud HSM, key rotation |
| **HashiCorp Vault** | Self-hosted | Dynamic secrets, encryption |
| **CyberArk** | Enterprise | Privileged access management |

### AWS KMS vs HashiCorp Vault: Key Differences

| Aspect | AWS KMS | HashiCorp Vault |
|--------|---------|------------------|
| **Primary Purpose** | Generates and manages encryption keys (customer master keys) to encrypt data directly inside AWS | Stores application secrets (passwords, API keys) and generates dynamic, temporary credentials for databases and services |
| **Deployment** | Managed service (AWS cloud) | Self-hosted or HCP (HashiCorp Cloud Platform) |
| **Key Storage** | Hardware Security Modules (HSMs) managed by AWS | Backend storage (Consul, Raft, etc.) |
| **Secret Types** | Encryption keys only | Passwords, API keys, certificates, dynamic credentials |
| **Dynamic Secrets** | ❌ No | ✅ Yes (generates temporary DB credentials) |
| **Encryption** | ✅ Yes (envelope encryption) | ✅ Yes (Transit engine) |
| **Cloud Integration** | Native AWS integration | Multi-cloud, works anywhere |
| **Cost** | Per key + per API call | Free (open-source) or HCP pricing |

### When to Use AWS KMS

| Scenario | Why AWS KMS |
|----------|-------------|
| Encrypting data in S3, EBS, RDS | Native integration with AWS services |
| Envelope encryption for applications | Generate data keys for client-side encryption |
| AWS-native workloads | Seamless integration, managed HSM |
| Compliance requirements (FIPS 140-2) | AWS KMS is FIPS certified |

### When to Use HashiCorp Vault

| Scenario | Why HashiCorp Vault |
|----------|---------------------|
| Multi-cloud environments | Works across AWS, GCP, Azure |
| Dynamic database credentials | Generates temporary, rotatable credentials |
| Centralized secret management | Single source of truth for all secrets |
| On-premise or hybrid deployments | Self-hosted option available |
| Complex access policies | Fine-grained ACL with multiple auth methods |

---

### Real-World Banking Scenario: AWS KMS

**Use Case:** A bank needs to encrypt sensitive customer data (account numbers, SSNs, transaction history) stored in AWS S3 and RDS databases.

```python
import boto3
from datetime import datetime

# Initialize KMS client
kms = boto3.client('kms', region_name='us-east-1')

# ============================================================
# SCENARIO 1: Create encryption keys for different data types
# ============================================================

# Create master key for customer PII
pii_key = kms.create_key(
    Description='Master key for customer PII (SSN, account numbers)',
    KeyUsage='ENCRYPT_DECRYPT',
    Origin='AWS_KMS',
    Tags=[
        {'TagKey': 'DataClassification', 'TagValue': 'PII'},
        {'TagKey': 'Environment', 'TagValue': 'Production'},
        {'TagKey': 'Compliance', 'TagValue': 'PCI-DSS'}
    ]
)
pii_key_id = pii_key['KeyMetadata']['KeyId']
print(f"PII Key created: {pii_key_id}")

# Create key for transaction data
txn_key = kms.create_key(
    Description='Master key for transaction data',
    Tags=[
        {'TagKey': 'DataClassification', 'TagValue': 'Confidential'},
        {'TagKey': 'Environment', 'TagValue': 'Production'}
    ]
)
txn_key_id = txn_key['KeyMetadata']['KeyId']
print(f"Transaction Key created: {txn_key_id}")

# Create alias for easy reference
kms.create_alias(
    AliasName='alias/bank-pii-master-key',
    TargetKeyId=pii_key_id
)

kms.create_alias(
    AliasName='alias/bank-txn-master-key',
    TargetKeyId=txn_key_id
)

# ============================================================
# SCENARIO 2: Encrypt sensitive customer data
# ============================================================

def encrypt_customer_data(customer_data):
    """Encrypt customer PII before storing in database."""
    
    # Encrypt account number
    account_encrypted = kms.encrypt(
        KeyId='alias/bank-pii-master-key',
        Plaintext=customer_data['account_number'].encode(),
        EncryptionContext={
            'Purpose': 'AccountNumberEncryption',
            'CustomerId': customer_data['customer_id']
        }
    )
    
    # Encrypt SSN
    ssn_encrypted = kms.encrypt(
        KeyId='alias/bank-pii-master-key',
        Plaintext=customer_data['ssn'].encode(),
        EncryptionContext={
            'Purpose': 'SSNEncryption',
            'CustomerId': customer_data['customer_id']
        }
    )
    
    return {
        'customer_id': customer_data['customer_id'],
        'account_number_encrypted': account_encrypted['CiphertextBlob'],
        'ssn_encrypted': ssn_encrypted['CiphertextBlob'],
        'account_number_key_id': account_encrypted['KeyId'],
        'ssn_key_id': ssn_encrypted['KeyId']
    }

# Example usage
customer = {
    'customer_id': 'CUST-001',
    'account_number': '1234567890',
    'ssn': '123-45-6789'
}

encrypted_customer = encrypt_customer_data(customer)
print(f"Encrypted account: {encrypted_customer['account_number_encrypted'][:50]}...")

# ============================================================
# SCENARIO 3: Decrypt data for authorized access
# ============================================================

def decrypt_customer_data(encrypted_data):
    """Decrypt customer PII for authorized access."""
    
    # Decrypt account number
    account_decrypted = kms.decrypt(
        CiphertextBlob=encrypted_data['account_number_encrypted'],
        EncryptionContext={
            'Purpose': 'AccountNumberEncryption',
            'CustomerId': encrypted_data['customer_id']
        }
    )
    
    # Decrypt SSN
    ssn_decrypted = kms.decrypt(
        CiphertextBlob=encrypted_data['ssn_encrypted'],
        EncryptionContext={
            'Purpose': 'SSNEncryption',
            'CustomerId': encrypted_data['customer_id']
        }
    )
    
    return {
        'customer_id': encrypted_data['customer_id'],
        'account_number': account_decrypted['Plaintext'].decode(),
        'ssn': ssn_decrypted['Plaintext'].decode()
    }

# ============================================================
# SCENARIO 4: Encrypt data in S3 (Server-Side Encryption)
# ============================================================
s3 = boto3.client('s3')

# Upload bank statements with encryption
s3.put_object(
    Bucket='bank-statements-bucket',
    Key='statements/2024/01/CUST-001-statement.pdf',
    Body=pdf_content,
    ServerSideEncryption='aws:kms',
    SSEKMSKeyId='alias/bank-pii-master-key',
    Metadata={
        'customer-id': 'CUST-001',
        'statement-month': '2024-01'
    }
)

# ============================================================
# SCENARIO 5: Key rotation for compliance
# ============================================================

# Enable automatic key rotation (rotates every year)
kms.enable_key_rotation(KeyId=pii_key_id)
print("Key rotation enabled for PII key")

# Manual key rotation (if needed)
kms.schedule_key_deletion(
    KeyId='old-key-id',
    PendingWindowInDays=7
)

# ============================================================
# SCENARIO 6: Audit key usage for compliance
# ============================================================n
# CloudTrail logs all KMS API calls automatically
# Query with CloudTrail API
cloudtrail = boto3.client('cloudtrail')

response = cloudtrail.lookup_events(
    LookupAttributes=[
        {
            'AttributeKey': 'EventName',
            'AttributeValue': 'Decrypt'
        }
    ],
    StartTime=datetime(2024, 1, 1),
    EndTime=datetime(2024, 1, 31),
    MaxResults=100
)

for event in response['Events']:
    print(f"Decrypt event: {event['EventTime']} - {event['Username']}")
```

**Banking Compliance Notes:**
- KMS keys are stored in FIPS 140-2 Level 2 validated HSMs
- All key usage is logged to CloudTrail for audit
- Encryption context provides additional authenticated data
- Key rotation ensures compliance with PCI DSS requirements

---

### Real-World Banking Scenario: HashiCorp Vault

**Use Case:** A bank needs to manage database credentials, API keys, and certificates for its banking applications across multiple environments.

---

#### Scenario 1: Store Static Secrets (API Keys, Passwords)

**What it does:** Stores long-lived secrets like API keys, passwords, and configuration values in Vault's encrypted KV (Key-Value) secrets engine.

**Why use it:** Instead of hardcoding secrets in config files or environment variables (which can be leaked), store them securely in Vault with access controls and audit logging.

**Banking Example:**
- Core banking API key for transaction processing
- Payment gateway credentials (Stripe, Square, etc.)
- Email service passwords for sending notifications
- Third-party service API keys

```python
import hvac  # HashiCorp Vault Python client
import json
from datetime import datetime

# Initialize Vault client
client = hvac.Client(
    url='https://vault.bank.internal:8200',
    token='your-vault-token'  # Or use AppRole auth
)

# Store core banking API key
client.secrets.kv.v2.create_or_update_secret(
    path='banking/core-banking-api',
    secret={
        'api_key': 'cb-api-key-xyz123',
        'api_secret': 'super-secret-api-secret',
        'base_url': 'https://core-bank.internal/api/v2',
        'timeout_seconds': 30,
        'retry_count': 3
    },
    mount_point='secret'
)

# Store payment gateway credentials
client.secrets.kv.v2.create_or_update_secret(
    path='banking/payment-gateway',
    secret={
        'merchant_id': 'MERCH-001',
        'api_key': 'pg-api-key-abc456',
        'api_secret': 'pg-secret-def789',
        'webhook_secret': 'webhook-ghi012',
        'environment': 'production'
    },
    mount_point='secret'
)

# Store email service credentials
client.secrets.kv.v2.create_or_update_secret(
    path='banking/email-service',
    secret={
        'smtp_host': 'smtp.bank.internal',
        'smtp_port': 587,
        'username': 'notifications@bank.com',
        'password': 'email-password-secure',
        'from_address': 'notifications@bank.com'
    },
    mount_point='secret'
)

print("Static secrets stored in Vault")
```

---

#### Scenario 2: Dynamic Database Credentials

**What it does:** Vault generates **temporary, unique database credentials** for each application instance or user. These credentials automatically expire after a set time (TTL).

**Why use it:** Instead of sharing one database password across all applications (risky!), each app gets its own credentials that:
- Expire automatically (no need to rotate manually)
- Are unique (if compromised, only one app is affected)
- Are audit-logged (know who accessed what)

**Banking Example - Different TTLs for Different Access Levels:**

| Role | Access Level | TTL (Time-to-Live) | Use Case |
|------|-------------|-------------------|----------|
| `banking-app-read` | SELECT only | **2 hours** | Dashboard queries, reports |
| `banking-app-write` | SELECT, INSERT, UPDATE | **1 hour** | Transaction processing |
| `banking-app-admin` | Full access | **30 minutes** | Schema changes, migrations |
| `banking-report-read` | SELECT only | **4 hours** | End-of-day reports |

```python
# Configure MySQL secrets engine
client.secrets.database.configure(
    name='banking-mysql',
    plugin_name='mysql-database-plugin',
    connection_url='{{username}}:{{password}}@bank-db.internal:3306/banking',
    allowed_roles=['banking-app-read', 'banking-app-write', 'banking-app-admin'],
    username='vault-admin',
    password='vault-admin-password'
)

# Create role for READ-ONLY access (expires in 2 hours)
client.secrets.database.create_role(
    name='banking-app-read',
    db_name='banking-mysql',
    default_ttl='2h',   # Credentials expire after 2 hours
    max_ttl='24h',      # Maximum allowed TTL
    creation_statements=[
        "CREATE USER '{{name}}'@'%' IDENTIFIED BY '{{password}}';",
        "GRANT SELECT ON banking.* TO '{{name}}'@'%';",
    ]
)

# Create role for READ-WRITE access (expires in 1 hour)
client.secrets.database.create_role(
    name='banking-app-write',
    db_name='banking-mysql',
    default_ttl='1h',   # Shorter TTL for write access
    max_ttl='2h',
    creation_statements=[
        "CREATE USER '{{name}}'@'%' IDENTIFIED BY '{{password}}';",
        "GRANT SELECT, INSERT, UPDATE ON banking.* TO '{{name}}'@'%';",
        "GRANT DELETE ON banking.transactions TO '{{name}}'@'%';",
    ]
)

# Create role for ADMIN access (expires in 30 minutes)
client.secrets.database.create_role(
    name='banking-app-admin',
    db_name='banking-mysql',
    default_ttl='30m',  # Very short TTL for admin access
    max_ttl='1h',
    creation_statements=[
        "CREATE USER '{{name}}'@'%' IDENTIFIED BY '{{password}}';",
        "GRANT ALL PRIVILEGES ON banking.* TO '{{name}}'@'%';",
    ]
)

# Generate READ credentials (valid for 2 hours)
read_credentials = client.secrets.database.generate_credentials(
    name='banking-app-read'
)

print(f"Generated READ DB credentials:")
print(f"  Username: {read_credentials['data']['username']}")
print(f"  Password: {read_credentials['data']['password']}")
print(f"  TTL: {read_credentials['data']['ttl']} seconds ({read_credentials['data']['ttl'] // 3600} hours)")
print(f"  Expires: Auto-expires after 2 hours!")

# Generate WRITE credentials (valid for 1 hour)
write_credentials = client.secrets.database.generate_credentials(
    name='banking-app-write'
)

print(f"\nGenerated WRITE DB credentials:")
print(f"  Username: {write_credentials['data']['username']}")
print(f"  TTL: {write_credentials['data']['ttl']} seconds ({write_credentials['data']['ttl'] // 60} minutes)")
print(f"  Expires: Auto-expires after 1 hour!")
```

---

#### Scenario 3: Dynamic AWS Credentials

**What it does:** Vault generates **temporary AWS credentials** (access key, secret key, session token) that expire after a set time.

**Why use it:** Instead of using long-lived IAM user credentials (risk of leakage), each application gets temporary credentials that:
- Auto-expire (no manual rotation needed)
- Have minimal permissions (least privilege)
- Are unique per request (no shared credentials)

**Banking Example - Different Access Levels:**

| Role | AWS Permissions | TTL | Use Case |
|------|----------------|-----|----------|
| `bank-s3-reader` | S3 GetObject, ListBucket | **1 hour** | Read bank statements |
| `bank-s3-writer` | S3 PutObject | **30 minutes** | Upload daily reports |
| `bank-kms-user` | KMS Encrypt, Decrypt | **15 minutes** | Encrypt/decrypt sensitive data |
| `bank-lambda-invoker` | Lambda InvokeFunction | **2 hours** | Trigger fraud detection |

```python
# Configure AWS secrets engine
client.secrets.aws.configure(
    access_key='vault-aws-access-key',
    secret_key='vault-aws-secret-key',
    region='us-east-1'
)

# Create role for S3 READ access (expires in 1 hour)
client.secrets.aws.create_role(
    name='bank-s3-reader',
    credential_type='assumed_role',
    default_ttl='1h',   # Credentials expire after 1 hour
    max_ttl='4h',
    policy_document={
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Action': ['s3:GetObject', 's3:ListBucket'],
            'Resource': [
                'arn:aws:s3:::bank-statements-bucket',
                'arn:aws:s3:::bank-statements-bucket/*'
            ]
        }]
    }
)

# Create role for KMS access (expires in 15 minutes)
client.secrets.aws.create_role(
    name='bank-kms-user',
    credential_type='assumed_role',
    default_ttl='15m',  # Very short for sensitive KMS operations
    max_ttl='1h',
    policy_document={
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Action': ['kms:Encrypt', 'kms:Decrypt', 'kms:GenerateDataKey'],
            'Resource': 'arn:aws:kms:us-east-1:123456789012:key/bank-key-id'
        }]
    }
)

# Generate temporary AWS credentials (valid for 1 hour)
aws_credentials = client.secrets.aws.generate_credentials(
    name='bank-s3-reader'
)

print(f"Generated AWS credentials:")
print(f"  Access Key: {aws_credentials['data']['access_key']}")
print(f"  Secret Key: {aws_credentials['data']['secret_key']}")
print(f"  Session Token: {aws_credentials['data']['security_token'][:50]}...")
print(f"  Expires: Auto-expires after 1 hour!")

# Generate KMS credentials (valid for 15 minutes)
kms_credentials = client.secrets.aws.generate_credentials(
    name='bank-kms-user'
)

print(f"\nGenerated KMS credentials:")
print(f"  TTL: Very short (15 minutes) for sensitive operations")
```

---

#### Scenario 4: PKI/TLS Certificates

**What it does:** Vault acts as a **Certificate Authority (CA)** and generates TLS certificates for internal services.

**Why use it:** Instead of manually managing certificates (risky, expiration issues), Vault:
- Auto-generates certificates with proper expiration
- Provides short-lived certificates (1 year vs 3-5 years)
- Centralizes certificate management
- Enables automatic rotation

**Banking Example - Certificate Types:**

| Certificate | TTL | Use Case |
|-------------|-----|----------|
| Root CA | **10 years** | Trust anchor for internal PKI |
| Intermediate CA | **5 years** | Issues leaf certificates |
| API Server Cert | **1 year** | Banking API HTTPS |
| Database Client Cert | **90 days** | mTLS for database connections |
| Internal Service Cert | **1 year** | Service-to-service mTLS |

```python
# Configure PKI secrets engine
client.secrets.pki.configure_tune(
    mount_point='pki',
    max_lease_ttl='87600h'  # 10 years for root CA
)

# Generate root CA (valid for 10 years)
root_cert = client.secrets.pki.generate_root(
    type='internal',
    common_name='Bank Internal Root CA',
    ttl='87600h'  # 10 years
)

# Issue certificate for banking API (valid for 1 year)
api_cert = client.secrets.pki.generate_leaf(
    name='banking-api-cert',
    common_name='api.bank.internal',
    alt_names=['api.bank.internal', 'api-internal.bank.internal'],
    ttl='8760h'  # 1 year
)

print(f"Generated API certificate:")
print(f"  Certificate: {api_cert['data']['certificate'][:100]}...")
print(f"  Private Key: {api_cert['data']['private_key'][:50]}...")
print(f"  Expires: 1 year from now")

# Issue database client certificate (valid for 90 days)
db_cert = client.secrets.pki.generate_leaf(
    name='db-client-cert',
    common_name='db-client.bank.internal',
    ttl='2160h'  # 90 days
)

print(f"\nGenerated DB client certificate:")
print(f"  Expires: 90 days from now (auto-rotate before expiry)")
```

---

#### Scenario 5: Application Retrieves Secrets

**What it does:** On application startup, retrieve all necessary secrets from Vault (static and dynamic).

**Why use it:** Application never stores secrets in code or config files. All secrets come from Vault at runtime.

**Banking Example - Application Startup Flow:**

```
1. App starts
2. Authenticates with Vault (AppRole, Kubernetes, etc.)
3. Retrieves static secrets (API keys)
4. Generates dynamic DB credentials (valid for 2 hours)
5. Generates dynamic AWS credentials (valid for 1 hour)
6. App runs with fresh, unique credentials
7. After TTL expires, app re-authenticates and gets new credentials
```

```python
def get_banking_config(environment='production'):
    """Application retrieves all necessary secrets from Vault."""
    
    # Get static secrets (API keys)
    core_banking = client.secrets.kv.v2.read_secret_version(
        path='banking/core-banking-api',
        mount_point='secret'
    )['data']['data']
    
    payment_gateway = client.secrets.kv.v2.read_secret_version(
        path='banking/payment-gateway',
        mount_point='secret'
    )['data']['data']
    
    # Get dynamic database credentials (valid for 2 hours)
    db_creds = client.secrets.database.generate_credentials(
        name='banking-app-read'
    )['data']
    
    # Get dynamic AWS credentials (valid for 1 hour)
    aws_creds = client.secrets.aws.generate_credentials(
        name='bank-s3-reader'
    )['data']
    
    return {
        'core_banking_api_key': core_banking['api_key'],
        'payment_merchant_id': payment_gateway['merchant_id'],
        'db_host': 'bank-db.internal',
        'db_user': db_creds['username'],
        'db_password': db_creds['password'],
        'db_password_ttl': db_creds['ttl'],  # Seconds until expiry
        'db_name': 'banking',
        'aws_access_key': aws_creds['access_key'],
        'aws_secret_key': aws_creds['secret_key'],
        'aws_session_token': aws_creds['security_token']
    }

# Application startup
config = get_banking_config()
print(f"Application config loaded from Vault")
print(f"  DB User: {config['db_user']}")
print(f"  DB Password expires in: {config['db_password_ttl'] // 60} minutes")
print(f"  AWS credentials expire in: 1 hour")
print(f"  App will auto-refresh credentials before expiry")
```

---

#### Scenario 6: Audit Logging for Compliance

**What it does:** Vault logs **every secret access** - who accessed what, when, and whether it succeeded.

**Why use it:** Banks must prove who accessed sensitive data for compliance (PCI DSS, SOX, GDPR). Vault provides complete audit trail.

**Banking Example - What Gets Logged:**

| Event | Logged Details |
|-------|----------------|
| Secret Read | User, timestamp, secret path, success/failure |
| Secret Write | User, timestamp, secret path, old value (optional) |
| Dynamic Credential Generation | User, role, TTL, database accessed |
| Authentication | User, method (AppRole, LDAP, etc.), IP address |
| Policy Change | Admin who changed policy, what changed |

```python
# Vault audit log is at /vault/audit/audit.log
# Example log entry:
{
    "type": "response",
    "auth": {
        "client_token": "hmac-sha256:xxx",
        "accessor": "hmac-sha256:yyy",
        "display_name": "banking-app-prod",
        "policies": ["banking-app-read"]
    },
    "request": {
        "operation": "read",
        "path": "database/creds/banking-app-read",
        "remote_address": "10.0.1.50"
    },
    "time": "2024-01-15T10:30:00Z"
}

# Query audit logs for compliance
print("\nAudit Log Entry:")
print("  User: banking-app-prod")
print("  Action: Read database credentials")
print("  Path: database/creds/banking-app-read")
print("  IP: 10.0.1.50")
print("  Time: 2024-01-15T10:30:00Z")
print("  Result: Success")
```

---

### HashiCorp Vault: Complete Banking Summary

| Scenario | What It Does | TTL/Expiration | Banking Use Case |
|----------|-------------|----------------|------------------|
| **Static Secrets** | Store API keys, passwords | Never expires (manual rotation) | Core banking API, payment gateway |
| **Dynamic DB Credentials** | Generate temporary DB users | 30 min - 2 hours | App database access |
| **Dynamic AWS Credentials** | Generate temporary AWS keys | 15 min - 4 hours | S3 access, KMS operations |
| **PKI/TLS Certificates** | Generate internal certificates | 90 days - 10 years | API servers, mTLS |
| **Secret Retrieval** | App gets secrets at runtime | Auto-refresh before expiry | Application startup |
| **Audit Logging** | Log all secret access | Permanent | PCI DSS, SOX compliance |

---

### AWS KMS vs HashiCorp Vault: Banking Implementation Summary

| Use Case | AWS KMS | HashiCorp Vault |
|----------|---------|------------------|
| **Encrypting stored data** | ✅ Primary use case | ❌ Not designed for this |
| **Database credentials** | ❌ No | ✅ Dynamic, auto-rotating |
| **API keys storage** | ❌ No | ✅ Secure storage with versioning |
| **Certificate management** | ❌ No | ✅ PKI engine built-in |
| **Cloud-native workloads** | ✅ Best choice | ⚠️ Works but not native |
| **Multi-cloud/on-premise** | ❌ AWS only | ✅ Works everywhere |

**Recommendation for Banks:**
- Use **AWS KMS** for encrypting data at rest in AWS services
- Use **HashiCorp Vault** for managing all application secrets and dynamic credentials
- Implement **both** for comprehensive security

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

**Encryption at Rest** protects data stored on disk (databases, files, backups, data lakes).
- **When:** Data is written to storage
- **Technology:** AES-256, envelope encryption
- **Protects Against:** Physical theft, unauthorized disk access, backup exposure
- **Banking Example:** Encrypting account numbers and SSNs in the database so even a database admin cannot read them without the key

**Encryption in Transit** protects data moving between systems (client to database, service to service).
- **When:** Data is transmitted over network
- **Technology:** TLS 1.2/1.3, SSL
- **Protects Against:** Network sniffing, man-in-the-middle attacks
- **Banking Example:** Using HTTPS (TLS 1.3) when a user's banking app communicates with the bank's API server

**Both are required for comprehensive security.** A banking application needs:
1. TLS 1.3 for all API communications (in transit)
2. AES-256 encryption for all stored sensitive data (at rest)
3. Never assume one covers the other - implement both layers

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
