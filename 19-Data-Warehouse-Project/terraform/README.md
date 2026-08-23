# 🏗️ Infrastructure as Code - Terraform

> **Automated provisioning of PostgreSQL and Data Warehouse infrastructure**

---

## 📋 Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Configuration](#3-configuration)
4. [Usage](#4-usage)
5. [Resources Created](#5-resources-created)
6. [Best Practices](#6-best-practices)

---

## 1. Overview

This folder contains **Terraform configuration** to provision and manage the Data Warehouse infrastructure.

### What is Terraform?

| Term | Meaning |
|------|---------|
| **Infrastructure as Code** | Define infrastructure in code files |
| **Declarative** | Specify what you want, not how to create it |
| **State Management** | Track current infrastructure state |
| **Plan & Apply** | Preview changes before applying |

### Why Use Terraform?

| Benefit | Description |
|---------|-------------|
| **Reproducibility** | Create identical environments |
| **Version Control** | Track infrastructure changes |
| **Collaboration** | Multiple teams can work together |
| **Documentation** | Code is the documentation |
| **Automation** | Integrate with CI/CD pipelines |

---

## 2. Prerequisites

### Required Software

| Software | Version | Purpose |
|----------|---------|---------|
| **Terraform** | >= 1.0 | Infrastructure provisioning |
| **PostgreSQL** | >= 15 | Database server |
| **psql** | Latest | Database management |

### Installation

```bash
# Install Terraform (macOS)
brew install terraform

# Install Terraform (Linux)
sudo apt-get install terraform

# Install Terraform (Windows)
choco install terraform

# Verify installation
terraform --version
```

---

## 3. Configuration

### Variables File

Create a `terraform.tfvars` file (not committed to git):

```hcl
# Database connection
db_host = "localhost"
db_port = 5432
db_admin_user = "postgres"
db_admin_password = "your-secure-password"

# User passwords
etl_user_password = "etl-secure-password"
analyst_password = "analyst-secure-password"
admin_password = "admin-secure-password"

# Environment
environment = "dev"

# Features
enable_audit_logging = true
enable_row_level_security = false
```

### Environment Variables

```bash
# Alternative: Use environment variables
export TF_VAR_db_admin_password="your-secure-password"
export TF_VAR_etl_user_password="etl-secure-password"
```

---

## 4. Usage

### Initialize Terraform

```bash
cd terraform
terraform init
```

### Plan Changes

```bash
# Preview what will be created
terraform plan

# Save plan to file
terraform plan -out=tfplan
```

### Apply Changes

```bash
# Apply changes
terraform apply

# Apply from saved plan
terraform apply tfplan

# Auto-approve (use with caution)
terraform apply -auto-approve
```

### Destroy Infrastructure

```bash
# Destroy all resources
terraform destroy

# Destroy specific resource
terraform destroy -target=postgresql_database.banking_dw
```

### View Current State

```bash
# Show current state
terraform show

# List all resources
terraform state list

# Show specific resource
terraform state show postgresql_database.banking_dw
```

---

## 5. Resources Created

### Database Resources

| Resource | Name | Description |
|----------|------|-------------|
| `postgresql_database` | banking_dw | Main data warehouse database |
| `postgresql_schema` | staging | Staging schema for raw data |
| `postgresql_schema` | gold | Gold schema for business data |
| `postgresql_schema` | raw | Raw schema for source data |

### Role Resources

| Resource | Name | Description |
|----------|------|-------------|
| `postgresql_role` | etl_user | ETL pipeline user |
| `postgresql_role` | analyst_role | Read-only analyst access |
| `postgresql_role` | admin_role | Full admin access |

### Table Resources

| Resource | Name | Description |
|----------|------|-------------|
| `postgresql_table` | stg_customers | Customer staging table |
| `postgresql_table` | stg_accounts | Account staging table |
| `postgresql_table` | audit_log | Audit logging table |

### Index Resources

| Resource | Name | Description |
|----------|------|-------------|
| `postgresql_index` | idx_stg_customers_id | Unique index on customer_id |
| `postgresql_index` | idx_stg_accounts_id | Unique index on account_id |
| `postgresql_index` | idx_stg_accounts_customer | Index on customer_id |

### Permission Resources

| Resource | Description |
|----------|-------------|
| `postgresql_grant` | ETL user permissions on staging |
| `postgresql_grant` | ETL user permissions on gold |
| `postgresql_grant` | Analyst read permissions on gold |

---

## 6. Best Practices

### Security

| Practice | Implementation |
|----------|----------------|
| **Secrets Management** | Use environment variables or Vault |
| **Least Privilege** | Grant minimal required permissions |
| **Audit Logging** | Enable for compliance |
| **Network Security** | Restrict database access |

### State Management

| Practice | Implementation |
|----------|----------------|
| **Remote State** | Store state in S3/GCS |
| **State Locking** | Use DynamoDB for locking |
| **Versioning** | Enable state file versioning |
| **Backup** | Regular state backups |

### Code Organization

| Practice | Implementation |
|----------|----------------|
| **Modularization** | Split into modules |
| **Environment Separation** | Use workspaces |
| **Variable Validation** | Define validation rules |
| **Documentation** | Add comments and README |

---

## 📊 Summary

| Component | Resources | Purpose |
|-----------|-----------|---------|
| **Database** | 1 | Main data warehouse |
| **Schemas** | 3 | staging, gold, raw |
| **Roles** | 3 | etl_user, analyst, admin |
| **Tables** | 3 | stg_customers, stg_accounts, audit_log |
| **Indexes** | 3 | Performance optimization |
| **Grants** | 3 | Permission management |

### Quick Commands

```bash
# Initialize
terraform init

# Preview
terraform plan

# Apply
terraform apply

# Destroy
terraform destroy
```

---

*Built with ❤️ for Data Engineers learning Infrastructure as Code*
