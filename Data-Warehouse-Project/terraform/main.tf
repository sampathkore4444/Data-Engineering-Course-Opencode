terraform {
  required_version = ">= 1.0"
  
  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.0"
    }
  }
}

# Configure PostgreSQL provider
provider "postgresql" {
  host     = var.db_host
  port     = var.db_port
  username = var.db_admin_user
  password = var.db_admin_password
  sslmode  = "disable"
}

# Create database
resource "postgresql_database" "banking_dw" {
  name              = "banking_dw"
  owner             = postgresql_role.etl_user.name
  template          = "template0"
  encoding          = "UTF8"
  lc_collate        = "en_US.UTF-8"
  lc_ctype          = "en_US.UTF-8"
  connection_limit  = -1
  allow_connections = true
}

# Create schemas
resource "postgresql_schema" "staging" {
  name  = "staging"
  owner = postgresql_role.etl_user.name
  
  depends_on = [postgresql_database.banking_dw]
}

resource "postgresql_schema" "gold" {
  name  = "gold"
  owner = postgresql_role.etl_user.name
  
  depends_on = [postgresql_database.banking_dw]
}

resource "postgresql_schema" "raw" {
  name  = "raw"
  owner = postgresql_role.etl_user.name
  
  depends_on = [postgresql_database.banking_dw]
}

# Create roles
resource "postgresql_role" "etl_user" {
  name     = "etl_user"
  login    = true
  password = var.etl_user_password
  
  lifecycle {
    prevent_destroy = true
  }
}

resource "postgresql_role" "analyst_role" {
  name     = "analyst_role"
  login    = false
  password = var.analyst_password
}

resource "postgresql_role" "admin_role" {
  name     = "admin_role"
  login    = true
  password = var.admin_password
  superuser = true
}

# Grant permissions
resource "postgresql_grant" "etl_staging" {
  database    = postgresql_database.banking_dw.name
  role        = postgresql_role.etl_user.name
  schema      = postgresql_schema.staging.name
  object_type = "schema"
  privileges  = ["CREATE", "USAGE"]
}

resource "postgresql_grant" "etl_gold" {
  database    = postgresql_database.banking_dw.name
  role        = postgresql_role.etl_user.name
  schema      = postgresql_schema.gold.name
  object_type = "schema"
  privileges  = ["CREATE", "USAGE"]
}

resource "postgresql_grant" "analyst_gold" {
  database    = postgresql_database.banking_dw.name
  role        = postgresql_role.analyst_role.name
  schema      = postgresql_schema.gold.name
  object_type = "schema"
  privileges  = ["USAGE"]
}

# Create tables in staging schema
resource "postgresql_table" "stg_customers" {
  name = "stg_customers"
  schema = postgresql_schema.staging.name
  
  column {
    name = "customer_id"
    type = "VARCHAR(50)"
    nullable = false
  }
  
  column {
    name = "customer_name"
    type = "VARCHAR(200)"
    nullable = false
  }
  
  column {
    name = "customer_type"
    type = "VARCHAR(50)"
    nullable = false
  }
  
  column {
    name = "phone"
    type = "VARCHAR(20)"
    nullable = false
  }
  
  column {
    name = "email"
    type = "VARCHAR(200)"
    nullable = false
  }
  
  column {
    name = "created_at"
    type = "TIMESTAMP"
    default = "NOW()"
  }
  
  column {
    name = "updated_at"
    type = "TIMESTAMP"
    default = "NOW()"
  }
}

resource "postgresql_table" "stg_accounts" {
  name = "stg_accounts"
  schema = postgresql_schema.staging.name
  
  column {
    name = "account_id"
    type = "VARCHAR(50)"
    nullable = false
  }
  
  column {
    name = "customer_id"
    type = "VARCHAR(50)"
    nullable = false
  }
  
  column {
    name = "account_type"
    type = "VARCHAR(50)"
    nullable = false
  }
  
  column {
    name = "balance"
    type = "DECIMAL(15,2)"
    nullable = false
  }
  
  column {
    name = "status"
    type = "VARCHAR(20)"
    nullable = false
  }
  
  column {
    name = "branch_id"
    type = "VARCHAR(50)"
  }
  
  column {
    name = "created_at"
    type = "TIMESTAMP"
    default = "NOW()"
  }
  
  column {
    name = "updated_at"
    type = "TIMESTAMP"
    default = "NOW()"
  }
}

# Create indexes
resource "postgresql_index" "idx_stg_customers_id" {
  name  = "idx_stg_customers_id"
  table = postgresql_table.stg_customers.name
  schema = postgresql_schema.staging.name
  
  columns = ["customer_id"]
  unique  = true
}

resource "postgresql_index" "idx_stg_accounts_id" {
  name  = "idx_stg_accounts_id"
  table = postgresql_table.stg_accounts.name
  schema = postgresql_schema.staging.name
  
  columns = ["account_id"]
  unique  = true
}

resource "postgresql_index" "idx_stg_accounts_customer" {
  name  = "idx_stg_accounts_customer"
  table = postgresql_table.stg_accounts.name
  schema = postgresql_schema.staging.name
  
  columns = ["customer_id"]
}

# Create audit log table
resource "postgresql_table" "audit_log" {
  name = "audit_log"
  schema = postgresql_schema.staging.name
  
  column {
    name = "log_id"
    type = "BIGSERIAL"
    nullable = false
  }
  
  column {
    name = "table_name"
    type = "VARCHAR(100)"
    nullable = false
  }
  
  column {
    name = "operation"
    type = "VARCHAR(10)"
    nullable = false
  }
  
  column {
    name = "record_id"
    type = "VARCHAR(100)"
  }
  
  column {
    name = "old_values"
    type = "JSONB"
  }
  
  column {
    name = "new_values"
    type = "JSONB"
  }
  
  column {
    name = "changed_by"
    type = "VARCHAR(100)"
  }
  
  column {
    name = "changed_at"
    type = "TIMESTAMP"
    default = "NOW()"
  }
}
