output "database_host" {
  description = "PostgreSQL host"
  value       = var.db_host
}

output "database_port" {
  description = "PostgreSQL port"
  value       = var.db_port
}

output "database_name" {
  description = "Database name"
  value       = postgresql_database.banking_dw.name
}

output "database_owner" {
  description = "Database owner"
  value       = postgresql_role.etl_user.name
}

output "staging_schema" {
  description = "Staging schema name"
  value       = postgresql_schema.staging.name
}

output "gold_schema" {
  description = "Gold schema name"
  value       = postgresql_schema.gold.name
}

output "raw_schema" {
  description = "Raw schema name"
  value       = postgresql_schema.raw.name
}

output "etl_user" {
  description = "ETL user name"
  value       = postgresql_role.etl_user.name
}

output "analyst_role" {
  description = "Analyst role name"
  value       = postgresql_role.analyst_role.name
}

output "admin_role" {
  description = "Admin role name"
  value       = postgresql_role.admin_role.name
}

output "connection_string" {
  description = "Database connection string"
  value       = "postgresql://${var.db_admin_user}:${var.db_admin_password}@${var.db_host}:${var.db_port}/${postgresql_database.banking_dw.name}"
  sensitive   = true
}
