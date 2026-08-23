variable "db_host" {
  description = "PostgreSQL host"
  type        = string
  default     = "localhost"
}

variable "db_port" {
  description = "PostgreSQL port"
  type        = number
  default     = 5432
}

variable "db_admin_user" {
  description = "PostgreSQL admin username"
  type        = string
  default     = "postgres"
}

variable "db_admin_password" {
  description = "PostgreSQL admin password"
  type        = string
  sensitive   = true
}

variable "etl_user_password" {
  description = "Password for ETL user"
  type        = string
  sensitive   = true
}

variable "analyst_password" {
  description = "Password for analyst role"
  type        = string
  sensitive   = true
}

variable "admin_password" {
  description = "Password for admin role"
  type        = string
  sensitive   = true
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Environment must be dev, staging, or production."
  }
}

variable "enable_audit_logging" {
  description = "Enable audit logging"
  type        = bool
  default     = true
}

variable "enable_row_level_security" {
  description = "Enable row-level security"
  type        = bool
  default     = false
}
