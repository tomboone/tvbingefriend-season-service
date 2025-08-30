variable "tf_shared_resource_group_name" {
  type = string
}

variable "tf_shared_storage_account_name" {
  type = string
}

variable "tf_shared_container_name" {
  type = string
}

variable "tf_shared_key" {
  type = string
}

variable "mysql_admin_username" {
  type        = string
  description = "MySQL flexible server admin username"
}

variable "mysql_admin_password" {
  type        = string
  description = "MySQL flexible server admin password"
  sensitive   = true
}