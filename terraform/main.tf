# DigiCert Global Root G2 certificate
data "http" "digicert_global_root_g2" {
  url = "https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem"
}

# Microsoft RSA Root Certificate Authority 2017 certificate (embedded PEM format)
locals {
  service_name = "tvbf-season-service"
  microsoft_rsa_root_2017_pem = <<-EOT
-----BEGIN CERTIFICATE-----
MIIFqDCCA5CgAwIBAgIQHtOXCX95q3YKHD0uKsEafjANBgkqhkiG9w0BAQwFADBl
MQswCQYDVQQGEwJVUzEeMBwGA1UEChMVTWljcm9zb2Z0IENvcnBvcmF0aW9uMTYw
NAYDVQQDEy1NaWNyb3NvZnQgUlNBIFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9yaXR5
IDIwMTcwHhcNMTkxMjE4MjI1MTIyWhcNNDIwNzE4MjMwMDIzWjBlMQswCQYDVQQG
EwJVUzEeMBwGA1UEChMVTWljcm9zb2Z0IENvcnBvcmF0aW9uMTYwNAYDVQQDEy1N
aWNyb3NvZnQgUlNBIFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9yaXR5IDIwMTcwggIi
MA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDKW765Q4wpqZEWCpW9R2LBiftJ
Nt9GkMml7XhqkyfERLTEaLgfJYNGCpqFvuA+UDJ2gcQgVC8BAl11nEhAglMQvL/H
O2iQtqQi5fZAWbAzbMjSXZ+7rCJqQs1WdMRZE4+ZdOPIBqo54WmD9GdO2GdxOQNH
9DCHgIBaoBNWxZDRnK7rJ2bwlwpVmlA8J4iCwgMeqLdqCFOZIXRUIVjJWB4rJJ8l
CnkJmxNdIJGRhOsj6XaGkYu4Qb0pFjzP+5oIWUxBm5VRWUXqL0WPUzE5gj3LJjjY
1u9xt4r5nU5I9D4J4DqZpyX9nPYcBOhUdEsA4QXcb5bhQ7sHV3NJvQXqLrU6mGPJ
qMqPHGYlPGFJcggAy0G5VYhJpQdSJCQ0HHOY2EEsX2yTRjfhWNj5mVYqYFJlhMX9
8bKDIKEhDlrGVGZNhMPWkJDG3rJqbfCEaAhsFAOLYaZuJNIUZ6eX8zXJ1jfUEHGH
5lh6CzlYE1OhCmF0zGBzKLKdEhF4LqAgtJOmvhYIe6cKRnWKqJRpF9NKgc7p5M9G
r4KWBcP1C1q1HmvAzXzlJhUFfWGQZPnr1q2h0V7VW+LNTaE6XPbDJ6mRCCCKJHIc
r4rCi8OsU5l+pPZhzLGP9LhaBXLnGNhIh5YaCZJCwXxPjUJJ0GNfDK6kBPSLsEEv
M7gK+G3JTmtDRVl4wIDAQABo1QwUjAOBgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/
BAUwAwEB/zAdBgNVHQ4EFgQUCcxZb4aycI8aw5D4pv+7TbIjMBAwCgYJKoBILxUB
BAMCATANBgkqhkiG9w0BAQwFAAOCAgEAjKsecR5hKI6jcP+5fxU7PjHRKqalCOe4
8H2Zb4Zm71ue3peTNrQ+W3e3lXMGm/CJ5e5JTsBDzDBvXSNjHl91EZCqPmvGCjQ2
3Rnr/Nq/wTN7WJTgC3kO+UYsGF1oZDy+LJJ5gmdFXQaD3lGbwOKZLUg1gWdMDbF0
1T7Lz7KEu3kJgZFWAmEQ2LT7Qmp0xFIYDtGZdYHbH3MdW5Vy7fGZ9rT7LfR6hZFC
Oy0gB9/fT+q/9NeEK6TlG9MgOzQhkwEZPG5w/QJQ5FQD8FBL5SU5UYYBuPb7Q9k1
kH0f1fEfJ3d4mT0m4vUnfKJcT9OJN1A+WLH5Lzn/5k5GZ5YmLiU1kWdJnYVlCJTz
g4G8BmG/k+d7QOJFbPFKOmTKJYd1mB4JmXNMfk8XlKU1b5TJC4wSPq8aKNbNPIYS
IOJ2r4FwJDOZNqYCDw0QKTtcwYIzD7g8Y5JDjNqVNfUqQPDa9g9UWgdFwg3qD1f1
2P5W4hb5g9LW8aNmDBUhY4AcJdFmQoD8B5PZ2y0DH4m6DLB+/bP10eQMnFLFO5F2
MpTNO7HAgS3XYJGGdGZeQV/4JHhJXqT1kJnJgR3eU6f3OfyeHqzHKQPNiJHjG9kn
RGGM5KkxENKe8kM6THEWx/T1JZkgxTDMXb1y8zlHe1rZzFPfJnRCJ7aTNOZJFKj1
g1M4J3JyJsQ=
-----END CERTIFICATE-----
EOT

  # Combine both PEM certificates
  mysql_ca_cert_content = "${data.http.digicert_global_root_g2.response_body}${local.microsoft_rsa_root_2017_pem}"
}

data "terraform_remote_state" "shared" {
  backend = "azurerm"
  config = {
    resource_group_name: var.tf_shared_resource_group_name
    storage_account_name: var.tf_shared_storage_account_name
    container_name: var.tf_shared_container_name
    key: var.tf_shared_key
  }
}

data "azurerm_service_plan" "existing" {
  name = data.terraform_remote_state.shared.outputs.app_service_plan_name
  resource_group_name = data.terraform_remote_state.shared.outputs.app_service_plan_resource_group
}

data "azurerm_resource_group" "existing" {
  name = data.terraform_remote_state.shared.outputs.resource_group_name
}

data "azurerm_storage_account" "existing" {
  name                     = data.terraform_remote_state.shared.outputs.storage_account_name
  resource_group_name      = data.terraform_remote_state.shared.outputs.resource_group_name
}

# Access storage resources from shared state
locals {
  storage_queues = data.terraform_remote_state.shared.outputs.storage_queues
  storage_tables = data.terraform_remote_state.shared.outputs.storage_tables
}


data "azurerm_mysql_flexible_server" "existing" {
  name                = data.terraform_remote_state.shared.outputs.mysql_server_name
  resource_group_name = data.terraform_remote_state.shared.outputs.mysql_server_resource_group_name
}

data "azurerm_log_analytics_workspace" "existing" {
  name                = data.terraform_remote_state.shared.outputs.log_analytics_workspace_name
  resource_group_name = data.terraform_remote_state.shared.outputs.log_analytics_workspace_resource_group_name
}

resource "azurerm_application_insights" "main" {
  name                = local.service_name
  resource_group_name = data.azurerm_resource_group.existing.name
  location            = data.azurerm_resource_group.existing.location
  workspace_id        = data.azurerm_log_analytics_workspace.existing.id
  application_type    = "web"
}

resource "azurerm_mysql_flexible_database" "prod" {
  name                = local.service_name
  resource_group_name = data.azurerm_mysql_flexible_server.existing.resource_group_name
  server_name         = data.azurerm_mysql_flexible_server.existing.name
  charset             = "utf8mb4"
  collation           = "utf8mb4_unicode_ci"
}

resource "azurerm_mysql_flexible_database" "stage" {
  name                = "${local.service_name}-stage"
  resource_group_name = data.azurerm_mysql_flexible_server.existing.resource_group_name
  server_name         = data.azurerm_mysql_flexible_server.existing.name
  charset             = "utf8mb4"
  collation           = "utf8mb4_unicode_ci"
}

# Generate random passwords for database users
resource "random_password" "prod_db_password" {
  length  = 32
  special = false
}

resource "random_password" "stage_db_password" {
  length  = 32
  special = false
}

# Create MySQL user for production with read access
resource "mysql_user" "prod_user" {
  user               = "${local.service_name}_user"
  host               = "%"
  plaintext_password = random_password.prod_db_password.result
}

# Create MySQL user for staging with read access
resource "mysql_user" "stage_user" {
  user               = "${local.service_name}_stage_user"
  host               = "%"
  plaintext_password = random_password.stage_db_password.result
}

# Grant read permissions to production user
resource "mysql_grant" "prod_grant" {
  user       = mysql_user.prod_user.user
  host       = mysql_user.prod_user.host
  database   = azurerm_mysql_flexible_database.prod.name
  privileges = ["ALL PRIVILEGES"]
}

# Grant read permissions to staging user
resource "mysql_grant" "stage_grant" {
  user       = mysql_user.stage_user.user
  host       = mysql_user.stage_user.host
  database   = azurerm_mysql_flexible_database.stage.name
  privileges = ["ALL PRIVILEGES"]
}

resource "azurerm_linux_function_app" "main" {
  name                       = local.service_name
  resource_group_name        = data.azurerm_resource_group.existing.name
  location                   = data.azurerm_resource_group.existing.location
  service_plan_id            = data.azurerm_service_plan.existing.id
  storage_account_name       = data.azurerm_storage_account.existing.name
  storage_account_access_key = data.azurerm_storage_account.existing.primary_access_key

  site_config {
    always_on = true
    application_insights_connection_string = azurerm_application_insights.main.connection_string
    application_insights_key               = azurerm_application_insights.main.instrumentation_key
    application_stack {
      python_version = "3.12"
    }
  }

  app_settings = {
    "WEBSITE_TIME_ZONE"            = "America/New_York"
    "TIMER_TRIGGER_SCHEDULE"       = "enabled"
    "MYSQL_SSL_CA_CONTENT"         = local.mysql_ca_cert_content
    "SQLALCHEMY_CONNECTION_STRING" = "mysql+pymysql://${mysql_user.prod_user.user}:${random_password.prod_db_password.result}@${data.azurerm_mysql_flexible_server.existing.fqdn}:3306/${azurerm_mysql_flexible_database.prod.name}?charset=${azurerm_mysql_flexible_database.prod.charset}&ssl_disabled=false&ssl_verify_cert=false&ssl_verify_identity=false"
    "SEASONS_QUEUE"                  = local.storage_queues["seasons-queue"]
    "SHOW_IDS_TABLE"               = local.storage_tables["showidstable"]
    "UPDATES_NCRON"                = "0 0 3 * * *"
  }

  sticky_settings {
    app_setting_names = [
      "TIMER_TRIGGER_SCHEDULE",
      "SQLALCHEMY_CONNECTION_STRING",
      "SEASONS_QUEUE",
      "SHOW_IDS_TABLE",
      "UPDATES_NCRON"
    ]
  }
}


# noinspection HILUnresolvedReference
resource "azurerm_linux_function_app_slot" "stage" {
  name                       = "stage"
  function_app_id            = azurerm_linux_function_app.main.id
  storage_account_name       = data.azurerm_storage_account.existing.name
  storage_account_access_key = data.azurerm_storage_account.existing.primary_access_key

  site_config {
    always_on                              = true
    application_insights_connection_string = azurerm_application_insights.main.connection_string
    application_insights_key               = azurerm_application_insights.main.instrumentation_key
    application_stack {
      python_version = "3.12"
    }
  }

  app_settings = {
    "WEBSITE_TIME_ZONE"            = "America/New_York"
    "TIMER_TRIGGER_SCHEDULE"       = "disabled"
    "MYSQL_SSL_CA_CONTENT"         = local.mysql_ca_cert_content
    "SQLALCHEMY_CONNECTION_STRING" = "mysql+pymysql://${mysql_user.stage_user.user}:${random_password.stage_db_password.result}@${data.azurerm_mysql_flexible_server.existing.fqdn}:3306/${azurerm_mysql_flexible_database.stage.name}?charset=${azurerm_mysql_flexible_database.stage.charset}&ssl_disabled=false&ssl_verify_cert=false&ssl_verify_identity=false"
    "INDEX_QUEUE"                  = local.storage_queues["seasons-queue-stage"]
    "SHOW_IDS_TABLE"               = local.storage_tables["showidstablestage"]
    "UPDATES_NCRON"                = "0 0 1 1 1 *"
  }
}

