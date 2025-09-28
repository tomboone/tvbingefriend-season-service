locals {
  service_name = "tvbf-season-service"
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
    cors {
      allowed_origins = var.allowed_origins
    }
  }

  app_settings = {
    "WEBSITE_TIME_ZONE"            = "America/New_York"
    "TIMER_TRIGGER_SCHEDULE"       = "enabled"
    "SQLALCHEMY_CONNECTION_STRING" = "mysql+pymysql://${mysql_user.prod_user.user}:${random_password.prod_db_password.result}@${data.azurerm_mysql_flexible_server.existing.fqdn}:3306/${azurerm_mysql_flexible_database.prod.name}?charset=${azurerm_mysql_flexible_database.prod.charset}"
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
      allowed_origins = var.allowed_origins
  }

  app_settings = {
    "WEBSITE_TIME_ZONE"            = "America/New_York"
    "TIMER_TRIGGER_SCHEDULE"       = "disabled"
    "SQLALCHEMY_CONNECTION_STRING" = "mysql+pymysql://${mysql_user.stage_user.user}:${random_password.stage_db_password.result}@${data.azurerm_mysql_flexible_server.existing.fqdn}:3306/${azurerm_mysql_flexible_database.stage.name}?charset=${azurerm_mysql_flexible_database.stage.charset}"
    "SEASONS_QUEUE"                  = local.storage_queues["seasons-queue-stage"]
    "SHOW_IDS_TABLE"               = local.storage_tables["showidstablestage"]
    "UPDATES_NCRON"                = "0 0 1 1 1 *"
  }
}

