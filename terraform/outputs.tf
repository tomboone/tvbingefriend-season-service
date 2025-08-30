output "function_app_name" {
  description = "The name of the main Function App."
  value       = azurerm_linux_function_app.main.name
}

output "function_app_resource_group_name" {
  description = "The resource group of the function app."
  value       = azurerm_linux_function_app.main.resource_group_name
}

output "prod_db_name" {
  description = "The name of the production database."
  value       = azurerm_mysql_flexible_database.prod.name
}

output "prod_db_user" {
  description = "The username for the production database managed identity."
  value       = azurerm_linux_function_app.main.name
}

output "stage_db_name" {
  description = "The name of the stage database."
  value       = azurerm_mysql_flexible_database.stage.name
}

output "stage_db_user" {
  description = "The username for the stage database managed identity."
  value       = "${azurerm_linux_function_app.main.name}-stage"
}

output "function_app_python_version" {
  description = "The Python version used by the Function App."
  value       = azurerm_linux_function_app.main.site_config[0].application_stack[0].python_version
}
