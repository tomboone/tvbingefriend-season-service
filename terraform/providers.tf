terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">=4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">=3.0"
    }
    mysql = {
      source  = "petoju/mysql"
      version = "~> 3.0"
    }
    http = {
      source = "hashicorp/http"
      version = ">=3.0"
    }
  }
}

provider "azurerm" {
  features {}
  use_oidc = true
}

provider "mysql" {
  endpoint = data.azurerm_mysql_flexible_server.existing.fqdn
  username = var.mysql_admin_username
  password = var.mysql_admin_password
  tls      = "true"
}
