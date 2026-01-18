terraform {
  required_version = ">= 1.5"
  backend "azurerm" {}
}

provider "azurerm" {
  features {}
}

