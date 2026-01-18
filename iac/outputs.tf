output "eventhub_connection_string" {
  value = azurerm_eventhub.eh.default_primary_connection_string
}

output "storage_account_name" {
  value = azurerm_storage_account.storage.name
}

