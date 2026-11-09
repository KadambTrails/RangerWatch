output "resource_group_name" {
  value = azurerm_resource_group.rangerwatch.name
}

output "postgres_server_name" {
  value = azurerm_postgresql_flexible_server.db.name
}

output "keyvault_name" {
  value = azurerm_key_vault.kv.name
}