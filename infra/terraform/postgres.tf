resource "azurerm_postgresql_flexible_server" "db" {
  name                   = "rangerwatch-pg"
  resource_group_name    = azurerm_resource_group.rangerwatch.name
  location               = azurerm_resource_group.rangerwatch.location

  administrator_login    = var.db_admin
  administrator_password = var.db_password

  sku_name = "B_Standard_B1ms"
  version  = "16"
}