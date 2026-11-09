data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "kv" {

  name                = "rangerwatch-kv"

  location            = azurerm_resource_group.rangerwatch.location

  resource_group_name = azurerm_resource_group.rangerwatch.name

  tenant_id = data.azurerm_client_config.current.tenant_id

  sku_name = "standard"
}