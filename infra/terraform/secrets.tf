resource "aws_secretsmanager_secret" "db_secret" {
  name = "rangerwatch-db-credentials"
}