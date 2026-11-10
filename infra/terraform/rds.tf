resource "aws_db_instance" "postgres" {

  identifier = "rangerwatch-postgres"

  engine         = "postgres"
  engine_version = "17"

  instance_class = "db.t3.micro"

  allocated_storage = 20

  db_name = "rangerwatch"

  username = var.db_admin
  password = var.db_password

  skip_final_snapshot = true

  publicly_accessible = true
}