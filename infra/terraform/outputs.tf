output "postgres_endpoint" {
  value = aws_db_instance.postgres.address
}

output "ecr_airflow_repo" {
  value = aws_ecr_repository.airflow.repository_url
}

output "ecr_spark_repo" {
  value = aws_ecr_repository.spark.repository_url
}