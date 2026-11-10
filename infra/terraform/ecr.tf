resource "aws_ecr_repository" "airflow" {
  name = "rangerwatch-airflow"
}

resource "aws_ecr_repository" "spark" {
  name = "rangerwatch-spark"
}