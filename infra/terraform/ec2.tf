# Planned EC2 deployment for RangerWatch

resource "aws_instance" "rangerwatch" {
  instance_type = "t3.small"
}