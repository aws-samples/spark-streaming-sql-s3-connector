# EMR Serverless without a VPC cannot reach SQS, and a remote queue may sit in another region.
# So jobs run in private subnets with a NAT gateway for egress (HTTPS only) and an S3 gateway
# endpoint so S3 traffic stays off the NAT. Nothing here accepts inbound traffic.

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

resource "aws_vpc" "e2e" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = local.name }
}

# Remove all rules from the default security group so nothing can use it by accident.
resource "aws_default_security_group" "e2e" {
  vpc_id = aws_vpc.e2e.id
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.e2e.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 2, 0)
  availability_zone       = local.azs[0]
  map_public_ip_on_launch = false

  tags = { Name = "${local.name}-public" }
}

resource "aws_subnet" "private" {
  count = length(local.azs)

  vpc_id            = aws_vpc.e2e.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 2, count.index + 1)
  availability_zone = local.azs[count.index]

  tags = { Name = "${local.name}-private-${count.index}" }
}

resource "aws_internet_gateway" "e2e" {
  vpc_id = aws_vpc.e2e.id
}

resource "aws_eip" "nat" {
  domain = "vpc"

  depends_on = [aws_internet_gateway.e2e]
}

# shortcut: one NAT in one AZ; if that AZ fails the run fails. Fine for a test run.
resource "aws_nat_gateway" "e2e" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.e2e.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.e2e.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.e2e.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.e2e.id
  }
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# Default (full) endpoint policy on purpose: EMR Serverless also reads and writes its own
# service-managed log storage through this path. Access to our data is limited by the job role.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.e2e.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
}

resource "aws_security_group" "emr" {
  name        = "${local.name}-emr"
  description = "EMR Serverless workers: no inbound, HTTPS outbound only"
  vpc_id      = aws_vpc.e2e.id
}

# SQS, S3, STS and the remote region's endpoints are all HTTPS.
resource "aws_vpc_security_group_egress_rule" "emr_https" {
  security_group_id = aws_security_group.emr.id
  description       = "HTTPS to AWS service endpoints"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}
