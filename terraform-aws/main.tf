provider "aws" {
  region = "us-east-1"
}

# ---------------- VPC ----------------
resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route_table" "rt" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }
}

resource "aws_route_table_association" "rta" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.rt.id
}

# ---------------- Security Groups ----------------
resource "aws_security_group" "crawler_sg" {
  name        = "crawler-security-group"
  description = "Security group for crawler nodes"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "indexer_sg" {
  name        = "indexer-security-group"
  description = "Security group for indexer nodes"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "master_sg" {
  name        = "master-security-group"
  description = "Security group for master node"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------------- SQS Queues ----------------
resource "aws_sqs_queue" "crawl_queue" {
  name = "CrawlQueue"
}

resource "aws_sqs_queue" "report_queue" {
  name = "ReportQueue"
}

resource "aws_sqs_queue" "dead_letter_queue" {
  name = "DeadLetterQueue"
}

resource "aws_sqs_queue" "index_feedback_queue" {
  name = "FeedbackQueue"
}

resource "aws_sqs_queue" "request_queue" {
  name = "RequestQueue"
}

resource "aws_sqs_queue" "response_queue" {
  name = "ResponseQueue"
}

resource "aws_sqs_queue" "search_queue" {
  name = "SearchQueue"
}

# ---------------- DynamoDB Tables ----------------
resource "aws_dynamodb_table" "crawler_heartbeat" {
  name           = "CrawlerHeartbeatTable"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "crawler_id"
  attribute {
    name = "crawler_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "indexer_heartbeat" {
  name           = "IndexerHeartbeatTable"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "indexer_id"
  attribute {
    name = "indexer_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "task_assignments" {
  name           = "CrawlerTaskAssignmets"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "url"
  attribute {
    name = "url"
    type = "S"
  }
}

resource "aws_dynamodb_table" "blocked_urls" {
  name           = "BlockedUrlsTable"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "url"
  attribute {
    name = "url"
    type = "S"
  }
}

resource "aws_dynamodb_table" "index_status" {
  name           = "IndexerTaskAssignments"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "url"
  attribute {
    name = "url"
    type = "S"
  }
}

# ---------------- EC2 Instances ----------------
resource "aws_instance" "master" {
  ami                    = var.ami_id
  instance_type          = "t2.micro"
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.master_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name
  key_name               = var.key_name

  user_data = <<-EOF
              #!/bin/bash
              apt-get update
              apt-get install -y python3-pip
              pip3 install boto3
              cd /home/ubuntu
              wget https://raw.githubusercontent.com/your-repo/master.py
              python3 master.py
              EOF

  tags = {
    Name = "MasterNode"
  }
}

resource "aws_instance" "crawler" {
  count                  = 3  # Start with 3 crawler nodes
  ami                    = var.ami_id
  instance_type          = "t2.micro"
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.crawler_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name
  key_name               = var.key_name

  user_data = <<-EOF
              #!/bin/bash
              apt-get update
              apt-get install -y python3-pip
              pip3 install boto3 requests beautifulsoup4
              cd /home/ubuntu
              wget https://raw.githubusercontent.com/your-repo/crawler.py
              python3 crawler.py
              EOF

  tags = {
    Name = "CrawlerNode-${count.index + 1}"
  }
}

resource "aws_instance" "indexer" {
  count                  = 2  # Start with 2 indexer nodes
  ami                    = var.ami_id
  instance_type          = "t2.micro"
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.indexer_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name
  key_name               = var.key_name

  user_data = <<-EOF
              #!/bin/bash
              apt-get update
              apt-get install -y python3-pip
              pip3 install boto3 opensearch-py
              cd /home/ubuntu
              wget https://raw.githubusercontent.com/your-repo/indexer.py
              python3 indexer.py
              EOF

  tags = {
    Name = "IndexerNode-${count.index + 1}"
  }
}

# ---------------- S3 ----------------
resource "aws_s3_bucket" "data_bucket" {
  bucket = "malak-crawler-bucket-${random_id.bucket_id.hex}"
  acl    = "private"
}

resource "random_id" "bucket_id" {
  byte_length = 4
}

# ---------------- DynamoDB ----------------
resource "aws_dynamodb_table" "urls" {
  name           = "CrawledURLs"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "URL"
  attribute {
    name = "URL"
    type = "S"
  }
}

# ---------------- IAM Role for EC2 ----------------
resource "aws_iam_role" "ec2_role" {
  name = "ec2_crawler_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "attach_policy" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "dynamo_policy" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

resource "aws_iam_role_policy_attachment" "opensearch_policy" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonOpenSearchServiceFullAccess"
}

resource "aws_iam_role_policy_attachment" "sqs_policy" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSQSFullAccess"
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "ec2_crawler_profile"
  role = aws_iam_role.ec2_role.name
}

# ---------------- OpenSearch ----------------
resource "aws_opensearch_domain" "search" {
  domain_name           = "crawler-search"
  engine_version        = "OpenSearch_1.3"
  cluster_config {
    instance_type = "t3.small.search"
    instance_count = 1
  }
  ebs_options {
    ebs_enabled = true
    volume_size = 10
  }
  access_policies = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = "*",
      Action = "es:*",
      Resource = "*"
    }]
  })
}
