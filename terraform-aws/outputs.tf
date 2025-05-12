output "master_node_public_ip" {
  description = "Public IP address of the master node"
  value       = aws_instance.master.public_ip
}

output "crawler_nodes_public_ips" {
  description = "Public IP addresses of the crawler nodes"
  value       = aws_instance.crawler[*].public_ip
}

output "indexer_nodes_public_ips" {
  description = "Public IP addresses of the indexer nodes"
  value       = aws_instance.indexer[*].public_ip
}

output "opensearch_endpoint" {
  description = "OpenSearch domain endpoint"
  value       = aws_opensearch_domain.search.endpoint
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket for storing crawled data"
  value       = aws_s3_bucket.data_bucket.bucket
}

output "sqs_queue_urls" {
  description = "URLs of all SQS queues"
  value = {
    crawl_queue        = aws_sqs_queue.crawl_queue.url
    report_queue       = aws_sqs_queue.report_queue.url
    dead_letter_queue  = aws_sqs_queue.dead_letter_queue.url
    feedback_queue     = aws_sqs_queue.index_feedback_queue.url
    request_queue      = aws_sqs_queue.request_queue.url
    response_queue     = aws_sqs_queue.response_queue.url
    search_queue       = aws_sqs_queue.search_queue.url
  }
}

output "dynamodb_tables" {
  description = "Names of all DynamoDB tables"
  value = {
    urls_table           = aws_dynamodb_table.urls.name
    crawler_heartbeat    = aws_dynamodb_table.crawler_heartbeat.name
    indexer_heartbeat    = aws_dynamodb_table.indexer_heartbeat.name
    task_assignments     = aws_dynamodb_table.task_assignments.name
    blocked_urls         = aws_dynamodb_table.blocked_urls.name
    index_status         = aws_dynamodb_table.index_status.name
  }
}
