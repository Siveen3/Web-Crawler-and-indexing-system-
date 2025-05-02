from crawler import Crawler

crawler = Crawler(
    crawler_id="crawler-1",
    crawler_queue_url="https://sqs.us-east-1.amazonaws.com/138749495090/CrawlQueue",
    master_queue_url="https://sqs.us-east-1.amazonaws.com/138749495090/ReportQueue",
    indexer_queue_url="https://sqs.us-east-1.amazonaws.com/138749495090/IndexQueue",
    s3_bucket="crawler-indexer-bucket",
    dynamodb_table="CrawlerHeartbeatTable",
)

crawler.start_crawling()
