from crawler_node import Crawler

crawler = Crawler(
    crawler_id="crawler-1",
    crawler_queue="https://sqs.us-east-1.amazonaws.com/138749495090/CrawlQueue",
    master_queue="https://sqs.us-east-1.amazonaws.com/138749495090/ReportQueue",
    s3_bucket="your-s3-bucket-name",
    dynamodb_table="CrawlerHeartbeatTable",
)

crawler.start_crawling()
