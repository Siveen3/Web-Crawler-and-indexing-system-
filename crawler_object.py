from crawler_node import Crawler

crawler = Crawler(
    crawler_id="crawler-1",
    crawler_queue="https://sqs.us-east-1.amazonaws.com/111111111111/CrawlerQueue",
    master_queue="https://sqs.us-east-1.amazonaws.com/111111111111/ReportQueue",
    #indexer_queue="https://sqs.us-east-1.amazonaws.com/111111111111/IndexerQueue",
    #s3_bucket="your-s3-bucket-name",
    dynamodb_table="CrawlerHeartbeatTable",
)

crawler.start_crawling()
