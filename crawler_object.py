import os
import sys
import logging
from crawler import Crawler

def setup_logging():
    logging.basicConfig(
        filename='crawler.log',
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Also log to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    console_handler.setFormatter(formatter)
    logging.getLogger('').addHandler(console_handler)

def get_crawler_id():
    # Try to get crawler ID from environment variable
    crawler_id = os.getenv('CRAWLER_ID')
    if not crawler_id:
        # Generate a unique ID if not provided
        import uuid
        crawler_id = f"crawler-{str(uuid.uuid4())[:8]}"
    return crawler_id

def main():
    setup_logging()
    crawler_id = get_crawler_id()
    
    try:
        crawler = Crawler(
            crawler_id=crawler_id,
            crawler_queue_url=os.getenv('CRAWLER_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/353176954707/CrawlQueue'),
            master_queue_url=os.getenv('MASTER_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/353176954707/ReportQueue'),
            indexer_queue_url=os.getenv('INDEXER_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/353176954707/IndexQueue'),
            s3_bucket=os.getenv('S3_BUCKET', 'crawler-indexer-buckets'),
            dynamodb_table=os.getenv('DYNAMODB_TABLE', 'CrawlerHeartbeatTable'),
            region=os.getenv('AWS_REGION', 'us-east-1'),
            delay=int(os.getenv('CRAWLER_DELAY', '10'))
        )
        
        logging.info(f"Starting crawler {crawler_id}")
        crawler.start_crawling()
    except Exception as e:
        logging.error(f"Fatal error in crawler {crawler_id}: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
