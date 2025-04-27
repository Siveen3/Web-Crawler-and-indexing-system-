# master_node.py

import boto3
import json
import time
from datetime import datetime, timezone

# CONFIGURATION
REGION_NAME = 'us-east-1'
CRAWL_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/138749495090/CrawlQueue'
REPORT_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/138749495090/ReportQueue'
DYNAMODB_TABLE_NAME = 'CrawlerHeartbeatTable'
MAX_DEPTH = 2  
BATCH_SEED_URLS = [
    "http://siveen.com",
    "http://soso.com/about",
    "http://hfffff.com/contact",
]

# Initialize clients
sqs = boto3.client('sqs', region_name=REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
heartbeat_table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def send_url_to_crawl_queue(url, depth=0):
    message = {
        "url": url,
        "depth": depth,
        "max_depth": MAX_DEPTH
    }
    response = sqs.send_message(
        QueueUrl=CRAWL_QUEUE_URL,
        MessageBody=json.dumps(message)
    )
    print(f"[Master] Sent URL to CrawlQueue: {url} (depth={depth})")

def submit_seed_urls(seed_urls):
    for url in seed_urls:
        send_url_to_crawl_queue(url, depth=0)

def monitor_crawl_queue():
    while True:
        response = sqs.get_queue_attributes(
            QueueUrl=CRAWL_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        num_messages = int(response['Attributes']['ApproximateNumberOfMessages'])
        print(f"[Monitor] Remaining URLs in queue: {num_messages}")

        monitor_crawlers_health()
        monitor_crawler_reports()

        if num_messages == 0:
            print("[Master] CrawlQueue is empty. Crawling seems complete!")
            break

        time.sleep(30)  # Check every 30 seconds

def monitor_crawlers_health():
    print("[Monitor] Checking crawler heartbeats...")
    response = heartbeat_table.scan()
    now = datetime.now(timezone.utc)

    for item in response['Items']:
        crawler_id = item['crawler_id']
        last_heartbeat = datetime.fromisoformat(item['last_heartbeat'])
        time_diff = (now - last_heartbeat).total_seconds()

        if time_diff > 120:  # 2 minutes threshold
            print(f"[Warning] {crawler_id} missed heartbeat! Last seen {int(time_diff)} seconds ago.")
        else:
            print(f"[Info] {crawler_id} is alive (last seen {int(time_diff)} seconds ago).")

def monitor_crawler_reports():
    print("[Monitor] Checking crawler reports...")
    while True:
        response = sqs.receive_message(
            QueueUrl=REPORT_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5
        )

        messages = response.get('Messages', [])

        if not messages:
            break  # No messages, exit

        for message in messages:
            body = json.loads(message['Body'])
            crawler_id = body.get('crawler_id', 'unknown')
            crawled_url = body.get('crawled_url', 'unknown')
            extracted_urls = body.get('extracted_urls', [])
            depth = body.get('depth', 0)
            status = body.get('status', 'unknown')
            error = body.get('error', '')

            if status == 'success':
                print(f"[{crawler_id}] Successfully crawled: {crawled_url} at depth {depth}")
                if depth < MAX_DEPTH:
                    for url in extracted_urls:
                        send_url_to_crawl_queue(url, depth=depth+1)
                else:
                    print(f"[{crawler_id}] Max depth reached for {crawled_url}. Not adding extracted URLs.")
            else:
                print(f"[{crawler_id}] Failed crawling: {crawled_url} Reason: {error}")

            # Delete the report message after processing
            sqs.delete_message(
                QueueUrl=REPORT_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )

if __name__ == "__main__":
    print("[Master] Submitting Seed URLs to CrawlQueue...")
    submit_seed_urls(BATCH_SEED_URLS)
    print("[Master] Monitoring CrawlQueue and Crawler Health...")
    monitor_crawl_queue()
