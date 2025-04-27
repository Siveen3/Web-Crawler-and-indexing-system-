# master_node.py

import boto3
import json
import time
from datetime import datetime, timezone

# CONFIGURATION
REGION_NAME = 'us-east-1'  
CRAWL_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/138749495090/CrawlQueue'  
DYNAMODB_TABLE_NAME = 'CrawlerHeartbeatTable'  
BATCH_SEED_URLS = [
    "http://siveen.com",
    "http://soso.com/about",
    "http://hfffff.com/contact",
]

# Initialize clients
sqs = boto3.client('sqs', region_name=REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
heartbeat_table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def send_url_to_crawl_queue(url):
    message = {
        "url": url
    }
    response = sqs.send_message(
        QueueUrl=CRAWL_QUEUE_URL,
        MessageBody=json.dumps(message)
    )
    print(f"[Master] Sent URL to CrawlQueue: {url}")


def submit_seed_urls(seed_urls):
    for url in seed_urls:
        send_url_to_crawl_queue(url)


def monitor_crawl_queue():
    while True:
        response = sqs.get_queue_attributes(
            QueueUrl=CRAWL_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        num_messages = int(response['Attributes']['ApproximateNumberOfMessages'])
        print(f"[Monitor] Remaining URLs in queue: {num_messages}")

        monitor_crawlers_health()

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


if __name__ == "__main__":
    print("[Master] Submitting Seed URLs to CrawlQueue...")
    submit_seed_urls(BATCH_SEED_URLS)
    print("[Master] Monitoring CrawlQueue and Crawler Health...")
    monitor_crawl_queue()
