# master_node.py

import boto3
import json
import time

# CONFIGURATION
REGION_NAME = 'us-east-1'  
CRAWL_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/138749495090/CrawlQueue' 
BATCH_SEED_URLS = [
    "http://dfgfdffde.com",
    "http://sgrytyr.com/about",
    "http://dfdfdddfe.com/contact",
]

# Initialize SQS client
sqs = boto3.client('sqs', region_name=REGION_NAME)

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

        if num_messages == 0:
            print("[Master] CrawlQueue is empty. Crawling seems complete!")
            break

        time.sleep(30)  # Check every 30 seconds


if __name__ == "__main__":
    print("[Master] Submitting Seed URLs to CrawlQueue...")
    submit_seed_urls(BATCH_SEED_URLS)
    print("[Master] Monitoring CrawlQueue...")
    monitor_crawl_queue()
