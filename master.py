# master_node.py

import boto3
import json
import time
from datetime import datetime, timezone

class MasterNode:
    def __init__(self, region_name, crawl_queue_url, report_queue_url, dynamodb_table_name, max_depth=2):
        self.region_name = region_name
        self.crawl_queue_url = crawl_queue_url
        self.report_queue_url = report_queue_url
        self.dynamodb_table_name = dynamodb_table_name
        self.max_depth = max_depth

        # Initialize clients
        self.sqs = boto3.client('sqs', region_name=self.region_name)
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name)
        self.heartbeat_table = self.dynamodb.Table(self.dynamodb_table_name)

    def send_url_to_crawl_queue(self, url, depth=0):
        message = {
            "url": url,
            "depth": depth,
            "max_depth": self.max_depth
        }
        self.sqs.send_message(
            QueueUrl=self.crawl_queue_url,
            MessageBody=json.dumps(message)
        )
        print(f"[Master] Sent URL to CrawlQueue: {url} (depth={depth})")

    def send_shutdown_signal_to_crawlers(self):
        print("[Master] Sending shutdown signals to crawlers...")
        response = self.heartbeat_table.scan()
        for item in response['Items']:
            crawler_id = item['crawler_id']
            shutdown_message = {
                "shutdown": True,
                "crawler_id": crawler_id
            }
            self.sqs.send_message(
                QueueUrl=self.crawl_queue_url,
                MessageBody=json.dumps(shutdown_message)
            )
            print(f"[Master] Sent shutdown signal to {crawler_id}")

    def submit_seed_urls(self, seed_urls):
        for url in seed_urls:
            self.send_url_to_crawl_queue(url, depth=0)

    def monitor_crawl_queue(self):
        while True:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.crawl_queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            num_messages = int(response['Attributes']['ApproximateNumberOfMessages'])
            print(f"[Monitor] Remaining URLs in queue: {num_messages}")

            self.monitor_crawlers_health()
            self.monitor_crawler_reports()

            if num_messages == 0:
                print("[Master] CrawlQueue is empty. Crawling seems complete!")
                self.send_shutdown_signal_to_crawlers() # SEND SHUTDOWN when done
                break

            time.sleep(30)  # Check every 30 seconds

    def monitor_crawlers_health(self):
        print("[Monitor] Checking crawler heartbeats...")
        response = self.heartbeat_table.scan()
        now = datetime.now(timezone.utc)

        for item in response['Items']:
            crawler_id = item['crawler_id']
            last_heartbeat = datetime.fromisoformat(item['last_heartbeat'])
            time_diff = (now - last_heartbeat).total_seconds()

            if time_diff > 120:  # 2 minutes threshold
                print(f"[Warning] {crawler_id} missed heartbeat! Last seen {int(time_diff)} seconds ago.")
            else:
                print(f"[Info] {crawler_id} is alive (last seen {int(time_diff)} seconds ago).")

    def monitor_crawler_reports(self):
        print("[Monitor] Checking crawler reports...")
        while True:
            response = self.sqs.receive_message(
                QueueUrl=self.report_queue_url,
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
                depth = body.get('depth') or 0
                status = body.get('status', 'unknown')
                error = body.get('error', '')

                if status == 'success':
                    print(f"[{crawler_id}] Successfully crawled: {crawled_url} at depth {depth}")
                    if depth < self.max_depth:
                        for url in extracted_urls:
                            self.send_url_to_crawl_queue(url, depth=depth+1)
                    else:
                        print(f"[{crawler_id}] Max depth reached for {crawled_url}. Not adding extracted URLs.")
                else:
                    print(f"[{crawler_id}] Failed crawling: {crawled_url} Reason: {error}")

                # Delete the report message after processing
                self.sqs.delete_message(
                    QueueUrl=self.report_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )


if __name__ == "__main__":
    master = MasterNode(
        region_name='us-east-1',
        crawl_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/CrawlQueue',
        report_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/ReportQueue',
        dynamodb_table_name='CrawlerHeartbeatTable',
        max_depth=2
    )

    BATCH_SEED_URLS = [
        "http://siveen.com",
        "http://soso.com/about",
        "http://hfffff.com/contact",
    ]

    print("[Master] Submitting Seed URLs to CrawlQueue...")
    master.submit_seed_urls(BATCH_SEED_URLS)
    print("[Master] Monitoring CrawlQueue and Crawler Health...")
    master.monitor_crawl_queue()
