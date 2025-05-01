import boto3
import json
import time
import logging
from datetime import datetime, timezone

class MasterNode:
    def __init__(self, region_name, crawl_queue_url, report_queue_url, heartbeat_table_name,
                  task_table_name, dead_letter_queue_url, blocked_table_name,
        max_depth=2):
        self.region_name = region_name
        self.crawl_queue_url = crawl_queue_url
        self.report_queue_url = report_queue_url
        self.heartbeat_table_name = heartbeat_table_name
        self.task_table_name = task_table_name
        self.dead_letter_queue_url = dead_letter_queue_url
        self.max_depth = max_depth

        self.sqs = boto3.client('sqs', region_name=self.region_name)
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name)
        self.heartbeat_table = self.dynamodb.Table(self.heartbeat_table_name)
        self.task_table = self.dynamodb.Table(self.task_table_name)
        self.blocked_table = self.dynamodb.Table(blocked_table_name)

        logging.basicConfig(filename='master_log.log', level=logging.INFO,
                            format='%(asctime)s [%(levelname)s] %(message)s')

        self.TIMEOUT_SECONDS = 120

    def send_url_to_crawl_queue(self, url, depth=0):
        if self.is_blocked_url(url):
            logging.warning(f"[Master] Skipping blocked URL: {url}")
            return
        message = {
            "url": url,
            "depth": depth,
            "max_depth": self.max_depth
        }
        self.sqs.send_message(
            QueueUrl=self.crawl_queue_url,
            MessageBody=json.dumps(message)
        )
        logging.info(f"[Master] Sent URL to CrawlQueue: {url} (depth={depth})")

        self.task_table.put_item(
            Item={
                'url': url,
                'depth': depth,
                'status': 'pending',
                'retries': 0,
                'assigned_at': datetime.now(timezone.utc).isoformat()
            }
        )

    def send_shutdown_signal_to_crawlers(self):
        logging.info("[Master] Sending shutdown signals to crawlers...")
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
            logging.info(f"[Master] Sent shutdown signal to {crawler_id}")

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
            logging.info(f"[Monitor] Remaining URLs in queue: {num_messages}")

            self.monitor_crawlers_health()
            self.monitor_crawler_reports()
            self.monitor_task_timeouts()

            if num_messages == 0:
                logging.info("[Master] CrawlQueue is empty. Crawling seems complete!")
                self.send_shutdown_signal_to_crawlers()
                break

            time.sleep(30)

    def monitor_crawlers_health(self):
        logging.info("[Monitor] Checking crawler heartbeats...")
        response = self.heartbeat_table.scan()
        now = datetime.now(timezone.utc)

        for item in response['Items']:
            crawler_id = item['crawler_id']
            last_heartbeat = datetime.fromisoformat(item['last_heartbeat'])
            time_diff = (now - last_heartbeat).total_seconds()

            if time_diff > 120:
                logging.warning(f"[Warning] {crawler_id} missed heartbeat! Last seen {int(time_diff)} seconds ago.")
            elif status == 'skipped':
                    logging.info(f"[{crawler_id}] Skipped URL (blocked by robots.txt): {crawled_url}")
                    self.blocked_table.put_item(Item={
                        'url': crawled_url,
                        'reason': error,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                    self.task_table.update_item(
                        Key={'url': crawled_url},
                        UpdateExpression="SET #s = :s",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":s": "skipped"}
                    )
            else:
                logging.info(f"[Info] {crawler_id} is alive (last seen {int(time_diff)} seconds ago).")

    # This function receives crawler status reports, handles retry, DLQ, and blocked URLs
def monitor_crawler_reports(self):
        logging.info("[Monitor] Checking crawler reports...")
        while True:
            response = self.sqs.receive_message(
                QueueUrl=self.report_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5
            )
            messages = response.get('Messages', [])
            if not messages:
                break

            for message in messages:
                body = json.loads(message['Body'])
                crawler_id = body.get('crawler_id', 'unknown')
                crawled_url = body.get('url', 'unknown')
                extracted_urls = body.get('extracted_urls', [])
                depth = body.get('depth') or 0
                status = body.get('status', 'unknown')
                error = body.get('error', '')

                if status == 'success':
                    logging.info(f"[{crawler_id}] Successfully crawled: {crawled_url} at depth {depth}")
                    if depth < self.max_depth:
                        for url in extracted_urls:
                            self.send_url_to_crawl_queue(url, depth=depth+1)
                    self.task_table.update_item(
                        Key={'url': crawled_url},
                        UpdateExpression="SET #s = :s",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":s": "done"}
                    )
                else:
                    logging.warning(f"[{crawler_id}] Failed crawling: {crawled_url} Reason: {error}")
                    response = self.task_table.get_item(Key={'url': crawled_url})
                    retries = response['Item'].get('retries', 0)
                    if retries < 3:
                        self.send_url_to_crawl_queue(crawled_url, depth=depth)
                        self.task_table.update_item(
                            Key={'url': crawled_url},
                            UpdateExpression="SET retries = :r, assigned_at = :t",
                            ExpressionAttributeValues={
                                ":r": retries + 1,
                                ":t": datetime.now(timezone.utc).isoformat()
                            }
                        )
                    else:
                        self.send_to_dead_letter_queue(crawled_url, reason=error)
                        self.task_table.update_item(
                            Key={'url': crawled_url},
                            UpdateExpression="SET #s = :s",
                            ExpressionAttributeNames={"#s": "status"},
                            ExpressionAttributeValues={":s": "failed"}
                        )

                self.sqs.delete_message(
                    QueueUrl=self.report_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )

def send_to_dead_letter_queue(self, url, reason="max retries exceeded"):
        message = {
            "url": url,
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self.sqs.send_message(
            QueueUrl=self.dead_letter_queue_url,
            MessageBody=json.dumps(message)
        )
        logging.warning(f"[DLQ] Sent URL to dead-letter queue: {url}")

def monitor_task_timeouts(self):
        logging.info("[Monitor] Checking for task timeouts...")
        now = datetime.now(timezone.utc)

        response = self.task_table.scan()
        for item in response['Items']:
            status = item.get('status', '')
            if status != 'pending':
                continue

            assigned_at = datetime.fromisoformat(item['assigned_at'])
            retries = item.get('retries', 0)
            url = item['url']
            depth = item['depth']

            if (now - assigned_at).total_seconds() > self.TIMEOUT_SECONDS:
                if retries < 3:
                    logging.warning(f"[Timeout] Requeuing stale task: {url}")
                    self.send_url_to_crawl_queue(url, depth=depth)
                    self.task_table.update_item(
                        Key={'url': url},
                        UpdateExpression="SET assigned_at = :t, retries = :r",
                        ExpressionAttributeValues={
                            ":t": now.isoformat(),
                            ":r": retries + 1
                        }
                    )
                else:
                    logging.error(f"[Fail] Task {url} exceeded retry limit. Sending to DLQ.")
                    self.send_to_dead_letter_queue(url, reason="timeout exceeded")
                    self.task_table.update_item(
                        Key={'url': url},
                        UpdateExpression="SET #s = :s",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":s": "failed"}
                    )


def is_blocked_url(self, url):
        response = self.blocked_table.get_item(Key={'url': url})
        return 'Item' in response


if __name__ == "__main__":
    master = MasterNode(
        region_name='us-east-1',
        crawl_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/CrawlQueue',
        report_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/ReportQueue',
        heartbeat_table_name='CrawlerHeartbeatTable',
        task_table_name='CrawlerTaskAssignments',
        dead_letter_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/DeadLetterQueue',
        blocked_table_name = ='BlockedUrlsTable',
        max_depth=2
    )

    BATCH_SEED_URLS = [
        "http://siveen.com",
        "http://soso.com/about",
        "http://hfffff.com/contact"
    ]

    logging.info("[Master] Submitting Seed URLs to CrawlQueue...")
    master.submit_seed_urls(BATCH_SEED_URLS)
    logging.info("[Master] Monitoring CrawlQueue and Crawler Health...")
    master.monitor_crawl_queue()
