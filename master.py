import boto3
import json
import time
import logging
from datetime import datetime, timezone
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

class MasterNode:
    def __init__(self, region_name, crawl_queue_url, report_queue_url, heartbeat_table_name, task_table_name, dead_letter_queue_url, blocked_table_name, index_feedback_queue_url,
                   index_status_table_name, ResponseQueue, request_queue_url,search_queue_url, max_depth=2):
         self.region_name = region_name
         self.crawl_queue_url = crawl_queue_url
         self.report_queue_url = report_queue_url
         self.heartbeat_table_name = heartbeat_table_name
         self.task_table_name = task_table_name
         self.dead_letter_queue_url = dead_letter_queue_url
         self.index_feedback_queue_url = index_feedback_queue_url
         self.index_status_table_name = index_status_table_name
         self.ResponseQueue = ResponseQueue
         self.max_depth = max_depth
         self.request_queue_url = request_queue_url
         self.sqs = boto3.client('sqs', region_name=self.region_name)
         self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name)
         self.heartbeat_table = self.dynamodb.Table(self.heartbeat_table_name)
         self.task_table = self.dynamodb.Table(self.task_table_name)
         self.blocked_table = self.dynamodb.Table(blocked_table_name)
         self.index_status_table = self.dynamodb.Table(self.index_status_table_name)
         self.search_queue_url = search_queue_url
         
         self.TIMEOUT_SECONDS = 120
         logging.basicConfig(filename='master_log.log', level=logging.INFO,
                             format='%(asctime)s [%(levelname)s] %(message)s')
 

    def send_url_to_crawl_queue(self, url, domain, depth=0):
        if self.is_blocked_url(url):
            logging.warning(f"[Master] Skipping blocked URL: {url}")
            return
        
        assigned_at = datetime.now(timezone.utc).isoformat()
        message = {
            "url": url,
            "depth": depth,
            "domain": domain,
            "assigned_at": assigned_at  # Include assigned_at in the message
        }
        self.sqs.send_message(
            QueueUrl=self.crawl_queue_url,
            MessageBody=json.dumps(message, cls=DecimalEncoder)
        )
        logging.info(f"[Master] Sent URL to CrawlQueue: {url} (depth={depth})")
    
        self.task_table.put_item(
            Item={
                'url': url,
                'assigned_at': assigned_at,  # Always include assigned_at
                'depth': depth,
                'domain': domain
                'status': 'pending',
                'retries': 0
            }
        )
       
    def monitor_client_requests(self):
        logging.info("[Monitor] Listening for client requests...")
        while True:
            response = self.sqs.receive_message(
                QueueUrl=self.request_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5
            )
            messages = response.get('Messages', [])
            if not messages:
                break

            for message in messages:
                body = json.loads(message['Body'])
                msg_type = body.get('type')

                if msg_type == 'search':
                    self.sqs.send_message(
                        QueueUrl=self.search_queue_url,
                        MessageBody=json.dumps(body)
                    )
                    logging.info(f"[Request] Forwarded search query to search queue: {body}")
                elif msg_type == 'crawl':
                    self.sqs.send_message(
                        QueueUrl=self.crawl_queue_url,
                        MessageBody=json.dumps(body)
                    )
                    logging.info(f"[Request] Forwarded crawl request to crawl queue: {body}")

                self.sqs.delete_message(
                    QueueUrl=self.request_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
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
                MessageBody=json.dumps(shutdown_message, cls=DecimalEncoder)
            )
            logging.info(f"[Master] Sent shutdown signal to {crawler_id}")
    def monitor_indexer_feedback(self):
        logging.info("[Monitor] Checking indexer feedback queue...")
        while True:
            response = self.sqs.receive_message(
                QueueUrl=self.index_feedback_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5
            )
            messages = response.get('Messages', [])
            if not messages:
                break

            for message in messages:
                body = json.loads(message['Body'])
                indexer_id = body.get('indexer_id', 'unknown')
                msg_content = body.get('message')
                status = body.get('status')
                error = body.get('error', '')
                timestamp = body.get('timestamp')

                if status == "search_success":
                    self.forward_search_result_to_client(msg_content)
                    item = {
                        'id': json.loads(msg_content).get('url', 'unknown'),
                        'status': status,
                        'timestamp': timestamp
                    }
                else:
                    item = {
                        'id': msg_content,
                        'status': status,
                        'timestamp': timestamp
                    }

                if error:
                    item['error'] = error

                self.index_status_table.put_item(Item=item)
                logging.info(f"[Indexer Feedback] {status}: {msg_content}")

                self.sqs.delete_message(
                    QueueUrl=self.index_feedback_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
    def forward_search_result_to_client(self, result_json_str):
        try:
            result_json = json.loads(result_json_str)
            self.sqs.send_message(
                QueueUrl=self.ResponseQueue,
                MessageBody=json.dumps(result_json)
            )
            logging.info(f"[Forwarded] Search result sent to client queue: {result_json}")
        except json.JSONDecodeError:
            logging.error(f"[Forwarded] Failed to decode result_json: {result_json_str}")

    def compute_index_search_error_rates(self):
        response = self.index_status_table.scan()
        items = response['Items']

        index_success = sum(1 for item in items if item['status'] == 'index_success')
        index_failed = sum(1 for item in items if item['status'] == 'index_failed')
        search_success = sum(1 for item in items if item['status'] == 'search_success')
        search_failed = sum(1 for item in items if item['status'] == 'search_failed')

        total_index = index_success + index_failed
        total_search = search_success + search_failed

        if total_index > 0:
            print(f"Indexing Error Rate: {(index_failed / total_index) * 100:.2f}%")
        else:
            print("Indexing Error Rate: 0.00%")

        if total_search > 0:
            print(f"Search Error Rate: {(search_failed / total_search) * 100:.2f}%")
        else:
            print("Search Error Rate: 0.00%")

        print(f"Total Indexed URLs: {index_success}")
    
    def monitor_crawl_queue(self):
        """Main monitoring loop that runs all monitoring tasks periodically"""
        while True:
            self.run_all_monitoring_tasks()
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
            else:
                logging.info(f"[Info] {crawler_id} is alive (last seen {int(time_diff)} seconds ago).")

    def count_crawled_urls(self):
        scanned = self.task_table.scan()
        done = sum(1 for item in scanned['Items'] if item['status'] == 'done')
        logging.info(f"Total URLs crawled successfully: {done}")
        print(f"Total URLs crawled successfully: {done}")

    def compute_error_rate(self):
        scanned = self.task_table.scan()
        total = len(scanned['Items'])
        failed = sum(1 for item in scanned['Items'] if item['status'] == 'failed')
        skipped = sum(1 for item in scanned['Items'] if item['status'] == 'skipped')
        logging.info(f" Failed: {failed}, Total: {total}")
        if total > 0:
            error_rate = (failed / total) * 100
            print(f"Error rate: {error_rate:.2f}%")
        else:
            print("Error rate: 0.00%")

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
                assigned_at = body.get('assigned_at')
                domain = body.get('domain', None)
                key = {'url': crawled_url, 'assigned_at': assigned_at} 

                if status == 'success':
                    logging.info(f"[{crawler_id}] Successfully crawled: {crawled_url} at depth {depth}")
                    if depth < self.max_depth:
                        for url in extracted_urls:
                            self.send_url_to_crawl_queue(url,domain, depth=depth+1)
                    self.task_table.update_item(
                        Key=key,
                        UpdateExpression="SET #s = :s",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":s": "done"}
                    )
                elif status == 'skipped':
                    logging.info(f"[{crawler_id}] Skipped URL (blocked by robots.txt): {crawled_url}")
                    self.blocked_table.put_item(Item={
                        'url': crawled_url,
                        'reason': error,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                    self.task_table.update_item(
                        Key=key,
                        UpdateExpression="SET #s = :s",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":s": "skipped"}
                    )
                else:
                    logging.warning(f"[{crawler_id}] Failed crawling: {crawled_url} Reason: {error}")
                    response = self.task_table.get_item(Key=key)
                    retries = response['Item'].get('retries', 0)
                    if retries < 3:
                        self.send_url_to_crawl_queue(crawled_url, depth=depth)
                        self.task_table.update_item(
                            Key=key,
                            UpdateExpression="SET retries = :r, assigned_at = :t",
                            ExpressionAttributeValues={
                                ":r": retries + 1,
                                ":t": datetime.now(timezone.utc).isoformat()
                            }
                        )
                    else:
                        self.send_to_dead_letter_queue(crawled_url, reason=error)
                        self.task_table.update_item(
                            Key=key,
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
            MessageBody=json.dumps(message, cls=DecimalEncoder)
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

            assigned_at = item.get('assigned_at')
            if not assigned_at:
                continue

            retries = item.get('retries', 0)
            url = item['url']
            depth = item['depth']
            domain = item['domain']

            if (now - datetime.fromisoformat(assigned_at)).total_seconds() > self.TIMEOUT_SECONDS:
                if retries < 3:
                    logging.warning(f"[Timeout] Requeuing stale task: {url}")
                    self.send_url_to_crawl_queue(url, domain, depth=depth)
                    # Delete old item
                    self.task_table.delete_item(Key={'url': url, 'assigned_at': assigned_at})
                    # Put new item with updated assigned_at and incremented retries
                    self.task_table.put_item(
                        Item={
                            'url': url,
                            'assigned_at': now.isoformat(),
                            'depth': depth,
                            'status': 'pending',
                            'retries': retries + 1
                        }
                    )
                else:
                    logging.error(f"[Fail] Task {url} exceeded retry limit. Sending to DLQ.")
                    self.send_to_dead_letter_queue(url, reason="timeout exceeded")
                    self.task_table.update_item(
                        Key={'url': url, 'assigned_at': assigned_at},
                        UpdateExpression="SET #s = :s",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":s": "failed"}
                    )

    def is_blocked_url(self, url):
        response = self.blocked_table.get_item(Key={'url': url})
        return 'Item' in response

    def run_all_monitoring_tasks(self):
        
        """Run all monitoring tasks in sequence"""
        logging.info("[Master] Starting comprehensive monitoring cycle")
        
        # Monitor client requests
        self.monitor_client_requests()
        
        # Monitor crawler health and reports
        self.monitor_crawlers_health()
        self.monitor_crawler_reports()
        
        # Monitor task timeouts
        self.monitor_task_timeouts()
        
        # Monitor indexer feedback
        self.monitor_indexer_feedback()
        
        # Monitor crawl queue status
        response = self.sqs.get_queue_attributes(
            QueueUrl=self.crawl_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        num_messages = int(response['Attributes']['ApproximateNumberOfMessages'])
        logging.info(f"[Monitor] Remaining URLs in queue: {num_messages}")
        
        # If queue is empty, perform completion tasks
        if num_messages == 0:
            logging.info("[Master] CrawlQueue is empty. Crawling seems complete!")
            self.send_shutdown_signal_to_crawlers()
            self.count_crawled_urls()
            self.compute_error_rate()
            self.compute_index_search_error_rates()

    
if __name__ == "__main__":
    master = MasterNode(
        region_name='us-east-1',
        crawl_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/CrawlQueue',
        report_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/ReportQueue',
        heartbeat_table_name='CrawlerHeartbeatTable',
        task_table_name='CrawlerTaskAssignments',
        dead_letter_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/DeadLetterQueue',
        blocked_table_name='BlockedUrlsTable',
        index_feedback_queue_url = 'https://sqs.us-east-1.amazonaws.com/138749495090/FeedbackQueue',
        request_queue_url= 'https://sqs.us-east-1.amazonaws.com/138749495090/RequestQueue',
        ResponseQueue= 'https://sqs.us-east-1.amazonaws.com/138749495090/ResponseQueue',
        search_queue_url = 'https://sqs.us-east-1.amazonaws.com/138749495090/SearchQueue',
        index_status_table_name = 'IndexerTaskAssignments',
        max_depth=2)

    logging.info("[Master] Starting comprehensive monitoring...")
    master.monitor_crawl_queue()
