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
                   index_status_table_name, ResponseQueue, request_queue_url, search_queue_url, indexer_heartbeat_table_name, max_depth=2):
         self.region_name = region_name
         self.crawl_queue_url = crawl_queue_url
         self.report_queue_url = report_queue_url
         self.heartbeat_table_name = heartbeat_table_name
         self.task_table_name = task_table_name
         self.dead_letter_queue_url = dead_letter_queue_url
         self.index_feedback_queue_url = index_feedback_queue_url
         self.index_status_table_name = index_status_table_name
         self.blocked_table_name = blocked_table_name
         self.ResponseQueue = ResponseQueue
         self.max_depth = max_depth
         self.request_queue_url = request_queue_url
         self.search_queue_url = search_queue_url
         self.indexer_heartbeat_table_name = indexer_heartbeat_table_name
         self.TIMEOUT_SECONDS = 120
         self.running = False
         self.last_crawl_count = 0
         self.last_crawl_time = time.time()
         self.last_indexed_count = 0
         self.last_indexed_time = time.time()
         self._init_aws_clients()
         self._init_logging()

    def _init_aws_clients(self):
        """Initialize AWS service clients"""
        try:
            self.sqs = boto3.client('sqs', region_name=self.region_name)
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name)
            self.heartbeat_table = self.dynamodb.Table(self.heartbeat_table_name)
            self.indexer_heartbeat_table = self.dynamodb.Table(self.indexer_heartbeat_table_name)
            self.task_table = self.dynamodb.Table(self.task_table_name)
            self.blocked_table = self.dynamodb.Table(self.blocked_table_name)
            self.index_status_table = self.dynamodb.Table(self.index_status_table_name)
        except Exception as e:
            logging.error(f"Failed to initialize AWS clients: {str(e)}")
            raise

    def _init_logging(self):
        """Initialize logging configuration"""
        try:
            logging.basicConfig(
                filename='master_log.log',
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
        except Exception as e:
            print(f"Failed to initialize logging: {str(e)}")
            raise

    def send_url_to_crawl_queue(self, url, domain, depth=0):
        """
        Send a URL to the crawl queue for processing.
        
        Args:
            url (str): The URL to crawl
            domain (str): The domain of the URL
            depth (int): The current crawl depth (default: 0)
            
        Raises:
            ValueError: If url or domain is invalid
        """
        if not url or not isinstance(url, str):
            raise ValueError("URL must be a non-empty string")
        if not domain or not isinstance(domain, str):
            raise ValueError("Domain must be a non-empty string")
        if not isinstance(depth, int) or depth < 0:
            raise ValueError("Depth must be a non-negative integer")
            
        if self.is_blocked_url(url):
            logging.warning(f"[Master] Skipping blocked URL: {url}")
            return
        response = self.task_table.get_item(Key={'url': url})
        if 'Item' in response:
            existing_task = response['Item']
            existing_status = existing_task.get('status')
            existing_depth = existing_task.get('depth', 0)
            # If URL is already pending with same or lower depth, skip it
            if existing_status == 'pending' and existing_depth >= depth:
                logging.info(f"[Master] Skipping URL already pending with depth {existing_depth}: {url}")
                return       
                
        try:
            assigned_at = datetime.now(timezone.utc).isoformat()
            message = {
                "url": url,
                "depth": depth,
                "domain": domain,
                "assigned_at": assigned_at
            }
            self.sqs.send_message(
                QueueUrl=self.crawl_queue_url,
                MessageBody=json.dumps(message, cls=DecimalEncoder)
            )
            logging.info(f"[Master] Sent URL to CrawlQueue: {url} (depth={depth})")        
            
            
            # If URL doesn't exist in task table, create new task and send to queue
            assigned_at = datetime.now(timezone.utc).isoformat()
            self.task_table.put_item(
                Item={
                    'url': url,
                    'assigned_at': assigned_at,
                    'depth': Decimal(str(depth)),
                    'domain': domain,
                    'status': 'pending',
                    'retries': Decimal(str(0))
                }
            )
        except Exception as e:
            logging.error(f"Failed to send URL to crawl queue: {str(e)}")
            raise

    def wake_up_crawler(self, crawler_id):
        """Wake up a specific crawler by sending a wake-up signal"""
        wake_message = {
            "wake_up": True,
            "crawler_id": crawler_id
        }
        self.sqs.send_message(
            QueueUrl=self.crawl_queue_url,
            MessageBody=json.dumps(wake_message, cls=DecimalEncoder)
        )
        logging.info(f"[Master] Sent wake-up signal to {crawler_id}")

    def wake_up_all_crawlers(self):
        """Wake up all crawlers that are in shutdown state"""
        logging.info("[Master] Attempting to wake up all crawlers...")
        response = self.heartbeat_table.scan()
        for item in response['Items']:
            crawler_id = item['crawler_id']
            if item.get('status') == 'shutdown':
                self.wake_up_crawler(crawler_id)
                logging.info(f"[Master] Woke up crawler: {crawler_id}")

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
                client_id = body.get('client_id')

                if msg_type == 'search':
                    self.sqs.send_message(
                        QueueUrl=self.search_queue_url,
                        MessageBody=json.dumps(body)
                    )
                    logging.info(f"[Request] Forwarded search query to search queue: {body}")
                elif msg_type == 'crawl':
                    url = body.get('url')
                    domain = body.get('domain')
                    depth = body.get('depth', 0)
                    assigned_at = datetime.now(timezone.utc).isoformat()
                    
                    # Record in task table
                    self.task_table.put_item(
                        Item={
                            'url': url,
                            'assigned_at': assigned_at,
                            'depth': Decimal(str(depth)),
                            'domain': domain,
                            'status': 'pending',
                            'retries': Decimal(str(0))
                        }
                    )
                    
                    # Forward to crawl queue
                    self.sqs.send_message(
                        QueueUrl=self.crawl_queue_url,
                        MessageBody=json.dumps(body)
                    )
                    logging.info(f"[Request] Forwarded crawl request to crawl queue: {body}")
                elif msg_type == 'monitoring_request':
                    # Collect monitoring data
                    monitoring_data = {
                        'active_crawlers': self.count_active_crawlers(),
                        'crawled_urls': self.count_crawled_urls(),
                        'crawl_rate': self.get_crawl_rate(),
                        'indexing_rate': self.get_indexing_rate(),
                        'error_rates': self.get_error_rates(),
                        'crawler_status': self.get_crawler_status(),
                        'queue_status': self.get_queue_status()
                    }
                    
                    # Send monitoring data back to client
                    response_message = {
                        'type': 'monitoring_data',
                        'client_id': client_id,
                        'data': monitoring_data
                    }
                    self.sqs.send_message(
                        QueueUrl=self.ResponseQueue,
                        MessageBody=json.dumps(response_message)
                    )
                    logging.info(f"[Request] Sent monitoring data to client {client_id}")

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
        logging.info("[Monitor] Checking indexer feedback queue...")  #! Add logging for indexer feedback
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
                        'url': json.loads(msg_content).get('url', 'unknown'),
                        'status': status,
                        'timestamp': timestamp
                    }
                else:
                    item = {
                        'url': msg_content,
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
        self.running = True
        try:
            while self.running:
                self.run_all_monitoring_tasks()
                time.sleep(30)
        except KeyboardInterrupt:
            logging.info("[Master] Received shutdown signal. Initiating graceful shutdown...")
            self.shutdown()
        except Exception as e:
            logging.error(f"[Master] Unexpected error in monitor_crawl_queue: {str(e)}")
            self.shutdown()


    def monitor_crawlers_health(self):
        logging.info("[Monitor] Checking crawler heartbeats...")
        response = self.heartbeat_table.scan()
        now = datetime.now(timezone.utc)

        for item in response['Items']:
            crawler_id = item['crawler_id']
            last_heartbeat = datetime.fromisoformat(item['last_heartbeat'])
            time_diff = (now - last_heartbeat).total_seconds()
            if time_diff > 120 and item.get('status') not in ['failed', 'shutdown']:
                logging.warning(f"[Warning] {crawler_id} missed heartbeat! Declaring as failed.")
                self.heartbeat_table.update_item(
                    Key={'crawler_id': crawler_id},
                    UpdateExpression="SET #s = :s",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":s": "failed"}
                )
                self.handle_failed_crawler(item)
            else:
                logging.info(f"[Info] {crawler_id} is alive (last seen {int(time_diff)} seconds ago).")

    def handle_failed_crawler(self, crawler_item):
        failed_task_url = crawler_item.get('current_task_url')
        if failed_task_url:
            # Check if the task is still pending and not already failed/timed out
            task = self.task_table.get_item(Key={'url': failed_task_url}).get('Item')
            if task and task.get('status') == 'pending':
                # Update the task's assigned_at timestamp to the current time
                now = datetime.now(timezone.utc)
                self.task_table.update_item(
                    Key={'url': failed_task_url},
                    UpdateExpression="SET assigned_at = :t",
                    ExpressionAttributeValues={":t": now.isoformat()}
                )
                logging.info(f"[Recovery] Updated timestamp for task {failed_task_url} from failed crawler {crawler_item['crawler_id']}")

        # Check for shutdown crawlers
        response = self.heartbeat_table.scan()
        shutdown_crawlers = [item['crawler_id'] for item in response['Items'] if item.get('status') == 'shutdown']

        if not shutdown_crawlers:
            logging.info("[Recovery] No shutdown crawlers found. Starting a new EC2 instance.")
            self.start_backup_crawler()
        else:
            crawler_id = shutdown_crawlers[0]
            logging.info(f"[Recovery] Waking up shutdown crawler: {crawler_id}")
            self.wake_up_crawler(crawler_id)

    def start_backup_crawler(self):
        ec2 = boto3.client('ec2', region_name=self.region_name)
        # Use your AMI, instance type, security group, etc. Replace ImageId with your AMI ID
        try:
            response = ec2.run_instances(
                ImageId='ami-xxxxxxx',  #!AMI ID ON AWS MN MARIAM isA
                InstanceType='t2.micro',
                MinCount=1,
                MaxCount=1,
                UserData='''#!/bin/bash
                cd /path/to/crawler
                python crawler.py
                '''
            )
            instance_id = response['Instances'][0]['InstanceId']
            logging.info(f"[Recovery] Starting backup crawler node with instance ID: {instance_id}")
            # Wait for the instance to be running
            waiter = ec2.get_waiter('instance_running')
            waiter.wait(InstanceIds=[instance_id])
            logging.info(f"[Recovery] Backup crawler node {instance_id} is now running.")
        except Exception as e:
            logging.error(f"[Recovery] Failed to start backup crawler: {e}")

    def count_crawled_urls(self):
        scanned = self.task_table.scan()
        done = sum(1 for item in scanned['Items'] if item['status'] == 'done')
        logging.info(f"Total URLs crawled successfully: {done}")
        print(f"Total URLs crawled successfully: {done}")

    def monitor_crawler_reports(self):
        logging.info("[Monitor] Checking crawler reports...")
        while True:
            try:
                response = self.sqs.receive_message(
                    QueueUrl=self.report_queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5
                )
                messages = response.get('Messages', [])
                if not messages:
                    break

                for message in messages:
                    try:
                        body = json.loads(message['Body'])
                        crawler_id = body.get('crawler_id', 'unknown')
                        crawled_url = body.get('url', '')
                        extracted_urls = body.get('extracted_urls', [])
                        depth = body.get('depth') or 0
                        status = body.get('status', 'unknown')
                        error = body.get('error', '')
                        domain = body.get('domain', None)
                        
                        if status == 'success':
                            logging.info(f"[{crawler_id}] Successfully crawled: {crawled_url} at depth {depth}")
                            if depth < self.max_depth:
                                for url in extracted_urls:
                                    self.send_url_to_crawl_queue(url, domain, depth=depth+1)
                            self.task_table.update_item(
                                Key={'url': crawled_url},
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
                                Key={'url': crawled_url},
                                UpdateExpression="SET #s = :s",
                                ExpressionAttributeNames={"#s": "status"},
                                ExpressionAttributeValues={":s": "skipped"}
                            )
                        elif status == 'shutdown':
                            logging.info(f"[{crawler_id}] {error}")
                        else:
                            logging.warning(f"[{crawler_id}] Failed crawling: {crawled_url} Reason: {error}")
                            try:
                                response = self.task_table.get_item(Key={'url': crawled_url})
                                retries = response.get('Item', {}).get('retries', 0)
                                if retries < 3:
                                    self.send_url_to_crawl_queue(crawled_url, domain, depth=depth)
                                    self.task_table.update_item(
                                        Key={'url': crawled_url},
                                        UpdateExpression="SET retries = :r, assigned_at = :t",
                                        ExpressionAttributeValues={
                                            ":r": Decimal(str(retries + 1)),
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
                            except Exception as e:
                                logging.error(f"Error processing failed crawl task: {str(e)}")

                        self.sqs.delete_message(
                            QueueUrl=self.report_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                    except Exception as e:
                        logging.error(f"Error processing message: {str(e)}")
                        continue
            except Exception as e:
                logging.error(f"Error in monitor_crawler_reports: {str(e)}")
                time.sleep(5)  # Add delay before retrying

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
            url = item.get('url')
            depth = item.get('depth')
            domain = item.get('domain', None)

            if (now - datetime.fromisoformat(assigned_at)).total_seconds() > self.TIMEOUT_SECONDS:
                if retries < 3:
                    logging.warning(f"[Timeout] Requeuing stale task: {url}")
                    self.send_url_to_crawl_queue(url, domain, depth=depth)
                    # Delete old item
                    self.task_table.delete_item(Key={'url': url})
                    # Put new item with updated assigned_at and incremented retries
                    self.task_table.put_item(
                        Item={
                            'url': url,
                            'assigned_at': now.isoformat(),
                            'depth': Decimal(str(depth)),
                            'status': 'pending',
                            'retries': Decimal(str(retries + 1))
                        }
                    )
                else:
                    self.send_to_dead_letter_queue(url, reason="timeout and max retries exceeded")
                    self.task_table.update_item(
                        Key={'url': url},
                        UpdateExpression="SET #s = :s",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":s": "failed"}
                    )

    def is_blocked_url(self, url):
        response = self.blocked_table.get_item(Key={'url': url})
        return 'Item' in response

    def monitor_indexers_health(self):
        """Monitor the health of indexer nodes by checking their heartbeats"""
        logging.info("[Monitor] Checking indexer heartbeats...")
        response = self.indexer_heartbeat_table.scan()
        now = datetime.now(timezone.utc)

        for item in response['Items']:
            indexer_id = item['indexer_id']
            last_heartbeat = datetime.fromisoformat(item['last_heartbeat'])
            time_diff = (now - last_heartbeat).total_seconds()
            
            if time_diff > 120 and item.get('status') not in ['failed', 'shutdown']:
                logging.warning(f"[Warning] {indexer_id} missed heartbeat! Declaring as failed.")
                self.indexer_heartbeat_table.update_item(
                    Key={'indexer_id': indexer_id},
                    UpdateExpression="SET #s = :s",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":s": "failed"}
                )
            else:
                logging.info(f"[Info] {indexer_id} is alive (last seen {int(time_diff)} seconds ago).")

    def run_all_monitoring_tasks(self):
        """Run all monitoring tasks in sequence"""
        logging.info("[Master] Starting comprehensive monitoring cycle")
        
        # Monitor client requests
        self.monitor_client_requests()
        
        # Monitor crawler health and reports
        self.monitor_crawlers_health()
        self.monitor_crawler_reports()
        
        # Monitor indexer health and feedback
        self.monitor_indexers_health()
        self.monitor_indexer_feedback()
        
        # Monitor task timeouts
        self.monitor_task_timeouts()
        
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
            self.print_crawl_quality_metrics()
            self.compute_index_search_error_rates()
        else:
            # Dynamic scaling logic
            active_crawlers = self.count_active_crawlers()
            min_crawlers = 1
            desired_crawlers = max(min_crawlers, min(num_messages // 10 + 1, 10))

            if active_crawlers < desired_crawlers:
                num_to_start = desired_crawlers - active_crawlers
                self.ensure_crawlers(num_to_start)
            elif active_crawlers > desired_crawlers:
                response = self.heartbeat_table.scan()
                running_crawlers = [item['crawler_id'] for item in response['Items'] if item.get('status') == 'running']
                # Shut down excess crawlers, but keep at least min_crawlers
                for crawler_id in running_crawlers[desired_crawlers:]:
                    self.send_shutdown_signal_to_crawler(crawler_id)
                logging.info(f"[Scaling] Sent shutdown to {active_crawlers - desired_crawlers} crawlers (active: {active_crawlers} -> {desired_crawlers})")
            else:
                logging.info(f"[Scaling] No scaling action needed. Active crawlers: {active_crawlers}, Desired: {desired_crawlers}")

        # Print dashboard at the end of the monitoring cycle
        self.print_dashboard()

    def count_active_crawlers(self):
        """Return the number of crawlers with status 'running'."""
        response = self.heartbeat_table.scan()
        return sum(1 for item in response['Items'] if item.get('status') == 'running')

    def send_shutdown_signal_to_crawler(self, crawler_id):
        shutdown_message = {
            "shutdown": True,
            "crawler_id": crawler_id
        }
        self.sqs.send_message(
            QueueUrl=self.crawl_queue_url,
            MessageBody=json.dumps(shutdown_message, cls=DecimalEncoder)
        )
        logging.info(f"[Master] Sent shutdown signal to {crawler_id}")

    def ensure_crawlers(self, num_to_start):
        """Wake up shutdown crawlers if available, otherwise start new ones."""
        response = self.heartbeat_table.scan()
        shutdown_crawlers = [item['crawler_id'] for item in response['Items'] if item.get('status') == 'shutdown']
        num_woken = 0
        for crawler_id in shutdown_crawlers[:num_to_start]:
            self.wake_up_crawler(crawler_id)
            num_woken += 1
        num_to_start_new = num_to_start - num_woken
        for _ in range(num_to_start_new):
            self.start_backup_crawler()
        logging.info(f"[Scaling] Woke up {num_woken} shutdown crawlers, started {num_to_start_new} new crawlers.")

    def print_crawl_quality_metrics(self):
        scanned = self.task_table.scan()
        total = len(scanned['Items'])
        crawled = sum(1 for item in scanned['Items'] if item['status'] == 'done')
        failed = sum(1 for item in scanned['Items'] if item['status'] == 'failed')
        skipped = sum(1 for item in scanned['Items'] if item['status'] == 'skipped')
        coverage = (crawled / total) * 100 if total > 0 else 0
        error_rate = (failed / total) * 100 if total > 0 else 0

        print(f"Crawl Coverage: {coverage:.2f}% ({crawled}/{total})")
        print(f"Error rate: {error_rate:.2f}% (Failed: {failed}, Total: {total})")
        print(f"Politeness: URLs skipped due to robots.txt: {skipped}")

    def print_crawler_node_status(self):
        response = self.heartbeat_table.scan()
        active = sum(1 for item in response['Items'] if item.get('status') == 'running')
        failed = sum(1 for item in response['Items'] if item.get('status') == 'failed')
        shutdown = sum(1 for item in response['Items'] if item.get('status') == 'shutdown')
        print(f"Crawler Nodes - Active: {active}, Failed: {failed}, Shutdown: {shutdown}")

    def print_crawl_rate(self):
        scanned = self.task_table.scan()
        crawled = sum(1 for item in scanned['Items'] if item['status'] == 'done')
        now = time.time()
        elapsed = now - self.last_crawl_time
        if elapsed > 0:
            rate = (crawled - self.last_crawl_count) / elapsed
            print(f"Crawl rate: {rate:.2f} URLs/sec")
        self.last_crawl_count = crawled
        self.last_crawl_time = now

    def print_indexing_rate(self):
        response = self.index_status_table.scan()
        indexed = sum(1 for item in response['Items'] if item['status'] == 'index_success')
        now = time.time()
        elapsed = now - self.last_indexed_time
        if elapsed > 0:
            rate = (indexed - self.last_indexed_count) / elapsed
            print(f"Indexing rate: {rate:.2f} URLs/sec")
        self.last_indexed_count = indexed
        self.last_indexed_time = now

    def print_dashboard(self):
        print("==== Monitoring Dashboard ====")
        self.count_crawled_urls()
        self.compute_index_search_error_rates()
        self.print_crawl_quality_metrics()
        self.print_crawler_node_status()
        self.print_crawl_rate()
        self.print_indexing_rate()
        print("=============================")
        
    def shutdown(self):
        """Handle graceful shutdown of the master node"""
        logging.info("[Master] Initiating graceful shutdown...")
        self.running = False
        try:
            # Send shutdown signals to all crawlers
            self.send_shutdown_signal_to_crawlers()
            
            # Print final statistics
            self.print_dashboard()
            
            logging.info("[Master] Shutdown complete")
        except Exception as e:
            logging.error(f"[Master] Error during shutdown: {str(e)}")
            raise

#!Option 2: Use an AMI (Amazon Machine Image)
#!Set up one EC2 instance with everything installed and configured.
#!Create an AMI (a snapshot) from it.
#!Launch multiple EC2s from that AMI — they're all preloaded with your app and ready to go.
#!Faster boot time, no need to re-download code or install dependencies.
    
if __name__ == "__main__":
    master = MasterNode(
        region_name='us-east-1',
        crawl_queue_url='https://sqs.us-east-1.amazonaws.com/353176954707/CrawlQueue',
        report_queue_url='https://sqs.us-east-1.amazonaws.com/353176954707/ReportQueue',
        heartbeat_table_name='CrawlerHeartbeatTable',
        task_table_name='CrawlerTaskAssignmets',
        dead_letter_queue_url='https://sqs.us-east-1.amazonaws.com/353176954707/DeadLetterQueue',
        blocked_table_name='BlockedUrlsTable',
        index_feedback_queue_url = 'https://sqs.us-east-1.amazonaws.com/353176954707/FeedbackQueue',
        request_queue_url= 'https://sqs.us-east-1.amazonaws.com/353176954707/RequestQueue',
        ResponseQueue= 'https://sqs.us-east-1.amazonaws.com/353176954707/ResponseQueue',
        search_queue_url = 'https://sqs.us-east-1.amazonaws.com/353176954707/SearchQueue',
        index_status_table_name = 'IndexerTaskAssignments',
        indexer_heartbeat_table_name = 'IndexerHeartbeatTable',
        max_depth=2)

    logging.info("[Master] Starting comprehensive monitoring...")
    master.monitor_crawl_queue()
    ####
