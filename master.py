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

    def send_url_to_crawl_queue(self, url, domain, depth=0, max_depth=2):
        """
        Send a URL to the crawl queue for processing.
        
        Args:
            url (str): The URL to crawl
            domain (str): The domain of the URL
            depth (int/Decimal): The current crawl depth (default: 0)
            
        Raises:
            ValueError: If url or domain is invalid
        """
        if not url or not isinstance(url, str):
            raise ValueError("URL must be a non-empty string")
        
        # Convert depth to int if it's a Decimal
        if isinstance(depth, Decimal):
            depth = int(depth)
        elif not isinstance(depth, int) or depth < 0:
            logging.warning(f"[Master] Invalid depth: {depth}. ")
            
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
                "max_depth": max_depth,
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
            
            # Only update dashboard for new root URLs (depth 0)
            if depth == 0:
                self.print_dashboard()
                
        except Exception as e:
            logging.error(f"Failed to send URL to crawl queue: {str(e)}")
            raise

    def wake_up_crawler(self, crawler_id):
        """Wake up a crawler that's in shutdown state"""
        try:
            wake_message = {
                "wake_up": True,
                "crawler_id": crawler_id
            }
            self.sqs.send_message(
                QueueUrl=self.crawl_queue_url,
                MessageBody=json.dumps(wake_message, cls=DecimalEncoder)
            )
            
            # Update heartbeat table to mark as starting
            self.heartbeat_table.update_item(
                Key={'crawler_id': crawler_id},
                UpdateExpression="SET #s = :s, last_heartbeat = :t",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":s": "starting",
                    ":t": datetime.now(timezone.utc).isoformat()
                }
            )
            logging.info(f"[Crawler] Sent wake-up signal to {crawler_id}")
            return True
        except Exception as e:
            logging.error(f"[Crawler] Failed to wake up crawler {crawler_id}: {str(e)}")
            return False

    def verify_crawler_state(self, crawler_id):
        """Verify crawler state is consistent between EC2 and heartbeat table"""
        try:
            # Check EC2 state
            ec2 = boto3.client('ec2', region_name=self.region_name)
            response = ec2.describe_instances(InstanceIds=[crawler_id])
            if not response['Reservations']:
                return False
            ec2_state = response['Reservations'][0]['Instances'][0]['State']['Name']
            
            # Check heartbeat state
            heartbeat_response = self.heartbeat_table.get_item(Key={'crawler_id': crawler_id})
            if 'Item' not in heartbeat_response:
                return False
            heartbeat_state = heartbeat_response['Item'].get('status')
            
            # State should be consistent
            if ec2_state == 'running' and heartbeat_state == 'running':
                return True
            elif ec2_state == 'running' and heartbeat_state == 'shutdown':
                # Crawler is running but marked as shutdown - update heartbeat
                self.heartbeat_table.update_item(
                    Key={'crawler_id': crawler_id},
                    UpdateExpression="SET #s = :s, last_heartbeat = :t",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":s": "running",
                        ":t": datetime.now(timezone.utc).isoformat()
                    }
                )
                return True
            elif ec2_state == 'stopped' and heartbeat_state == 'running':
                # Crawler is stopped but marked as running - update heartbeat
                self.heartbeat_table.update_item(
                    Key={'crawler_id': crawler_id},
                    UpdateExpression="SET #s = :s",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":s": "shutdown"}
                )
                return True
            
            return False
        except Exception as e:
            logging.error(f"[Verify] Error verifying crawler {crawler_id} state: {str(e)}")
            return False

    def wait_for_crawler_ready(self, crawler_id, timeout=300):
        """Wait for a crawler to be fully ready (running and in heartbeat table)"""
        logging.info(f"[Startup] Waiting for crawler {crawler_id} to be ready...")
        start_time = time.time()
        consecutive_failures = 0
        
        while time.time() - start_time < timeout:
            try:
                # Verify both EC2 and heartbeat states
                if not self.verify_crawler_state(crawler_id):
                    consecutive_failures += 1
                    if consecutive_failures >= 3:  # 3 consecutive failures
                        logging.error(f"[Startup] Crawler {crawler_id} failed to stabilize")
                        return False
                    time.sleep(10)
                    continue
                
                # Check if crawler is in heartbeat table and running
                response = self.heartbeat_table.get_item(Key={'crawler_id': crawler_id})
                if 'Item' in response:
                    status = response['Item'].get('status')
                    last_heartbeat = response['Item'].get('last_heartbeat')
                    
                    if status == 'running':
                        # Verify heartbeat is recent (within last 30 seconds)
                        if last_heartbeat:
                            last_heartbeat_time = datetime.fromisoformat(last_heartbeat)
                            if (datetime.now(timezone.utc) - last_heartbeat_time).total_seconds() <= 30:
                                logging.info(f"[Startup] Crawler {crawler_id} is fully ready")
                                return True
                
                consecutive_failures = 0  # Reset on successful check
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logging.error(f"[Startup] Error checking crawler {crawler_id}: {str(e)}")
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    return False
                time.sleep(10)
        
        logging.error(f"[Startup] Timeout waiting for crawler {crawler_id}")
        return False

    def initialize_crawler_pool(self):
        """Initialize the crawler pool with 2 crawlers, then put one in shutdown"""
        logging.info("[Startup] Initializing crawler pool...")
        try:
            # Start two crawlers
            crawler_ids = []
            for _ in range(2):
                instance_id = self.start_backup_crawler()
                if instance_id:
                    crawler_ids.append(instance_id)
            
            if len(crawler_ids) < 2:
                raise Exception("Failed to start initial crawlers")
            
            # Wait for both crawlers to be ready
            for crawler_id in crawler_ids:
                if not self.wait_for_crawler_ready(crawler_id):
                    # Cleanup failed crawler
                    try:
                        ec2 = boto3.client('ec2', region_name=self.region_name)
                        ec2.terminate_instances(InstanceIds=[crawler_id])
                        self.heartbeat_table.delete_item(Key={'crawler_id': crawler_id})
                    except Exception as e:
                        logging.error(f"[Startup] Failed to cleanup crawler {crawler_id}: {str(e)}")
                    raise Exception(f"Crawler {crawler_id} failed to become ready")
            
            # Verify crawler states one final time
            for crawler_id in crawler_ids:
                if not self.verify_crawler_state(crawler_id):
                    raise Exception(f"Crawler {crawler_id} state verification failed")
            
            # Put one crawler into shutdown state
            crawler_to_shutdown = crawler_ids[0]
            self.send_shutdown_signal_to_crawler(crawler_to_shutdown)
            
            # Wait for shutdown to take effect
            shutdown_timeout = 60  # 1 minute timeout
            start_time = time.time()
            while time.time() - start_time < shutdown_timeout:
                response = self.heartbeat_table.get_item(Key={'crawler_id': crawler_to_shutdown})
                if 'Item' in response and response['Item'].get('status') == 'shutdown':
                    logging.info(f"[Startup] Successfully put crawler {crawler_to_shutdown} into shutdown state")
                    return True
                time.sleep(5)
            
            raise Exception(f"Failed to put crawler {crawler_to_shutdown} into shutdown state")
            
        except Exception as e:
            logging.error(f"[Startup] Failed to initialize crawler pool: {str(e)}")
            # Cleanup any remaining crawlers
            for crawler_id in crawler_ids:
                try:
                    ec2 = boto3.client('ec2', region_name=self.region_name)
                    ec2.terminate_instances(InstanceIds=[crawler_id])
                    self.heartbeat_table.delete_item(Key={'crawler_id': crawler_id})
                except Exception as cleanup_error:
                    logging.error(f"[Startup] Failed to cleanup crawler {crawler_id}: {str(cleanup_error)}")
            return False

    def send_shutdown_signal_to_crawler(self, crawler_id):
        """Send shutdown signal to a crawler with verification"""
        try:
            # Verify crawler exists and is running
            if not self.verify_crawler_state(crawler_id):
                logging.error(f"[Shutdown] Cannot shutdown crawler {crawler_id} - invalid state")
                return False

            # Check if crawler is already in shutdown state
            response = self.heartbeat_table.get_item(Key={'crawler_id': crawler_id})
            if 'Item' in response and response['Item'].get('status') == 'shutdown':
                logging.info(f"[Shutdown] Crawler {crawler_id} is already in shutdown state")
                return True

            # Send shutdown signal
            shutdown_message = {
                "shutdown": True,
                "crawler_id": crawler_id
            }
            self.sqs.send_message(
                QueueUrl=self.crawl_queue_url,
                MessageBody=json.dumps(shutdown_message, cls=DecimalEncoder)
            )
            
            # Update heartbeat table to mark as shutdown
            self.heartbeat_table.update_item(
                Key={'crawler_id': crawler_id},
                UpdateExpression="SET #s = :s",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":s": "shutdown"}
            )
            
            # Verify shutdown state
            shutdown_timeout = 60  # 1 minute timeout
            start_time = time.time()
            while time.time() - start_time < shutdown_timeout:
                response = self.heartbeat_table.get_item(Key={'crawler_id': crawler_id})
                if 'Item' in response and response['Item'].get('status') == 'shutdown':
                    logging.info(f"[Shutdown] Successfully put crawler {crawler_id} into shutdown state")
                    return True
                time.sleep(5)
            
            logging.error(f"[Shutdown] Failed to verify shutdown state for crawler {crawler_id}")
            return False
            
        except Exception as e:
            logging.error(f"[Shutdown] Failed to send shutdown signal to {crawler_id}: {str(e)}")
            return False

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
                    depth = 0
                    max_depth = min(body.get('depth', 0), self.max_depth)
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
                    
                    # Forward to crawl queue with both depth and max_depth
                    crawl_message = {
                        "type": "crawl",
                        "url": url,
                        "depth": depth,
                        "max_depth": max_depth,
                        "domain": domain,
                        "client_id": client_id
                    }
                    self.sqs.send_message(
                        QueueUrl=self.crawl_queue_url,
                        MessageBody=json.dumps(crawl_message)
                    )
                    logging.info(f"[Request] Forwarded crawl request to crawl queue: {crawl_message}")

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
                client_id = body.get('client_id', None)
                query = body.get('query', None)

                if status == "search_success" or status == "search_failed":
                    self.forward_search_result_to_client(msg_content, client_id)
                    item = {
                        'url': query,  # Use query instead of url for search results
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
    
    def forward_search_result_to_client(self, result_json_str, client_id):
        try:
            # Handle both string and list results
            if isinstance(result_json_str, str):
                result_json = json.loads(result_json_str)
            else:
                result_json = result_json_str  # Already a Python object (list/dict)
                
            # Add client_id to the response
            response_message = {        
                'client_id': client_id,
                'result': result_json
            }
            self.sqs.send_message(
                QueueUrl=self.ResponseQueue,
                MessageBody=json.dumps(response_message)
            )
            logging.info(f"[Forwarded] Search result sent to client queue for client {client_id}: {result_json}")
        except json.JSONDecodeError:
            logging.error(f"[Forwarded] Failed to decode result_json: {result_json_str}")
        except Exception as e:
            logging.error(f"[Forwarded] Error forwarding search result: {str(e)}")


    def compute_index_search_error_rates(self):
        response = self.index_status_table.scan()
        items = response['Items']

        index_success = sum(1 for item in items if item['status'] == 'index_success')
        index_failed = sum(1 for item in items if item['status'] == 'index_failed')
        search_success = sum(1 for item in items if item['status'] == 'search_success')
        search_failed = sum(1 for item in items if item['status'] == 'search_failed')

        total_index = index_success + index_failed
        total_search = search_success + search_failed

        index_error_rate = (index_failed / total_index * 100) if total_index > 0 else 0
        search_error_rate = (search_failed / total_search * 100) if total_search > 0 else 0

        return {
            "index_error_rate": index_error_rate,
            "search_error_rate": search_error_rate,
            "total_indexed": index_success
        }

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


    def monitor_health(self, table_name, node_type):
        """Monitor the health of nodes by checking their heartbeats
        
        Args:
            table_name (str): Name of the heartbeat table to monitor
            node_type (str): Type of node ('crawler' or 'indexer')
        """
        logging.info(f"[Monitor] Checking {node_type} heartbeats...")
        table = self.dynamodb.Table(table_name)
        now = datetime.now(timezone.utc)

        for item in table.scan()['Items']:
            node_id = item[f'{node_type}_id']
            last_heartbeat = datetime.fromisoformat(item['last_heartbeat'])
            time_diff = (now - last_heartbeat).total_seconds()
            
            if time_diff > 120 and item.get('status') not in ['failed', 'shutdown']:
                logging.warning(f"[Warning] {node_id} missed heartbeat! Declaring as failed.")
                table.update_item(
                    Key={f'{node_type}_id': node_id},
                    UpdateExpression="SET #s = :s",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":s": "failed"}
                )
                if node_type == 'crawler':
                    self.handle_failed_crawler(item)
            else:
                logging.info(f"[Info] {node_id} is alive (last seen {int(time_diff)} seconds ago).")

    def monitor_crawlers_health(self):
        """Monitor the health of crawler nodes"""
        self.monitor_health(self.heartbeat_table_name, 'crawler')

    def monitor_indexers_health(self):
        """Monitor the health of indexer nodes"""
        self.monitor_health(self.indexer_heartbeat_table_name, 'indexer')

    def handle_failed_crawler(self, crawler_item):
        """Handle a failed crawler by attempting to reboot it"""
        crawler_id = crawler_item.get('crawler_id')
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
                logging.info(f"[Recovery] Updated timestamp for task {failed_task_url} from failed crawler {crawler_id}")

        try:
            # Try to reboot the failed crawler
            ec2 = boto3.client('ec2', region_name=self.region_name)
            response = ec2.describe_instances(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': ['CrawlerNode']
                    },
                    {
                        'Name': 'instance-state-name',
                        'Values': ['running', 'pending', 'stopping', 'stopped']
                    }
                ]
            )
            
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    if instance['InstanceId'] == crawler_id:
                        try:
                            ec2.reboot_instances(InstanceIds=[crawler_id])
                            logging.info(f"[Recovery] Attempting to reboot failed crawler {crawler_id}")
                            
                            # Update heartbeat table to mark as recovering
                            self.heartbeat_table.update_item(
                                Key={'crawler_id': crawler_id},
                                UpdateExpression="SET #s = :s, last_heartbeat = :t",
                                ExpressionAttributeNames={"#s": "status"},
                                ExpressionAttributeValues={
                                    ":s": "recovering",
                                    ":t": datetime.now(timezone.utc).isoformat()
                                }
                            )
                            return True
                        except Exception as e:
                            logging.error(f"[Recovery] Failed to reboot crawler {crawler_id}: {str(e)}")
                            break
                break
            
            # If reboot failed or crawler not found, start a new one
            logging.info(f"[Recovery] Starting new crawler to replace failed crawler {crawler_id}")
            self.start_backup_crawler()
            return True
            
        except Exception as e:
            logging.error(f"[Recovery] Error in crawler recovery process: {str(e)}")
            return False

    def wait_for_instance_running(self, instance_id, timeout=300):
        """Wait for an instance to be fully running and ready
        
        Args:
            instance_id (str): The ID of the instance to wait for
            timeout (int): Maximum time to wait in seconds (default 5 minutes)
            
        Returns:
            bool: True if instance is running and ready, False if timeout
        """
        logging.info(f"[Instance] Waiting for instance {instance_id} to be ready...")
        start_time = time.time()
        ec2 = boto3.client('ec2', region_name=self.region_name)
        
        while time.time() - start_time < timeout:
            try:
                # Check instance state
                response = ec2.describe_instances(InstanceIds=[instance_id])
                state = response['Reservations'][0]['Instances'][0]['State']['Name']
                
                if state == 'running':
                    # Check if instance is in heartbeat table (indicating crawler is ready)
                    response = self.heartbeat_table.scan(
                        FilterExpression='crawler_id = :id',
                        ExpressionAttributeValues={':id': instance_id}
                    )
                    if response.get('Items'):
                        logging.info(f"[Instance] Instance {instance_id} is fully ready")
                        return True
                
                elif state in ['terminated', 'shutting-down']:
                    logging.error(f"[Instance] Instance {instance_id} is terminating")
                    return False
                
                time.sleep(10)  # Wait 10 seconds before next check
                
            except Exception as e:
                logging.error(f"[Instance] Error checking instance {instance_id}: {str(e)}")
                time.sleep(10)
        
        logging.error(f"[Instance] Timeout waiting for instance {instance_id}")
        return False

    def get_running_instances(self):
        """Get list of running crawler instances that are fully ready"""
        try:
            ec2 = boto3.client('ec2', region_name=self.region_name)
            response = ec2.describe_instances(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': ['CrawlerNode']
                    },
                    {
                        'Name': 'instance-state-name',
                        'Values': ['running', 'pending']
                    }
                ]
            )
            
            running_instances = []
            pending_instances = []
            
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    instance_id = instance['InstanceId']
                    state = instance['State']['Name']
                    
                    if state == 'running':
                        # Check if instance is in heartbeat table
                        response = self.heartbeat_table.scan(
                            FilterExpression='crawler_id = :id',
                            ExpressionAttributeValues={':id': instance_id}
                        )
                        if response.get('Items'):
                            running_instances.append(instance_id)
                        else:
                            pending_instances.append(instance_id)
                    elif state == 'pending':
                        pending_instances.append(instance_id)
            
            return running_instances, pending_instances
            
        except Exception as e:
            logging.error(f"[Instance] Error getting running instances: {str(e)}")
            return [], []

    def start_backup_crawler(self):
        """Start a new crawler instance from the AMI and wait for it to be ready"""
        try:
            # First check if we already have enough instances starting up
            running_instances, pending_instances = self.get_running_instances()
            if len(running_instances) >= 2:
                logging.info(f"[Instance] Already have {len(running_instances)} running instances, skipping new instance creation")
                return None
                
            if len(pending_instances) > 0:
                logging.info(f"[Instance] Waiting for {len(pending_instances)} pending instances to start")
                for instance_id in pending_instances:
                    if self.wait_for_instance_running(instance_id):
                        running_instances.append(instance_id)
                        pending_instances.remove(instance_id)
                
                if len(running_instances) >= 2:
                    logging.info("[Instance] Enough instances are now running")
                    return None
            
            # Only start new instance if we still need one
            if len(running_instances) + len(pending_instances) < 2:
                ec2 = boto3.client('ec2', region_name=self.region_name)
                # Get the latest AMI ID for our crawler
                response = ec2.describe_images(
                    Filters=[
                        {
                            'Name': 'name',
                            'Values': ['crawler-ami-*']
                        }
                    ]
                )
                
                if not response['Images']:
                    logging.error("[Instance] No crawler AMI found!")
                    return None
                    
                # Sort by creation date and get the latest
                latest_ami = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)[0]
                ami_id = latest_ami['ImageId']
                
                # Launch instance from AMI
                response = ec2.run_instances(
                    ImageId=ami_id,
                    InstanceType='t2.micro',
                    MinCount=1,
                    MaxCount=1,
                    TagSpecifications=[
                        {
                            'ResourceType': 'instance',
                            'Tags': [
                                {
                                    'Key': 'Name',
                                    'Value': 'CrawlerNode'
                                }
                            ]
                        }
                    ],
                    UserData='''#!/bin/bash
                    cd /home/ubuntu
                    # Activate virtual environment
                    source crawler-venv/bin/activate
                    # Set environment variables
                    export CRAWLER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/353176954707/CrawlQueue"
                    export MASTER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/353176954707/ReportQueue"
                    export INDEXER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/353176954707/IndexQueue"
                    export S3_BUCKET="crawler-indexer-buckets"
                    export DYNAMODB_TABLE="CrawlerHeartbeatTable"
                    export AWS_REGION="us-east-1"
                    export CRAWLER_DELAY="10"
                    # Run the crawler
                    python crawler_object.py
                    '''
                )
                
                instance_id = response['Instances'][0]['InstanceId']
                logging.info(f"[Instance] Starting new crawler node with instance ID: {instance_id}")
                
                # Wait for instance to be fully ready
                if self.wait_for_instance_running(instance_id):
                    logging.info(f"[Instance] New crawler node {instance_id} is ready")
                    return instance_id
                else:
                    logging.error(f"[Instance] Failed to start crawler node {instance_id}")
                    return None
                    
        except Exception as e:
            logging.error(f"[Instance] Failed to start backup crawler: {str(e)}")
            return None

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
                        depth = body.get('depth', 0)
                        max_depth = body.get('max_depth', self.max_depth)
                        status = body.get('status', 'unknown')
                        error = body.get('error', '')
                        domain = body.get('domain', None)
                        
                        if status == 'success':
                            logging.info(f"[{crawler_id}] Successfully crawled: {crawled_url} at depth {depth}")
                            if depth < max_depth:
                                for url in extracted_urls:
                                    self.send_url_to_crawl_queue(url, domain, depth=depth+1, max_depth=max_depth)
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
                                    self.send_url_to_crawl_queue(crawled_url, domain, depth=depth, max_depth=max_depth)
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
            max_depth = item.get('max_depth')
            domain = item.get('domain', None)

            if (now - datetime.fromisoformat(assigned_at)).total_seconds() > self.TIMEOUT_SECONDS:
                if retries < 3:
                    logging.warning(f"[Timeout] Requeuing stale task: {url}")
                    self.send_url_to_crawl_queue(url, domain, depth=depth, max_depth=max_depth)
                    # Delete old item
                    self.task_table.delete_item(Key={'url': url})
                    # Put new item with updated assigned_at and incremented retries
                    self.task_table.put_item(
                        Item={
                            'url': url,
                            'assigned_at': now.isoformat(),
                            'depth': Decimal(str(depth)),
                            'max_depth': Decimal(str(max_depth)),
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

    def get_crawler_pool(self):
        """Get list of all crawler nodes (running and shutdown)"""
        try:
            response = self.heartbeat_table.scan()
            crawlers = {
                'running': [],
                'shutdown': []
            }
            
            for item in response['Items']:
                crawler_id = item['crawler_id']
                status = item.get('status', 'unknown')
                
                if status == 'running':
                    crawlers['running'].append(crawler_id)
                elif status == 'shutdown':
                    crawlers['shutdown'].append(crawler_id)
            
            return crawlers
            
        except Exception as e:
            logging.error(f"[Crawler] Error getting crawler pool: {str(e)}")
            return {'running': [], 'shutdown': []}

    def calculate_desired_crawlers(self, num_messages):
        """Calculate the desired number of crawlers based on queue size"""
        # Base calculation: 1 crawler per 10 URLs, minimum 2
        base_crawlers = max(2, num_messages // 10 + 1)
        
        # Get current crawler pool
        crawler_pool = self.get_crawler_pool()
        num_running = len(crawler_pool['running'])
        num_shutdown = len(crawler_pool['shutdown'])
        
        # If we have too many shutdown crawlers (>4), reduce the number
        if num_shutdown > 4:
            excess_shutdown = num_shutdown - 4
            for crawler_id in crawler_pool['shutdown'][:excess_shutdown]:
                try:
                    ec2 = boto3.client('ec2', region_name=self.region_name)
                    ec2.terminate_instances(InstanceIds=[crawler_id])
                    logging.info(f"[Scaling] Terminated excess shutdown crawler {crawler_id}")
                except Exception as e:
                    logging.error(f"[Scaling] Failed to terminate crawler {crawler_id}: {str(e)}")
        
        return base_crawlers

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
        
        # Monitor crawl queue status and scale crawlers
        response = self.sqs.get_queue_attributes(
            QueueUrl=self.crawl_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        num_messages = int(response['Attributes']['ApproximateNumberOfMessages'])
        logging.info(f"[Monitor] Remaining URLs in queue: {num_messages}")
        
        # Get current crawler pool
        crawler_pool = self.get_crawler_pool()
        num_running = len(crawler_pool['running'])
        num_shutdown = len(crawler_pool['shutdown'])
        
        # Calculate desired number of crawlers
        desired_crawlers = self.calculate_desired_crawlers(num_messages)
        
        if num_running < desired_crawlers:
            # Need more crawlers
            num_to_start = desired_crawlers - num_running
            
            # First try to wake up shutdown crawlers
            num_to_wake = min(num_to_start, num_shutdown)
            for crawler_id in crawler_pool['shutdown'][:num_to_wake]:
                self.wake_up_crawler(crawler_id)
                num_running += 1
            
            # If we still need more, start new ones
            if num_running < desired_crawlers:
                num_to_create = desired_crawlers - num_running
                for _ in range(num_to_create):
                    self.start_backup_crawler()
            
            logging.info(f"[Scaling] Scaled up to {desired_crawlers} crawlers (woke up {num_to_wake}, created {num_to_create})")
            
        elif num_running > desired_crawlers:
            # Too many running crawlers
            excess = num_running - desired_crawlers
            
            # Put excess crawlers into shutdown state, but maintain minimum of 2 running
            num_to_shutdown = min(excess, num_running - 2)
            for crawler_id in crawler_pool['running'][:num_to_shutdown]:
                self.send_shutdown_signal_to_crawler(crawler_id)
            
            logging.info(f"[Scaling] Put {num_to_shutdown} crawlers into shutdown state (now {num_running - num_to_shutdown} running)")
            
        else:
            logging.info(f"[Scaling] No scaling needed. Running crawlers: {num_running}, Desired: {desired_crawlers}")
        
        # Print dashboard at the end of the monitoring cycle
        self.print_dashboard()

    def print_crawl_quality_metrics(self):
        scanned = self.task_table.scan()
        total = len(scanned['Items'])
        crawled = sum(1 for item in scanned['Items'] if item['status'] == 'done')
        failed = sum(1 for item in scanned['Items'] if item['status'] == 'failed')
        skipped = sum(1 for item in scanned['Items'] if item['status'] == 'skipped')
        coverage = (crawled / total) * 100 if total > 0 else 0
        error_rate = (failed / total) * 100 if total > 0 else 0

        return {
            "crawl_coverage": coverage,
            "crawled": crawled,
            "total_tasks": total,
            "crawl_error_rate": error_rate,
            "crawl_failed": failed,
            "politeness": skipped
        }

    def print_crawler_node_status(self):
        response = self.heartbeat_table.scan()
        active = sum(1 for item in response['Items'] if item.get('status') == 'running')
        failed = sum(1 for item in response['Items'] if item.get('status') == 'failed')
        shutdown = sum(1 for item in response['Items'] if item.get('status') == 'shutdown')
        return {
            "active": active,
            "failed": failed,
            "shutdown": shutdown
        }

    def print_crawl_rate(self):
        scanned = self.task_table.scan()
        crawled = sum(1 for item in scanned['Items'] if item['status'] == 'done')
        now = time.time()
        elapsed = now - self.last_crawl_time
        if elapsed > 0:
            rate = (crawled - self.last_crawl_count) / elapsed
            self.last_crawl_count = crawled
            self.last_crawl_time = now
            return {"crawl_rate": rate}
        return {"crawl_rate": 0.0}

    def print_indexing_rate(self):
        response = self.index_status_table.scan()
        indexed = sum(1 for item in response['Items'] if item['status'] == 'index_success')
        now = time.time()
        elapsed = now - self.last_indexed_time
        if elapsed > 0:
            rate = (indexed - self.last_indexed_count) / elapsed
            self.last_indexed_count = indexed
            self.last_indexed_time = now
            return {"indexing_rate": rate}
        return {"indexing_rate": 0.0}

    def print_dashboard(self):
        """Collect all metrics and send them through the queue"""
        # Collect all metrics
        metrics = {}
        metrics.update(self.compute_index_search_error_rates())
        metrics.update(self.print_crawl_quality_metrics())
        metrics.update(self.print_crawler_node_status())
        metrics.update(self.print_crawl_rate())
        metrics.update(self.print_indexing_rate())
        
        try:
            self.sqs.send_message(
                QueueUrl=self.ResponseQueue,
                MessageBody=json.dumps({
                    'feedback': metrics
                })
            )
            logging.info("[Dashboard] Sent metrics to queue")
        except Exception as e:
            logging.error(f"[Dashboard] Failed to send metrics: {str(e)}")

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

    def reboot_all_instances(self):
        """Reboot all running crawler instances"""
        logging.info("[Reboot] Attempting to reboot all crawler instances...")
        try:
            ec2 = boto3.client('ec2', region_name=self.region_name)
            # Find all instances with CrawlerNode tag
            response = ec2.describe_instances(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': ['CrawlerNode']
                    },
                    {
                        'Name': 'instance-state-name',
                        'Values': ['running', 'pending', 'stopping', 'stopped']
                    }
                ]
            )
            
            instance_ids = []
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    instance_ids.append(instance['InstanceId'])
            
            if instance_ids:
                # First try to reboot running instances
                running_instances = []
                for instance_id in instance_ids:
                    try:
                        ec2.reboot_instances(InstanceIds=[instance_id])
                        running_instances.append(instance_id)
                        logging.info(f"[Reboot] Reboot initiated for instance {instance_id}")
                    except Exception as e:
                        logging.error(f"[Reboot] Failed to reboot instance {instance_id}: {str(e)}")
                
                if running_instances:
                    # Wait for instances to reboot
                    logging.info(f"[Reboot] Waiting for {len(running_instances)} instances to reboot...")
                    waiter = ec2.get_waiter('instance_running')
                    waiter.wait(InstanceIds=running_instances)
                    logging.info("[Reboot] All instances have been rebooted successfully")
                else:
                    logging.warning("[Reboot] No instances were rebooted")
            else:
                logging.info("[Reboot] No instances found to reboot")
                
        except Exception as e:
            logging.error(f"[Reboot] Error during instance reboot process: {str(e)}")
            raise

    def reset_sqs_queues(self):
        """Reset all SQS queues by purging and setting attributes"""
        logging.info("[Reset] Resetting SQS queues...")
        queue_urls = {
            'crawl': self.crawl_queue_url,
            'report': self.report_queue_url,
            'request': self.request_queue_url,
            'response': self.ResponseQueue,
            'search': self.search_queue_url,
            'feedback': self.index_feedback_queue_url,
            'dead_letter': self.dead_letter_queue_url
        }
        
        for queue_name, queue_url in queue_urls.items():
            try:
                # First purge the queue
                self.sqs.purge_queue(QueueUrl=queue_url)
                logging.info(f"[Reset] Purged {queue_name} queue")
                
                # Reset queue attributes
                attributes = {
                    'VisibilityTimeout': '120',  # 2 minutes
                    'MessageRetentionPeriod': '345600',  # 4 days
                    'DelaySeconds': '0',
                    'ReceiveMessageWaitTimeSeconds': '5'  # Long polling
                }
                
                # Add dead-letter queue configuration for main queues
                if queue_name in ['crawl', 'report', 'request', 'search']:
                    attributes['RedrivePolicy'] = json.dumps({
                        'deadLetterTargetArn': self.dead_letter_queue_url,
                        'maxReceiveCount': '3'
                    })
                
                self.sqs.set_queue_attributes(
                    QueueUrl=queue_url,
                    Attributes=attributes
                )
                logging.info(f"[Reset] Reset attributes for {queue_name} queue")
                
            except Exception as e:
                logging.error(f"[Reset] Failed to reset {queue_name} queue: {str(e)}")
                raise

    def reset_dynamodb_tables(self):
        """Reset all DynamoDB tables by clearing and updating capacity"""
        logging.info("[Reset] Resetting DynamoDB tables...")
        tables = {
            'heartbeat': {
                'table': self.heartbeat_table,
                'key': 'crawler_id',
                'capacity': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            'indexer_heartbeat': {
                'table': self.indexer_heartbeat_table,
                'key': 'indexer_id',
                'capacity': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            'task': {
                'table': self.task_table,
                'key': 'url',
                'capacity': {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
            },
            'index_status': {
                'table': self.index_status_table,
                'key': 'url',
                'capacity': {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
            },
            'blocked': {
                'table': self.blocked_table,
                'key': 'url',
                'capacity': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }
        }
        
        for table_name, table_info in tables.items():
            try:
                # Clear the table
                response = table_info['table'].scan()
                items = response.get('Items', [])
                
                with table_info['table'].batch_writer() as batch:
                    for item in items:
                        batch.delete_item(Key={table_info['key']: item[table_info['key']]})
                
                logging.info(f"[Reset] Cleared {table_name} table")
                
                # Update table capacity
                table_info['table'].update(
                    ProvisionedThroughput=table_info['capacity']
                )
                logging.info(f"[Reset] Updated capacity for {table_name} table")
                
            except Exception as e:
                logging.error(f"[Reset] Failed to reset {table_name} table: {str(e)}")
                raise

    def reset_system_state(self):
        """Reset all system state and clean up resources"""
        logging.info("[Master] Resetting system state...")
        try:
            # Reset AWS services first
            self.reset_sqs_queues()
            self.reset_dynamodb_tables()
            
            # Reset counters and state
            self.last_crawl_count = 0
            self.last_crawl_time = time.time()
            self.last_indexed_count = 0
            self.last_indexed_time = time.time()
            self.running = False
            
            # Initialize crawler pool (2 crawlers, one in shutdown)
            if not self.initialize_crawler_pool():
                raise Exception("Failed to initialize crawler pool")
            
            logging.info("[Reset] System state reset complete")
            
        except Exception as e:
            logging.error(f"[Reset] Error during system reset: {str(e)}")
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
        max_depth=5)

    # Reset system state and initialize crawler pool
    master.reset_system_state()
    
    # Wait a moment to ensure crawler pool is ready
    time.sleep(10)
    
    logging.info("[Master] Starting comprehensive monitoring...")
    master.monitor_crawl_queue()
    ####
