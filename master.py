import boto3
import json
import time
import logging
from datetime import datetime, timezone
from decimal import Decimal
import uuid

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
         # Add crawler control parameters
         self.MAX_CRAWLERS = 5  # Maximum number of crawlers allowed
         self.MIN_CRAWLERS = 2  # Minimum number of crawlers
         self.MESSAGES_PER_CRAWLER = 20  # Number of messages per crawler for scaling
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
        """Wake up a specific crawler by sending a wake-up signal"""
        wake_message = {
            "status": "wake_up",
            "crawler_id": crawler_id
        }
        self.sqs.send_message(
            QueueUrl=self.crawl_queue_url,
            MessageBody=json.dumps(wake_message, cls=DecimalEncoder)
        )
        logging.info(f"[Master] Sent wake-up signal to {crawler_id}")

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
                    max_depth = min(body.get('max_depth', 0), self.max_depth)
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
                'status': 'shutdown',
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
        """Start a new crawler instance from the AMI"""
        logging.info("[Backup] Starting new crawler instance creation process")
        ec2 = boto3.client('ec2', region_name=self.region_name)
        try:
            # First, find an existing crawler instance to copy settings from
            logging.info("[Backup] Looking for existing crawler instance to copy settings")
            try:
                existing_crawler = None
                response = ec2.describe_instances(
                    Filters=[
                        {
                            'Name': 'tag:Name',
                            'Values': ['CrawlerNode']
                        },
                        {
                            'Name': 'instance-state-name',
                            'Values': ['running', 'pending', 'stopped']
                        }
                    ]
                )
                
                for reservation in response['Reservations']:
                    for instance in reservation['Instances']:
                        # Found an existing crawler to copy settings from
                        existing_crawler = instance
                        logging.info(f"[Backup] Found existing crawler instance {instance['InstanceId']} to copy settings from")
                        break
                    if existing_crawler:
                        break
            except Exception as e:
                logging.warning(f"[Backup] Error looking for existing crawler: {e}")
                existing_crawler = None
            
            # Get the latest AMI ID for our crawler
            logging.info("[Backup] Searching for crawler AMI")
            response = ec2.describe_images(
                Filters=[
                    {
                        'Name': 'name',
                        'Values': ['crawler-ami-*']
                    }
                ]
            )
            
            if not response['Images']:
                logging.error("[Backup] No crawler AMI found!")
                return
            
            # Sort by creation date and get the latest
            latest_ami = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)[0]
            ami_id = latest_ami['ImageId']
            logging.info(f"[Backup] Using AMI: {ami_id} ({latest_ami['Name']})")
            
            # Generate a unique ID for this crawler
            crawler_id = f"crawler-{str(uuid.uuid4())[:8]}"
            
            # Prepare instance launch parameters, copying from existing instance if available
            launch_params = {
                'ImageId': ami_id,
                'InstanceType': 't2.micro',
                'MinCount': 1,
                'MaxCount': 1,
                'TagSpecifications': [
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {
                                'Key': 'Name',
                                'Value': 'CrawlerNode'
                            },
                            {
                                'Key': 'CrawlerId',
                                'Value': crawler_id
                            }
                        ]
                    }
                ]
            }
            
            # If we found an existing crawler, copy its settings
            if existing_crawler:
                # Copy security groups
                if 'SecurityGroups' in existing_crawler:
                    sg_ids = [sg['GroupId'] for sg in existing_crawler['SecurityGroups']]
                    launch_params['SecurityGroupIds'] = sg_ids
                    logging.info(f"[Backup] Using security groups: {sg_ids}")
                
                # Copy subnet
                if 'SubnetId' in existing_crawler:
                    launch_params['SubnetId'] = existing_crawler['SubnetId']
                    logging.info(f"[Backup] Using subnet: {existing_crawler['SubnetId']}")
                
                # Copy IAM role if it exists
                if 'IamInstanceProfile' in existing_crawler and existing_crawler['IamInstanceProfile']:
                    role_arn = existing_crawler['IamInstanceProfile']['Arn']
                    role_name = role_arn.split('/')[-1]
                    launch_params['IamInstanceProfile'] = {'Name': role_name}
                    logging.info(f"[Backup] Using IAM role: {role_name}")
                else:
                    logging.warning("[Backup] No IAM role found on existing instance. AWS service access may fail.")
                    # Try to find a suitable role
                    try:
                        iam = boto3.client('iam', region_name=self.region_name)
                        roles = iam.list_roles()
                        for role in roles['Roles']:
                            role_name = role['RoleName']
                            if 'EC2' in role_name and ('Crawler' in role_name or 'DynamoDB' in role_name):
                                launch_params['IamInstanceProfile'] = {'Name': role_name}
                                logging.info(f"[Backup] Found likely IAM role: {role_name}")
                                break
                    except Exception as e:
                        logging.error(f"[Backup] Error searching for IAM roles: {e}")
            else:
                logging.warning("[Backup] No existing crawler found to copy settings from. Using defaults.")
                # Set default security group (default VPC security group)
                try:
                    # Use the specific security group and subnet values provided
                    launch_params['SecurityGroupIds'] = ['sg-0790ac0a83cfc4b55']
                    logging.info(f"[Backup] Using specific security group: sg-0790ac0a83cfc4b55")
                    
                    # Set the specific subnet
                    launch_params['SubnetId'] = 'subnet-0abdea8c046e3b4ca'
                    logging.info(f"[Backup] Using specific subnet: subnet-0abdea8c046e3b4ca")
                    
                    # Note the VPC for reference
                    logging.info(f"[Backup] VPC: vpc-02ca01805beb89dde")
                except Exception as e:
                    logging.error(f"[Backup] Error setting up specific network: {e}")
                
                # Try to find or create a suitable IAM role
                try:
                    iam = boto3.client('iam', region_name=self.region_name)
                    # Look for existing roles first
                    roles = iam.list_roles()
                    crawler_role = None
                    for role in roles['Roles']:
                        if 'CrawlerRole' in role['RoleName'] or ('EC2' in role['RoleName'] and 'DynamoDB' in role['RoleName']):
                            crawler_role = role['RoleName']
                            break
                    
                    if crawler_role:
                        launch_params['IamInstanceProfile'] = {'Name': crawler_role}
                        logging.info(f"[Backup] Using existing IAM role: {crawler_role}")
                    else:
                        logging.error("[Backup] No suitable IAM role found. Crawler WILL NOT have AWS service access!")
                        logging.error("[Backup] Create an IAM role with DynamoDB, SQS and S3 access and name it 'CrawlerRole'")
                except Exception as e:
                    logging.error(f"[Backup] Error handling IAM roles: {e}")
            
            # Enhanced UserData script with better error handling and diagnostics
            user_data_script = f"""#!/bin/bash
# Navigate to the home directory
cd ~
# Log startup for debugging
echo "===== CRAWLER STARTUP: {crawler_id} =====" > startup.log
echo "Starting crawler initialization: $(date)" >> startup.log
echo "Instance metadata:" >> startup.log
curl -s http://169.254.169.254/latest/meta-data/instance-id >> startup.log
echo "" >> startup.log

# Check for network connectivity
echo "Testing internet connectivity..." >> startup.log
if ping -c 3 amazon.com >> startup.log 2>&1; then
    echo "Internet connectivity: OK" >> startup.log
else
    echo "WARNING: Internet connectivity check failed" >> startup.log
fi

# Check for AWS API connectivity
echo "Testing AWS connectivity..." >> startup.log
if curl -s https://dynamodb.us-east-1.amazonaws.com >> /dev/null 2>&1; then
    echo "AWS API connectivity: OK" >> startup.log
else
    echo "WARNING: AWS API connectivity check failed" >> startup.log
fi

# Check for IAM role
echo "Checking for IAM role..." >> startup.log
curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/ >> startup.log 2>&1

# Activate virtual environment if it exists in current directory
if [ -d "crawler-venv" ]; then
    source crawler-venv/bin/activate
    echo "Activated virtual environment at ~/crawler-venv" >> startup.log
elif [ -d "/home/ubuntu/crawler-venv" ]; then
    source /home/ubuntu/crawler-venv/bin/activate
    echo "Activated virtual environment at /home/ubuntu/crawler-venv" >> startup.log
else
    echo "WARNING: crawler-venv not found in expected locations" >> startup.log
    if command -v python3 -m venv &> /dev/null; then
        echo "Creating new virtual environment" >> startup.log
        python3 -m venv crawler-venv
        source crawler-venv/bin/activate
        pip install boto3 requests beautifulsoup4 urllib3
    else
        echo "CRITICAL: Cannot create virtual environment" >> startup.log
    fi
fi

# Verify Python and boto3 are available
echo "Python version:" >> startup.log
python --version >> startup.log 2>&1
echo "Installed packages:" >> startup.log
pip list >> startup.log 2>&1

# Set environment variables with unique crawler ID
export CRAWLER_ID="{crawler_id}"
export CRAWLER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/353176954707/CrawlQueue"
export MASTER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/353176954707/ReportQueue"
export INDEXER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/353176954707/IndexQueue"
export S3_BUCKET="crawler-indexer-buckets"
export DYNAMODB_TABLE="CrawlerHeartbeatTable"
export AWS_REGION="us-east-1"
export CRAWLER_DELAY="10"

# Find crawler_object.py
echo "Searching for crawler_object.py..." >> startup.log
if [ -f "crawler_object.py" ]; then
    CRAWLER_SCRIPT="./crawler_object.py"
    echo "Found crawler_object.py in current directory" >> startup.log
elif [ -f "/home/ubuntu/crawler_object.py" ]; then
    CRAWLER_SCRIPT="/home/ubuntu/crawler_object.py"
    echo "Found crawler_object.py in /home/ubuntu" >> startup.log
else
    echo "ERROR: crawler_object.py not found in expected locations" >> startup.log
    echo "Searching for it..." >> startup.log
    find ~ -name "crawler_object.py" >> startup.log 2>&1
    exit 1
fi

# Verify crawler.py exists too
if [ -f "crawler.py" ]; then
    echo "Found crawler.py in current directory" >> startup.log
elif [ -f "/home/ubuntu/crawler.py" ]; then
    echo "Found crawler.py in /home/ubuntu" >> startup.log
else
    echo "ERROR: crawler.py not found in expected locations" >> startup.log
    find ~ -name "crawler.py" >> startup.log 2>&1
    exit 1
fi

# Test AWS credentials and DynamoDB access before starting crawler
echo "Testing AWS credentials and DynamoDB access..." >> startup.log
echo "Skipping DynamoDB test - continuing with crawler startup" >> startup.log

# Run the crawler
echo "Starting crawler with command: python $CRAWLER_SCRIPT" >> startup.log
python $CRAWLER_SCRIPT > ~/crawler_{crawler_id}.log 2>&1 &
CRAWLER_PID=$!
echo "Crawler PID: $CRAWLER_PID" >> startup.log

# Add a hook to ensure crawler is running
sleep 10
if ps -p $CRAWLER_PID > /dev/null; then
    echo "Crawler is running successfully (PID: $CRAWLER_PID)" >> startup.log
else
    echo "WARNING: Crawler process not found after 10 seconds" >> startup.log
    echo "Checking for other Python processes:" >> startup.log
    ps aux | grep python >> startup.log
    
    # Try to diagnose and fix common issues
    echo "Trying to restart crawler..." >> startup.log
    python $CRAWLER_SCRIPT > ~/crawler_{crawler_id}_retry.log 2>&1 &
fi

# Add a watchdog to restart crawler if it fails
echo "Setting up watchdog to monitor crawler process" >> startup.log
(
while true; do
    sleep 60
    if ! ps -p $CRAWLER_PID > /dev/null; then
        echo "$(date) - Crawler process died, restarting..." >> ~/watchdog.log
        python $CRAWLER_SCRIPT > ~/crawler_{crawler_id}_restarted.log 2>&1 &
        CRAWLER_PID=$!
        echo "New crawler PID: $CRAWLER_PID" >> ~/watchdog.log
    fi
done
) &
"""
            
            # Add the UserData script to the launch parameters
            launch_params['UserData'] = user_data_script
            
            # Launch the instance with all the configured parameters
            logging.info(f"[Backup] Launching new crawler instance with parameters: {launch_params}")
            response = ec2.run_instances(**launch_params)
            
            instance_id = response['Instances'][0]['InstanceId']
            logging.info(f"[Backup] Started crawler node {crawler_id} with instance ID: {instance_id}")
            
            # Wait for the instance to be running
            try:
                logging.info(f"[Backup] Waiting for instance {instance_id} to reach running state")
                waiter = ec2.get_waiter('instance_running')
                waiter.wait(InstanceIds=[instance_id])
                logging.info(f"[Backup] Instance {instance_id} is now running")
                
                # Get the public IP for SSH access
                instance_info = ec2.describe_instances(InstanceIds=[instance_id])
                if 'PublicIpAddress' in instance_info['Reservations'][0]['Instances'][0]:
                    public_ip = instance_info['Reservations'][0]['Instances'][0]['PublicIpAddress']
                    logging.info(f"[Backup] Instance public IP: {public_ip} (for SSH access)")
            except Exception as e:
                logging.error(f"[Backup] Error waiting for instance: {e}")
            
            logging.info(f"[Backup] Crawler node {crawler_id} (instance {instance_id}) startup complete.")
            return instance_id
            
        except Exception as e:
            logging.error(f"[Recovery] Failed to start backup crawler: {e}", exc_info=True)
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
                        depth = body.get('depth') or 0
                        max_depth = body.get('max_depth', self.max_depth)  # Get max_depth from message or use default
                        status = body.get('status', 'unknown')
                        error = body.get('error', '')
                        domain = body.get('domain', None)
                        
                        if status == 'success':
                            logging.info(f"[{crawler_id}] Successfully crawled: {crawled_url} at depth {depth}")
                            if depth < max_depth:  # Use the max_depth from message
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
                            # Update crawler status in heartbeat table
                            self.heartbeat_table.update_item(
                                Key={'crawler_id': crawler_id},
                                UpdateExpression="SET #s = :s",
                                ExpressionAttributeNames={"#s": "status"},
                                ExpressionAttributeValues={":s": "shutdown"}
                            )
                        else:
                            logging.warning(f"[{crawler_id}] Failed crawling: {crawled_url} Reason: {error}")
                            try:
                                # Add check to ensure crawled_url is not None or empty
                                if not crawled_url:
                                    logging.error(f"[{crawler_id}] Cannot process failed crawl: URL is None or empty")
                                    continue
                                
                                response = self.task_table.get_item(Key={'url': crawled_url})
                                retries = response.get('Item', {}).get('retries', 0)
                                if retries < 3:
                                    self.task_table.update_item(
                                        Key={'url': crawled_url},
                                        UpdateExpression="SET retries = :r, assigned_at = :t",
                                        ExpressionAttributeValues={
                                            ":r": Decimal(str(retries + 1)),
                                            ":t": datetime.now(timezone.utc).isoformat()
                                        }
                                    )
                                    self.send_url_to_crawl_queue(crawled_url, domain, depth=depth, max_depth=max_depth)
                                   
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
            max_depth = item.get('max_depth', self.max_depth)
            domain = item.get('domain', None)

            if (now - datetime.fromisoformat(assigned_at)).total_seconds() > self.TIMEOUT_SECONDS:
                if retries < 3:
                    logging.warning(f"[Timeout] Requeuing stale task: {url}")
                    self.send_url_to_crawl_queue(url, domain, depth=depth, max_depth= max_depth)
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
        
        # Monitor crawl queue status with improved scaling logic
        response = self.sqs.get_queue_attributes(
            QueueUrl=self.crawl_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        num_messages = int(response['Attributes']['ApproximateNumberOfMessages'])
        logging.info(f"[Monitor] Remaining URLs in queue: {num_messages}")
        
        # If queue is empty, perform completion tasks
        if num_messages == 0:
            logging.info("[Master] CrawlQueue is empty. Crawling seems complete!")
        else:
            # Improved scaling logic with better limits
            active_crawlers = self.count_active_crawlers()
            # Calculate desired crawlers based on queue size, but respect MAX_CRAWLERS
            desired_crawlers = min(
                self.MAX_CRAWLERS,
                max(
                    self.MIN_CRAWLERS,
                    (num_messages // self.MESSAGES_PER_CRAWLER) + 1
                )
            )

            if active_crawlers < desired_crawlers:
                # Only start new crawlers if we're below MAX_CRAWLERS
                if active_crawlers < self.MAX_CRAWLERS:
                    num_to_start = min(desired_crawlers - active_crawlers, self.MAX_CRAWLERS - active_crawlers)
                    self.ensure_crawlers(num_to_start)
                    logging.info(f"[Scaling] Starting {num_to_start} new crawlers (active: {active_crawlers} -> {active_crawlers + num_to_start})")
                else:
                    logging.info(f"[Scaling] At maximum crawler limit ({self.MAX_CRAWLERS})")
            elif active_crawlers > desired_crawlers:
                # Only shut down excess crawlers if we're above MIN_CRAWLERS
                if active_crawlers > self.MIN_CRAWLERS:
                    num_to_shutdown = min(active_crawlers - desired_crawlers, active_crawlers - self.MIN_CRAWLERS)
                    response = self.heartbeat_table.scan()
                    running_crawlers = [item['crawler_id'] for item in response['Items'] if item.get('status') == 'running']
                    for crawler_id in running_crawlers[:num_to_shutdown]:
                        self.send_shutdown_signal_to_crawler(crawler_id)
                    logging.info(f"[Scaling] Shutting down {num_to_shutdown} crawlers (active: {active_crawlers} -> {active_crawlers - num_to_shutdown})")
                else:
                    logging.info(f"[Scaling] At minimum crawler limit ({self.MIN_CRAWLERS})")
            else:
                logging.info(f"[Scaling] No scaling needed. Active crawlers: {active_crawlers}, Desired: {desired_crawlers}")

    def count_active_crawlers(self):
        """Return the number of crawlers with status 'running'."""
        response = self.heartbeat_table.scan()
        return sum(1 for item in response['Items'] if item.get('status') == 'running')

    def send_shutdown_signal_to_crawler(self, crawler_id):
        # Check if crawler is already in shutdown state
        response = self.heartbeat_table.get_item(Key={'crawler_id': crawler_id})
        if 'Item' in response and response['Item'].get('status') == 'shutdown':
            logging.info(f"[Master] Crawler {crawler_id} is already in shutdown state.")
            return

        shutdown_message = {
            'status': 'shutdown',
            'crawler_id': crawler_id
        }
        self.sqs.send_message(
            QueueUrl=self.crawl_queue_url,
            MessageBody=json.dumps(shutdown_message, cls=DecimalEncoder)
        )
        logging.info(f"[Master] Sent shutdown signal to {crawler_id}")

    def ensure_crawlers(self, num_to_start):
        """Wake up shutdown crawlers if available, otherwise start new ones."""
        if num_to_start <= 0:
            return

        # Check if we would exceed MAX_CRAWLERS
        current_crawlers = self.count_active_crawlers()
        if current_crawlers + num_to_start > self.MAX_CRAWLERS:
            num_to_start = self.MAX_CRAWLERS - current_crawlers
            if num_to_start <= 0:
                logging.info(f"[Scaling] Cannot start more crawlers: at maximum limit ({self.MAX_CRAWLERS})")
                return

        response = self.heartbeat_table.scan()
        shutdown_crawlers = [item['crawler_id'] for item in response['Items'] if item.get('status') == 'shutdown']
        num_woken = 0
        
        # First try to wake up shutdown crawlers
        for crawler_id in shutdown_crawlers[:num_to_start]:
            self.wake_up_crawler(crawler_id)
            num_woken += 1
            
        # Only start new crawlers if we still need more
        num_to_start_new = num_to_start - num_woken
        if num_to_start_new > 0:
            logging.info(f"[Scaling] Waking up {num_woken} shutdown crawlers, starting {num_to_start_new} new crawlers")
            for _ in range(num_to_start_new):
                self.start_backup_crawler()
        else:
            logging.info(f"[Scaling] Woke up {num_woken} shutdown crawlers")

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
            logging.info(f"[Dashboard] Metrics: {metrics}")

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

    def reset_system_state(self):
        """Reset all system state and clean up resources"""
        logging.info("[Master] Resetting system state...")
        try:
            # First, terminate all running instances to stop any active processing
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
            
            instance_ids = []
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    instance_ids.append(instance['InstanceId'])
            
            if instance_ids:
                ec2.terminate_instances(InstanceIds=instance_ids)
                logging.info(f"[Reset] Terminated {len(instance_ids)} running instances")
                # Wait for instances to terminate
                time.sleep(10)

            # Purge all queues first to prevent new processing
            queue_urls = [
                self.crawl_queue_url,
                self.report_queue_url,
                self.request_queue_url,
                self.ResponseQueue,
                self.search_queue_url,
                self.index_feedback_queue_url,
                self.dead_letter_queue_url
            ]
            
            for queue_url in queue_urls:
                try:
                    self.sqs.purge_queue(QueueUrl=queue_url)
                    logging.info(f"[Reset] Purged queue: {queue_url}")
                except Exception as e:
                    logging.error(f"[Reset] Failed to purge queue {queue_url}: {str(e)}")

            # Wait for queues to be purged
            time.sleep(5)

            # Now reset all DynamoDB tables
            tables = {
                'heartbeat': self.heartbeat_table,
                'indexer_heartbeat': self.indexer_heartbeat_table,
                'task': self.task_table,
                'index_status': self.index_status_table,
                'blocked': self.blocked_table
            }

            for table_name, table in tables.items():
                try:
                    # Get all items
                    response = table.scan()
                    items = response.get('Items', [])
                    
                    # Delete items in batches of 25 (DynamoDB batch write limit)
                    with table.batch_writer() as batch:
                        for item in items:
                            if table_name == 'heartbeat':
                                batch.delete_item(Key={'crawler_id': item['crawler_id']})
                            elif table_name == 'indexer_heartbeat':
                                batch.delete_item(Key={'indexer_id': item['indexer_id']})
                            elif table_name == 'task':
                                batch.delete_item(Key={'url': item['url']})
                            elif table_name == 'index_status':
                                batch.delete_item(Key={'url': item['url']})
                            elif table_name == 'blocked':
                                batch.delete_item(Key={'url': item['url']})
                    
                    logging.info(f"[Reset] Cleared {table_name} table")
                except Exception as e:
                    logging.error(f"[Reset] Failed to clear {table_name} table: {str(e)}")

            # Reset all counters and state
            self.last_crawl_count = 0
            self.last_crawl_time = time.time()
            self.last_indexed_count = 0
            self.last_indexed_time = time.time()
            self.running = False

            # First verify cleanup is complete
            self.verify_cleanup()
            logging.info("[Reset] Cleanup verification complete")
            
            # Now start new crawlers after cleanup is verified
            for _ in range(self.MIN_CRAWLERS):
                self.start_backup_crawler()
            logging.info(f"[Reset] Started {self.MIN_CRAWLERS} initial crawlers")

            # Wait for crawlers to start
            if not self.wait_for_crawlers_to_start(expected_count=self.MIN_CRAWLERS, timeout=300):
                logging.error("[Reset] Failed to start initial crawlers")
                raise Exception("Failed to start initial crawlers")
            
            logging.info("[Master] System state reset complete")
        except Exception as e:
            logging.error(f"[Reset] Error during system reset: {str(e)}")
            raise
    
    def verify_cleanup(self):
        """Verify that all resources are properly cleaned up"""
        logging.info("[Verify] Checking cleanup status...")
        
        # Check SQS queues
        queue_urls = [
            self.crawl_queue_url,
            self.report_queue_url,
            self.request_queue_url,
            self.ResponseQueue,
            self.search_queue_url,
            self.index_feedback_queue_url,
            self.dead_letter_queue_url
        ]
        
        for queue_url in queue_urls:
            try:
                response = self.sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
                )
                visible = int(response['Attributes']['ApproximateNumberOfMessages'])
                not_visible = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])
                if visible > 0 or not_visible > 0:
                    logging.warning(f"[Verify] Queue {queue_url} still has messages: {visible} visible, {not_visible} not visible")
                    # Try to purge again
                    self.sqs.purge_queue(QueueUrl=queue_url)
            except Exception as e:
                logging.error(f"[Verify] Failed to check queue {queue_url}: {str(e)}")

        # Check DynamoDB tables
        tables = {
            'heartbeat': self.heartbeat_table,
            'indexer_heartbeat': self.indexer_heartbeat_table,
            'task': self.task_table,
            'index_status': self.index_status_table,
            'blocked': self.blocked_table
        }

        for table_name, table in tables.items():
            try:
                response = table.scan()
                if response.get('Items'):
                    logging.warning(f"[Verify] Table {table_name} still has {len(response['Items'])} items")
                    # Try to clear again
                    with table.batch_writer() as batch:
                        for item in response['Items']:
                            if table_name == 'heartbeat':
                                batch.delete_item(Key={'crawler_id': item['crawler_id']})
                            elif table_name == 'indexer_heartbeat':
                                batch.delete_item(Key={'indexer_id': item['indexer_id']})
                            elif table_name == 'task':
                                batch.delete_item(Key={'url': item['url']})
                            elif table_name == 'index_status':
                                batch.delete_item(Key={'url': item['url']})
                            elif table_name == 'blocked':
                                batch.delete_item(Key={'url': item['url']})
            except Exception as e:
                logging.error(f"[Verify] Failed to check table {table_name}: {str(e)}")

        # Only check for unexpected instances, not the ones we just created
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
                        'Values': ['running', 'pending', 'stopping', 'stopped']
                    }
                ]
            )
            
            # Get the expected number of instances (MIN_CRAWLERS)
            expected_instances = self.MIN_CRAWLERS
            actual_instances = sum(1 for reservation in response['Reservations'] 
                                 for instance in reservation['Instances'])
            
            if actual_instances > expected_instances:
                logging.warning(f"[Verify] Found {actual_instances} instances (expected {expected_instances})")
                # Only terminate if we have more than expected
                instance_ids = []
                for reservation in response['Reservations']:
                    for instance in reservation['Instances']:
                        instance_ids.append(instance['InstanceId'])
                if instance_ids:
                    # Sort by launch time and keep the most recent ones
                    instance_details = [(id, ec2.describe_instances(InstanceIds=[id])['Reservations'][0]['Instances'][0]['LaunchTime']) 
                                     for id in instance_ids]
                    instance_details.sort(key=lambda x: x[1], reverse=True)
                    # Keep the most recent instances (up to MIN_CRAWLERS)
                    instances_to_terminate = [id for id, _ in instance_details[expected_instances:]]
                    if instances_to_terminate:
                        ec2.terminate_instances(InstanceIds=instances_to_terminate)
                        logging.info(f"[Verify] Terminated {len(instances_to_terminate)} excess instances")
        except Exception as e:
            logging.error(f"[Verify] Failed to check EC2 instances: {str(e)}")


    def wait_for_crawlers_to_start(self, expected_count=3, timeout=300, shutdown_after_start=False):
        """Wait for crawlers to be in running state and optionally shutdown one"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            # First check EC2 instances
            ec2 = boto3.client('ec2', region_name=self.region_name)
            try:
                response = ec2.describe_instances(
                    Filters=[
                        {
                            'Name': 'tag:Name',
                            'Values': ['CrawlerNode']
                        },
                        {
                            'Name': 'instance-state-name',
                            'Values': ['running', 'pending']  # Also check pending instances
                        }
                    ]
                )
                
                running_instances = []
                pending_instances = []
                for reservation in response['Reservations']:
                    for instance in reservation['Instances']:
                        if instance['State']['Name'] == 'running':
                            running_instances.append(instance['InstanceId'])
                        elif instance['State']['Name'] == 'pending':
                            pending_instances.append(instance['InstanceId'])
                
                logging.info(f"[Startup] EC2 Status - Running: {len(running_instances)}, Pending: {len(pending_instances)}")
                if pending_instances:
                    logging.info(f"[Startup] Pending instances: {pending_instances}")
            except Exception as e:
                logging.error(f"[Startup] Error checking EC2 instances: {e}")
                running_instances = []
                pending_instances = []
            
            # Then check heartbeat table
            try:
                heartbeat_response = self.heartbeat_table.scan()
                registered_crawlers = [item for item in heartbeat_response['Items'] 
                                     if item.get('status') == 'running']
                other_status_crawlers = [item for item in heartbeat_response['Items'] 
                                       if item.get('status') != 'running']
                
                running_count = len(registered_crawlers)
                logging.info(f"[Startup] Heartbeat Status - Running: {running_count}, Other status: {len(other_status_crawlers)}")
                if other_status_crawlers:
                    logging.info(f"[Startup] Other status crawlers: {[(item['crawler_id'], item.get('status')) for item in other_status_crawlers]}")
            except Exception as e:
                logging.error(f"[Startup] Error checking heartbeat table: {e}")
                running_count = 0
            
            if running_count >= expected_count and len(running_instances) >= expected_count:
                logging.info(f"[Startup] All {expected_count} crawlers are now running and registered")
                
                if shutdown_after_start:
                    if registered_crawlers:
                        # Shutdown the first running crawler
                        crawler_to_shutdown = registered_crawlers[0]
                        crawler_id = crawler_to_shutdown['crawler_id']
                        logging.info(f"[Startup] Shutting down crawler {crawler_id}")
                        
                        # Send shutdown signal and update status
                        self.send_shutdown_signal_to_crawler(crawler_id)
                        self.heartbeat_table.update_item(
                            Key={'crawler_id': crawler_id},
                            UpdateExpression="SET #s = :s",
                            ExpressionAttributeNames={"#s": "status"},
                            ExpressionAttributeValues={":s": "shutdown"}
                        )
                
                return True
                
            elapsed = time.time() - start_time
            logging.info(f"[Startup] Waiting for crawlers to start and register... (Elapsed: {int(elapsed)}s, EC2: {len(running_instances)}, Registered: {running_count}/{expected_count})")
            time.sleep(10)
            
        logging.error(f"[Startup] Timeout waiting for crawlers to start after {timeout}s. Final status - EC2: {len(running_instances)}, Registered: {running_count}/{expected_count}")
        return False

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
        index_feedback_queue_url='https://sqs.us-east-1.amazonaws.com/353176954707/FeedbackQueue',
        request_queue_url='https://sqs.us-east-1.amazonaws.com/353176954707/RequestQueue',
        ResponseQueue='https://sqs.us-east-1.amazonaws.com/353176954707/ResponseQueue',
        search_queue_url='https://sqs.us-east-1.amazonaws.com/353176954707/SearchQueue',
        index_status_table_name='IndexerTaskAssignments',
        indexer_heartbeat_table_name='IndexerHeartbeatTable',
        max_depth=2)

    # Reset system (starts 2 crawlers)
    master.reset_system_state()

    # Start one more crawler
    master.start_backup_crawler()

    # Wait for all 3 to be running
    if master.wait_for_crawlers_to_start(expected_count=3, timeout=300):
        # Get the heartbeat table to find crawler IDs
        heartbeat_response = master.heartbeat_table.scan()
        running_crawlers = [item for item in heartbeat_response['Items'] 
                          if item.get('status') == 'running']
        
        if running_crawlers:
            # Shutdown the first running crawler
            crawler_to_shutdown = running_crawlers[0]
            crawler_id = crawler_to_shutdown['crawler_id']
            logging.info(f"[Startup] Shutting down crawler {crawler_id}")
            
            # Send shutdown signal and update status
            master.send_shutdown_signal_to_crawler(crawler_id)
            
    else:
        logging.error("[Startup] Failed to start all crawlers")

    # Then start monitoring
    logging.info("[Master] Starting comprehensive monitoring...")
    master.monitor_crawl_queue()
