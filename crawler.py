import time
import json
import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging
from datetime import datetime, timezone
import urllib.robotparser
from urllib.parse import urlparse
import signal
import hashlib

class Crawler:
    def __init__(self, 
                 crawler_id,
                 crawler_queue_url, 
                 master_queue_url, 
                 indexer_queue_url, 
                 s3_bucket, 
                 dynamodb_table,
                 region='us-east-1',
                 delay=10  # Politeness logic
                 ):

        # Initialization of crawler node.
        self.crawler_id = crawler_id
        self.crawler_queue_url = crawler_queue_url
        self.master_queue_url = master_queue_url
        self.indexer_queue_url = indexer_queue_url
        self.s3_bucket = s3_bucket
        self.dynamodb_table = dynamodb_table
        self.region = region
        self.delay = delay
        self.is_shutdown = False  # Add shutdown state

        # Create AWS clients
        self.sqs = boto3.client('sqs', region_name=self.region)  # AWS SQS client
        self.s3 = boto3.client('s3', region_name=self.region)    # AWS S3 client
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
        self.heartbeat_table = self.dynamodb.Table(self.dynamodb_table)
        # Configure logging to show time, log level, and message
        logging.basicConfig(filename=f'crawler_{self.crawler_id}.log', filemode='w', level=logging.INFO, 
                            format='%(asctime)s [%(levelname)s] %(message)s')
        
        # Handle graceful shutdown
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def heartbeat(self):
        try:
            self.heartbeat_table.put_item(
                Item={
                    'crawler_id': self.crawler_id,
                    'status': 'running',
                    'last_heartbeat': datetime.now(timezone.utc).isoformat()
                }
            )
            logging.info(f"Heartbeat sent for crawler {self.crawler_id} at {datetime.now()}")
        
        except Exception as e:
            logging.error(f"Failed to send heartbeat: {e}")    

    def handle_shutdown(self, signum, frame):
        logging.warning(f"Received shutdown signal (signal {signum}). Preparing to stop...")
        self.send_to_master(
            url="",
            extracted_urls=[],
            depth=-1,
            max_depth=-1,
            status="shutdown",
            error="Crawler is about to shut down due to manual request"
        )
        self.shutdown_requested = True

    def handle_master_shutdown(self, receipt_handle):
        # Handle shutdown signal from master node
        logging.info(f"[Crawler {self.crawler_id}] Received shutdown signal from master")
        self.send_to_master(
            url="",
            extracted_urls=[],
            depth=-1,
            max_depth=-1,
            status="shutdown",
            error="Received shutdown signal from master"
        )
        # Remove from heartbeat table
        try:
            self.heartbeat_table.put_item(
                Item={
                    'crawler_id': self.crawler_id,
                    'status': 'shutdown',
                    'last_heartbeat': datetime.now(timezone.utc).isoformat()
                }
            )
            logging.info(f"[Crawler {self.crawler_id}] Removed from heartbeat table")
        except Exception as e:
            logging.error(f"[Crawler {self.crawler_id}] Error removing from heartbeat table: {e}")
        
        # Delete the message
        self.sqs.delete_message(
            QueueUrl=self.crawler_queue_url,
            ReceiptHandle=receipt_handle
        )
        logging.info(f"[Crawler {self.crawler_id}] Entered shutdown state")
        self.is_shutdown = True
        return True

    def handle_wake_up(self, receipt_handle):
        """Handle wake-up signal from master node"""
        logging.info(f"[Crawler {self.crawler_id}] Received wake-up signal from master")
        # Register in heartbeat table
        try:
            self.heartbeat_table.put_item(
                Item={
                    'crawler_id': self.crawler_id,
                    'status': 'running',
                    'last_heartbeat': datetime.now(timezone.utc).isoformat()
                }
            )
            logging.info(f"[Crawler {self.crawler_id}] Registered in heartbeat table")
        except Exception as e:
            logging.error(f"[Crawler {self.crawler_id}] Error registering in heartbeat table: {e}")
            return False
        
        # Delete the message
        self.sqs.delete_message(
            QueueUrl=self.crawler_queue_url,
            ReceiptHandle=receipt_handle
        )
        logging.info(f"[Crawler {self.crawler_id}] Ready to process URLs")
        return True

    def is_allowed_by_robots(self, url):
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
            return rp.can_fetch("*", url)   # returns True if the URL is allowed by robots.txt
        except:
            logging.warning(f"Error reading robots.txt for URL: {url}")
            return True

    def fetch_url(self, url):
        logging.info(f"Starting fetch attempt for URL: {url}")
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            logging.info(f"Successfully fetched URL: {url} (Status: {response.status_code})")
            return response.text
        except Exception as e:
            logging.warning(f"Error fetching URL: {url} | Error: {str(e)}")
            return None


    def extract_content(self, html_content, base_url, domain):
        # Extracts the title, text content, meta description, canonical URL, and all URLs from the HTML content.
        soup = BeautifulSoup(html_content, 'html.parser')

        title = soup.title.string.strip() if soup.title and soup.title.string else "Untitled"
        text_content = soup.get_text(separator=' ', strip=True)
        
        # Extract meta description
        meta_tag = soup.find("meta", attrs={"name": "description"})
        meta_description = meta_tag["content"].strip() if meta_tag and meta_tag.get("content") else ""
        
        # Extract meta keywords
        keywords = []
        keywords_tag = soup.find("meta", attrs={"name": "keywords"})
        if keywords_tag and keywords_tag.get("content"):
            keywords = [k.strip() for k in keywords_tag["content"].split(",")]
        
        canonical_tag = soup.find("link", rel="canonical")
        canonical_url = urljoin(base_url, canonical_tag["href"].strip()) if canonical_tag and canonical_tag.get("href") else None

        urls = set()
        for a in soup.find_all('a', href=True):
            parsed = urlparse(urljoin(base_url, a['href']))
            url_domain = parsed.netloc.lower()
            if parsed.scheme in ('http', 'https') and (domain is None or domain in url_domain):
                normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}{parsed.query}{parsed.fragment}"
                if normalized_url != base_url:
                    urls.add(normalized_url)
        urls = list(urls)
        logging.info(f"Extracted {len(urls)} URLs from URL: {base_url}")

        return title, text_content, meta_description, canonical_url, urls, keywords


    def send_to_master(self, url, extracted_urls, depth, max_depth, status, error=None, assigned_at=None, domain=None):
        # Send crawl results (urls and status) to the master queue.
        message = {
            "crawler_id": self.crawler_id,
            "status": status,
            "error": error,
            "url": url,
            "extracted_urls": extracted_urls,
            "depth": depth,
            "max_depth": max_depth,
            "domain": domain,
            "assigned_at": assigned_at,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        try:
            self.sqs.send_message(
                QueueUrl=self.master_queue_url,
                MessageBody=json.dumps(message)
            )
            logging.info(f"Reported crawl result to master for URL: {url}")
        except Exception as e:
            logging.error(f"Failed to report crawl result to master for URL: {url} | Error: {e}")

    def upload_content_to_s3(self, url, title, meta_description, canonical_url, text_content, keywords):
        # Upload extracted text content to S3.        
        s3_key = f"crawled_content/{hashlib.md5(url.encode()).hexdigest()}.json"
        content = {
            "url": url,
            "title": title,
            "meta_description": meta_description,
            "canonical_url": canonical_url,
            "text_content": text_content,
            "keywords": keywords
        }
        try:
            self.s3.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=json.dumps(content))
            logging.info(f"Uploaded content to S3: {s3_key}")
            return s3_key
        except Exception as e:
            logging.error(f"Failed to upload content to S3: {e}")
            return None

    def send_to_indexer(self, s3_key, url):
        # Send S3 info to indexer queue.
        message = {
            "url": url,
            "s3_key": s3_key
        }
        try:
            self.sqs.send_message(
                QueueUrl=self.indexer_queue_url,
                MessageBody=json.dumps(message)
            )
            logging.info(f"Sent index data for URL: {url}")
        except Exception as e:
            logging.error(f"Failed to send index data for URL: {url} | Error: {e}")

    def start_crawling(self):
        # Start pulling URLs from the crawler queue.
        while True:
            # Send a heartbeat at the start of each iteration
            self.heartbeat()
            
            # Check for shutdown request
            if self.shutdown_requested:
                logging.info("Shutdown requested. Exiting crawler before processing new messages.")
                break
            
            response = self.sqs.receive_message(
                QueueUrl = self.crawler_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )

            messages = response.get('Messages', [])
            if not messages:
                logging.info("Waiting for messages in crawler queue...")
                time.sleep(self.delay)
                continue

            message = messages[0]
            receipt_handle = message['ReceiptHandle']
            body = json.loads(message['Body'])
            
            # Check for wake-up signal from master
            if body.get('status') == 'wake_up' and body.get('crawler_id') == self.crawler_id:
                if self.is_shutdown:  # Only handle wake-up if we're in shutdown state
                    self.handle_wake_up(receipt_handle)
                    self.is_shutdown = False
                continue

            # Check for shutdown signal from master
            if body.get('status') == 'shutdown'  and body.get('crawler_id') == self.crawler_id:
                if not self.is_shutdown:  # Only handle shutdown if we're not already shutdown
                    self.handle_master_shutdown(receipt_handle)
                continue

            # Only process URLs if not in shutdown state
            if not self.is_shutdown:
                url = body.get('url')
                depth = body.get('depth', 0)
                max_depth = body.get('max_depth', 2)  # Default to 2 if not provided
                domain = body.get('domain')
                assigned_at = body.get('assigned_at')
                logging.info(f"Processing URL: {url}")

                if not self.is_allowed_by_robots(url):
                    logging.warning(f"URL blocked by robots.txt: {url}")
                    self.send_to_master(url=url, extracted_urls=[], depth=depth, max_depth=max_depth, status="skipped", error="robots.txt disallowed")
                    continue

                html_content = self.fetch_url(url)

                if html_content:
                    title, text_content, meta_description, canonical_url, extracted_urls, keywords = self.extract_content(html_content, url, domain)
                    logging.info(f"Successfully processed URL: {url}")

                    self.send_to_master(url=url, status="success", extracted_urls=extracted_urls, depth=depth, max_depth=max_depth, domain=domain, assigned_at=assigned_at)
                    s3_key = self.upload_content_to_s3(url, title, meta_description, canonical_url, text_content, keywords)
                    if s3_key:
                        self.send_to_indexer(s3_key, url)
                    else:
                        logging.error(f"Failed to upload content to S3 for URL: {url}")
                        self.send_to_master(url=url, extracted_urls=[], depth=depth, max_depth=max_depth, status="failed", error="Failed to upload to S3")
                else:
                    logging.error(f"Failed to fetch URL: {url}")
                    self.send_to_master(url=url, extracted_urls=[], depth=depth, max_depth=max_depth, status="failed", error="Failed to fetch")

            # Delete the processed message from queue
            try:
                self.sqs.delete_message(
                    QueueUrl=self.crawler_queue_url,
                    ReceiptHandle=receipt_handle
                )
                logging.info(f"Deleted message from crawler queue for URL: {url}")
            except Exception as e:
                logging.error(f"Failed to delete message from queue for URL {url}: {e}")

            time.sleep(self.delay)  # Respect delay to avoid hammering servers
