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
                 delay=1    # Politeness logic
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

        # Create AWS clients
        self.sqs = boto3.client('sqs', region_name=self.region)  # AWS SQS client
        self.s3 = boto3.client('s3', region_name=self.region)    # AWS S3 client
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
        self.heartbeat_table = self.dynamodb.Table(self.dynamodb_table)
        self.crawled_table = self.dynamodb.Table(self.dynamodb_table)
        # Configure logging to show time, log level, and message
        logging.basicConfig(filename='crawler_node.log', filemode='w', level=logging.INFO, 
                            format='%(asctime)s [%(levelname)s] %(message)s')
        
        # Handle graceful shutdown
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)


    def handle_shutdown(self, signum, frame):
        logging.warning(f"Received shutdown signal (signal {signum}). Preparing to stop...")
        self.send_to_master(
            url="",
            extracted_urls=[],
            depth=-1,
            status="shutdown",
            error="Crawler is about to shut down due to manual request"
        )
        self.shutdown_requested = True

    def heartbeat(self):
        try:
            self.heartbeat_table.put_item(
                Item={
                    'crawler_id': self.crawler_id,
                    'status': 'running',
                    'last_heartbeat': datetime.now(timezone.utc).isoformat()
                }
            )
            logging.info(f"Heartbeat sent for crawler {self.crawler_id} at {time.time()}")
        
        except Exception as e:
            logging.error(f"Failed to send heartbeat: {e}")


    def save_crawled_url(self, url, domain, s3_key):
        try:
            self.crawled_table.put_item(
                Item={
                    'url': url,
                    'domain': domain,
                    's3_key': s3_key,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
            )
            logging.info(f"Saved crawled URL: {url} to crawled_table")
        except Exception as e:
            logging.error(f"Failed to save crawled URL: {url} | Error: {e}")
            self.send_to_master(url=url, extracted_urls=[], depth=-1, status="failed", error=f"Failed to save to crawled_table: {e}")
            

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

    def fetch_url(self, url, max_retries=3, backoff=2):
        # backoff: wait time in seconds between retries.
        logging.info(f"Starting fetch attempt for URL: {url}")

        for attempt in range(1, max_retries + 1):
            try:
                logging.info(f"Fetching attempt {attempt} for URL: {url}")
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                logging.info(f"Successfully fetched URL: {url} (Status: {response.status_code})")
                return response.text
            except Exception as e:
                logging.warning(f"Error fetching URL: {url} | Error: {str(e)}")
                logging.warning(f"Attempt {attempt} failed. {3-attempt} attempts remaining.")
                if attempt < max_retries:
                    time.sleep(backoff)
                else:
                    logging.error(f"All {max_retries} attempts failed to fetch URL: {url}")
                    return None
                # DID WE SEND TO MASTER?


    def extract_content(self, html_content, base_url, domain):
        # Extracts the title, text content, meta description, canonical URL, and all URLs from the HTML content.
        soup = BeautifulSoup(html_content, 'html.parser')

        title = soup.title.string.strip() if soup.title and soup.title.string else "Untitled"
        text_content = soup.get_text(separator=' ', strip=True)
        meta_tag = soup.find("meta", attrs={"name": "description"})
        meta_description = meta_tag["content"].strip() if meta_tag and meta_tag.get("content") else ""
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

        return title, text_content, meta_description, canonical_url, urls


    def send_to_master(self, url, extracted_urls, depth, status, error=None, assigned_at=None, domain=None):
        # Send crawl results (urls and status) to the master queue.
        message = {
            "crawler_id": self.crawler_id,
            "status": status,
            "error": error,
            "url": url,
            "extracted_urls": extracted_urls,
            "depth": depth,
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

    def upload_content_to_s3(self, url, title, meta_description, canonical_url, text_content):
        # Upload extracted text content to S3.        
        s3_key = f"crawled_content/{hashlib.md5(url.encode()).hexdigest()}.json"
        content = {
            "url": url,
            "title": title,
            "meta_description": meta_description,
            "canonical_url": canonical_url,
            "text_content": text_content
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
            
            response = self.sqs.receive_message(
                QueueUrl = self.crawler_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )

            if self.shutdown_requested:
                logging.info("Shutdown requested. Exiting before processing new messages.")
                break

            messages = response.get('Messages', [])
            if not messages:
                logging.info("Waiting for messages in crawler queue...")
                time.sleep(self.delay)
                continue

            message = messages[0]
            if self.shutdown_requested:
                logging.info("Shutdown requested. Exiting during message processing.")
                break

            logging.info(f"Processing message: {message}")

            receipt_handle = message['ReceiptHandle']
            body = json.loads(message['Body'])
            url = body.get('url')
            depth = body.get('depth', 0)
            domain = body.get('domain')
            assigned_at = body.get('assigned_at')
            logging.info(f"Processing URL: {url}")

            if not self.is_allowed_by_robots(url):
                logging.warning(f"This URL is blocked by robots.txt: {url}")
                self.send_to_master(url=url, extracted_urls=[], depth=depth, status="skipped", error="robots.txt disallowed")
                logging.info(f"Reported blocked URL to master: {url}")

            else:
                html_content = self.fetch_url(url)

                if html_content:
                    title, text_content, meta_description, canonical_url, extracted_urls = self.extract_content(html_content, url, domain)
                    logging.info(f"Finished processing URL: {url}")

                    self.send_to_master(url=url, status="success", extracted_urls=extracted_urls, depth=depth, domain=domain, assigned_at=assigned_at)
                    logging.info(f"Reported successful URL to master: {url}")
                    s3_key = self.upload_content_to_s3(url, title, meta_description, canonical_url, text_content)
                    if s3_key:
                        self.send_to_indexer(s3_key, url)
                    else:
                        logging.error(f"Skipping indexer send due to failed S3 upload for URL: {url}")
                        self.send_to_master(url=url, extracted_urls=[], depth=depth, status="failed", error="Failed to upload to S3")
                        continue
                else:
                    self.send_to_master(url=url, extracted_urls=[], depth=depth, status="failed", error="Failed to fetch")
                    logging.info(f"Reported failed URL to master: {url}")
                    continue

            # Delete the processed message from queue
            try:
                self.sqs.delete_message(
                    QueueUrl=self.crawler_queue_url,
                    ReceiptHandle=receipt_handle
                )
                logging.info(f"Deleted message from crawler queue for URL: {url}")
            except Exception as e:
                logging.error(f"Failed to delete message from queue: {e}")

            time.sleep(self.delay)  # Respect delay to avoid hammering servers

