import time
import json
import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging
import os       #temporary until we have a real S3 bucket or whatever we need to do :) 


class Crawler:
    def __init__(self, 
                 crawler_queue, 
                 master_queue, 
                 indexer_queue, 
                 s3_bucket,
                 delay=1    # Politeness logic
                 ):

        # Initialization of crawler node.
        self.crawler_queue = crawler_queue
        self.master_queue = master_queue
        self.indexer_queue = indexer_queue
        self.s3_bucket = s3_bucket
        self.delay = delay

        # Create AWS clients
        self.sqs = boto3.client('sqs')  # AWS SQS client
        self.s3 = boto3.client('s3')    # AWS S3 client

        # Configure logging to show time, log level, and message
        logging.basicConfig(filename='crawler_node.log', filemode='w', level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    def fetch_url(self, url):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"Failed to fetch {url}: {e}")
            return None

    def extract_content_and_links(self, html_content, base_url):
        # Extract text and absolute URLs from HTML content.
        soup = BeautifulSoup(html_content, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True)
        links = [urljoin(base_url, a['href']) for a in soup.find_all('a', href=True)]
        return text_content, links

    def send_to_master(self, crawled_url, extracted_links, status, error=None):
        # Send crawl results (links and status) to the master queue.
        message = {
            "type": "crawl_result",
            "crawled_url": crawled_url,
            "extracted_links": extracted_links,
            "status": status,
            "error": error
        }

        self.sqs.send_message(
            QueueUrl=self.master_queue,
            MessageBody=json.dumps(message)
        )

        logging.info(f"Sent crawl result to master for URL: {crawled_url}")

    def upload_content_to_s3(self, document_url, text_content):
        # Upload extracted text content to S3.
        s3_key = f"crawled_content/{hash(document_url)}.json"
        content = {
            "document_url": document_url,
            "text_content": text_content
        }

        self.s3.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=json.dumps(content))
        logging.info(f"Uploaded content to S3: {s3_key}")
        return s3_key

    def save_content_locally(self, document_url, text_content):
        # Save extracted text content to a local file instead of S3 (for testing).
        if not os.path.exists("crawled_content"):
            os.makedirs("crawled_content")  # Create directory if it doesn't exist

        filename = f"crawled_content/{hash(document_url)}.json"
        content = {
            "document_url": document_url,
            "text_content": text_content
        }
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=4)

        logging.info(f"Saved content locally at: {filename}")
        return filename

    def send_to_indexer(self, s3_key, document_url):
        # Send S3 info to indexer queue.
        message = {
            "url": document_url,
            "title": "LESA HASHOF HANGEBAK EZAY",
            "s3_key": s3_key,
        }

        self.sqs.send_message(
            QueueUrl=self.indexer_queue,
            MessageBody=json.dumps(message)
        )

        logging.info(f"Sent index data for URL: {document_url}")

    def start_crawling(self):
        # Start pulling URLs from the crawler queue.
        while True:
            response = self.sqs.receive_message(
                QueueUrl = self.crawler_queue,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )

            

            messages = response.get('Messages', [])
            if not messages:
                logging.info("Waiting for messages in crawler queue...")
                time.sleep(self.delay)
                continue

            # Process each URL in the queue.
            for message in messages:
                receipt_handle = message['ReceiptHandle']
                body = json.loads(message['Body'])
                url = body.get('url')

                logging.info(f"Processing URL: {url}")
                html_content = self.fetch_url(url)

                if html_content:
                    text_content, extracted_links = self.extract_content_and_links(html_content, url)
                    self.send_to_master(url, extracted_links, status="success")
                    #s3_key = self.upload_content_to_s3(url, text_content)
                    #self.send_to_indexer(s3_key, url)
                    self.save_content_locally(url, text_content)
                else:
                    self.send_to_master(url, extracted_links=[], status="failed", error="Failed to fetch")

                # Delete the processed message from queue
                self.sqs.delete_message(
                    QueueUrl=self.crawler_queue,
                    ReceiptHandle=receipt_handle
                )

                logging.info(f"Finished processing URL: {url}")

                time.sleep(self.delay)  # Respect delay to avoid hammering servers
