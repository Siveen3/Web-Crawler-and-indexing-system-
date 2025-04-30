import boto3
import json
import os
import re
import time
import logging
import nltk

from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import Schema, ID, TEXT
from whoosh.qparser import QueryParser

nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')

class Indexer:
    def __init__(self, content_queue_url, search_queue_url, region_name='us-east-1', index_dir='index_dir'):
        self.region_name = region_name
        self.content_queue_url = content_queue_url
        self.search_queue_url = search_queue_url
        self.index_dir = index_dir

        # Initialize stopwords and lemmatizer
        self.stopwords = set(stopwords.words('english'))
        self.lemmatizer = WordNetLemmatizer()

        # Initialize AWS clients
        self.sqs = boto3.client('sqs', region_name=self.region_name)
        self.s3 = boto3.client('s3', region_name=self.region_name)

        self.schema = Schema(
            url=ID(stored=True, unique=True),
            title=TEXT(stored=True),
            content=TEXT(stored=True)
        )
        self.index = self.initialize_index()

        # Configure logging
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - Indexer - %(levelname)s - %(message)s')


    def initialize_index(self):
        os.makedirs(self.index_dir, exist_ok=True)  # Create directory if it doesn't exist
        if exists_in(self.index_dir):
            return open_dir(self.index_di, exis)
        return create_in(self.index_dir, self.schema)

    def clean_text(self, text):
        text = re.sub(r'<[^>]+>', '', text)     # Remove HTML tags
        text = re.sub(r'[^a-z0-9\s]', '', text.lower()) # Remove special characters and punctuation
        #apply text.lower here so no need to make it in preprocessing
        
        text = re.sub(r'\s+', ' ', text).strip() # Remove extra spaces
        return text

    def preprocessing(self, text):
        text = self.clean_text(text) # OR PASS it LATER IN YOUR MAIN/START
        tokens = text.split()
        filtered_tokens = [t for t in tokens if t not in self.stopwords]
        lemmatized_tokens = [self.lemmatizer.lemmatize(t) for t in filtered_tokens]
        return ' '.join(lemmatized_tokens)
    

        def search_index(self, ix, query_str):
        processed_query = self.preprocessing(query_str)

        with ix.searcher() as searcher:
            # Define the query parser and specify the fields to search (e.g., title, content)
            query_parser = QueryParser("content", schema=ix.schema)
            query = query_parser.parse(processed_query)

            # Perform the search
            results = searcher.search(query, limit=None)

            # Return results as a list of dictionaries
            search_results = [
                {"url": result["url"], "title": result["title"], "content": result["content"]}
                for result in results
            ]
            return search_results

    def indexer_process(self):
        ix = self.index

        sqs_content = boto3.client('sqs', region_name=self.region_name, aws_access_key_id='your-access-key', aws_secret_access_key='your-secret-key')
        sqs_search = boto3.client('sqs', region_name=self.region_name, aws_access_key_id='your-access-key', aws_secret_access_key='your-secret-key')
        sqs_response = boto3.client('sqs', region_name=self.region_name, aws_access_key_id='your-access-key', aws_secret_access_key='your-secret-key')

        content_queue_url = 'your-content-queue-url'
        search_queue_url = 'your-search-queue-url'

        logging.info("Indexer node started and waiting for messages...")

        while True:
            # --- 1. Check for new content to index ---
            try:
                response_content = sqs_content.receive_message(
                    QueueUrl=content_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=2
                )

                messages_content = response_content.get('Messages', [])
            except Exception as e:
                logging.error(f"Error receiving from content queue: {e}")
                messages_content = []

            if messages_content:
                message = messages_content[0]
                body = json.loads(message['Body'])
                receipt_handle = message['ReceiptHandle']

                content_to_index = body.get('content')
                url_recv = body.get('url')
                title_recv = body.get('title')

                if content_to_index and url_recv:
                    try:
                        with ix.writer() as writer:
                            writer.add_document(
                                url=url_recv,
                                title=title_recv,
                                content=self.preprocessing(content_to_index)
                            )

                        logging.info(f"Successfully indexed content for URL: {url_recv}")
                    except Exception as e:
                        logging.error(f"Error indexing content for URL {url_recv}: {e}")

                try:
                    # Delete the processed message from queue                
                    sqs_content.delete_message(QueueUrl=content_queue_url, ReceiptHandle=receipt_handle)
                    logging.info(f"Deleted content message for URL: {url_recv}")
                except Exception as e:
                    logging.error(f"Error deleting message from content queue: {e}")

            # --- 2. Check for search requests ---
            try:
                response_search = sqs_search.receive_message(
                    QueueUrl=search_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=2
                )

                messages_search = response_search.get('Messages', [])
            except Exception as e:
                logging.error(f"Error receiving from search queue: {e}")
                messages_search = []

            if messages_search:
                message = messages_search[0]
                body = json.loads(message['Body'])
                receipt_handle = message['ReceiptHandle']

                query = body.get('query')
                response_queue = body.get('response_queue')

                if query and response_queue:
                    # Perform the search
                    try:
                        results = self.search_index(ix, query)

                        # Send the search results back to the provided queue
                        sqs_response.send_message(
                            QueueUrl=response_queue,
                            MessageBody=json.dumps(results)
                        )

                        logging.info(f"Processed search query: {query}")
                    except Exception as e:
                        logging.error(f"Error processing search query: {e}")

                try:
                    sqs_search.delete_message(QueueUrl=search_queue_url, ReceiptHandle=receipt_handle)
                    logging.info(f"Deleted search message for query: {query}")
                except Exception as e:
                    logging.error(f"Error deleting message from search queue: {e}")

            # --- 3. Sleep if no activity ---
            if not messages_content and not messages_search:
                time.sleep(2)



'''
    def download_s3_text(self, s3_key, bucket):
        try:
            obj = self.s3.get_object(Bucket=bucket, Key=s3_key)
            content = json.loads(obj['Body'].read().decode('utf-8'))
            return content.get("text_content", "")
        except Exception as e:
            logging.error(f"Failed to read from S3 ({s3_key}): {e}")
            return ""

    def index_document(self, url, title, content):
        try:
            with self.index.writer() as writer:
                writer.update_document(
                    url=url,
                    title=title,
                    content=self.preprocessing(content)
                )
            logging.info(f"Indexed: {url}")
        except Exception as e:
            logging.error(f"Error indexing document ({url}): {e}")

    def handle_index_message(self, message):
        try:
            body = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']

            url = body.get("url")
            title = body.get("title")
            s3_key = body.get("s3_key")
            s3_bucket = s3_key.split("/")[0] if "/" in s3_key else self.default_bucket()

            if url and title and s3_key:
                text = self.download_s3_text(s3_key, s3_bucket)
                if text:
                    self.index_document(url, title, text)
                else:
                    logging.warning(f"No content found for {url}")
            else:
                logging.warning("Invalid message format received for indexing.")

            self.sqs.delete_message(QueueUrl=self.content_queue_url, ReceiptHandle=receipt_handle)

        except Exception as e:
            logging.error(f"Failed to process indexing message: {e}")

    def handle_search_message(self, message):
        try:
            # Process the search query (search_index)
            body = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']
            query = body.get("query")
            response_queue = body.get("response_queue")

            if query and response_queue:
                processed_query = self.preprocessing(query)
                parser = QueryParser("content", schema=self.index.schema)
                q = parser.parse(processed_query)

                with self.index.searcher() as searcher:
                    results = searcher.search(q, limit=10)
                    response = [
                        {"url": r["url"], "title": r["title"]} for r in results
                    ]
                    self.sqs.send_message(
                        QueueUrl=response_queue,
                        MessageBody=json.dumps(response)
                    )
                    logging.info(f"Returned {len(response)} result(s) for query: {query}")
            else:
                logging.warning("Invalid search request received.")

            self.sqs.delete_message(QueueUrl=self.search_queue_url, ReceiptHandle=receipt_handle)

        except Exception as e:
            logging.error(f"Failed to handle search message: {e}")

    def default_bucket(self):
        return self.s3_bucket if hasattr(self, 's3_bucket') else 'your-default-bucket-name'

    def start(self):
        logging.info("Indexer started and listening...")
        while True:
            has_work = False

            # Check for content to index
            try:
                resp = self.sqs.receive_message(
                    QueueUrl=self.content_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=2
                )
                for msg in resp.get('Messages', []):
                    self.handle_index_message(msg)
                    has_work = True
            except Exception as e:
                logging.error(f"Error receiving from content queue: {e}")

            # Check for search queries
            try:
                resp = self.sqs.receive_message(
                    QueueUrl=self.search_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=2
                )
                for msg in resp.get('Messages', []):
                    self.handle_search_message(msg)
                    has_work = True
            except Exception as e:
                logging.error(f"Error receiving from search queue: {e}")

            if not has_work:
                time.sleep(1)
'''
