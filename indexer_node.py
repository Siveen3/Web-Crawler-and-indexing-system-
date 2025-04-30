import boto3
import json
import logging
import re
import time
from elasticsearch import Elasticsearch


class WebIndexer:
    # NO DEFAULT VALUES
    def __init__(self, es_host="localhost", es_port=9200, index_name="web_content",
                 region='us-east-1', aws_key="your-access-key", aws_secret="your-secret-key",
                 content_queue_url='your-content-queue-url', search_queue_url='your-search-queue-url', delay=2):

        self.es_host = es_host
        self.es_port = es_port
        self.index_name = index_name
        self.delay = delay

        self.region = region
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.content_queue_url = content_queue_url
        self.search_queue_url = search_queue_url

        # Initialize Elasticsearch
        self.es = self.initialize_elasticsearch()
        
        # Initialize clients
        self.sqs_content = boto3.client('sqs', region_name=region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        self.sqs_search = boto3.client('sqs', region_name=region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        self.sqs_response = boto3.client('sqs', region_name=region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        self.s3 = boto3.client('s3', region_name=region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

    def initialize_elasticsearch(self):
        es = Elasticsearch([{'host': self.es_host, 'port': self.es_port}])
        if not es.indices.exists(index=self.index_name):
            index_settings = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "analysis": {
                        "filter": {
                            "english_stop": {"type": "stop", "stopwords": "_english_"},
                            "english_stemmer": {"type": "stemmer", "language": "english"},
                            "english_possessive_stemmer": {"type": "stemmer", "language": "possessive_english"}
                        },
                        "analyzer": {
                            "english_analyzer": {
                                "tokenizer": "standard",
                                "filter": ["english_possessive_stemmer", "lowercase", "english_stop", "english_stemmer"]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "url": {"type": "keyword", "copy_to": ["canonical_url"]},
                        "title": {"type": "text", "analyzer": "english_analyzer"},
                        "content": {"type": "text", "analyzer": "english_analyzer"},
                        "meta_description": {"type": "text", "analyzer": "english_analyzer"}
                    }
                }
            }
            es.indices.create(index=self.index_name, body=index_settings)
            logging.info(f"Created Elasticsearch index: {self.index_name}")
        return es

    def preprocess_text(self, text):
        if not text:
            return ""
        text = re.sub(r'<[^>]+>', '', text) # Remove HTML tags
        text = re.sub(r'[^a-z\s0-9]', '', text.lower()) # Remove special characters and punctuation
        return re.sub(r'\s+', ' ', text).strip() # Remove extra spaces

    def index_document(self, url, title, content, meta_description=None):
        doc = {
            "url": url,
            "title": title,
            "content": self.preprocess_text(content),
            "meta_description": self.preprocess_text(meta_description) if meta_description else ""
        }
        try:
            self.es.index(index=self.index_name, id=url, body=doc)
            logging.info(f"Indexed document: {url}")
        except Exception as e:
            logging.error(f"Failed to index {url}: {e}")


    def read_from_s3(self, s3_key):
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            logging.info(f"Read from S3: {s3_key}")
            return content

        except Exception as e:
            logging.error(f"Failed to read from S3: {e}")
            return None

    def search_index(self, query_str):
        search_body = {
            "query": {
                "function_score": {
                    "query": {
                        "multi_match": {
                            "query": query_str,
                            "fields": ["title^3", "content^1", "meta_description^2"],
                            "type": "cross_fields"
                        }
                    }
                }
            },
            "highlight": {
                "fields": {
                    "content": {"number_of_fragments": 3},
                    "title": {},
                    "meta_description": {}
                }
            }
        }
        try:
            results = self.es.search(index=self.index_name, body=search_body)
            return self.format_search_results(results)
        except Exception as e:
            logging.error(f"Search failed: {e}")
            return []

    def format_search_results(self, results):
        formatted = []
        for hit in results['hits']['hits']:
            source = hit['_source']
            formatted.append({
                "url": hit["_id"],
                "title": source.get("title", ""),
                #"content": source.get("content", ""),
                "highlight": hit.get("highlight", {})
            })
        return formatted


    def index_process(self):
        try:
            response_content = self.sqs_content.receive_message(
                QueueUrl=self.content_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=2
            )
            message_content = response_content.get('Messages', [])
        except Exception as e:
            logging.error(f"Error receiving from content queue: {e}")
            message_content = []

        if message_content:
            message = message_content[0]
            body = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']
            
            s3_key = body.get('s3_key')
            url = body.get('url')

            # READ S3 CONTENT
            content = self.read_from_s3(s3_key)
            if content:
                title = content['title']
                meta_description = content['meta_description']      
                canonical_url = content['canonical_url']
                text_content = content['text_content']

            else:
                return


            if text_content:
                self.index_document(
                    url=canonical_url if canonical_url else url,
                    title=title,
                    content=text_content,
                    meta_description=meta_description
                )
                
                try:
                    self.sqs_content.delete_message(
                        QueueUrl=self.content_queue_url, 
                        ReceiptHandle=receipt_handle
                    )
                    logging.info(f"Deleted content message for URL: {url}")
                except Exception as e:
                    logging.error(f"Error deleting message from content queue: {e}")

    def search_process(self):
        try:
            response_search = self.sqs_search.receive_message(
                QueueUrl=self.search_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=2
            )
            message_search = response_search.get('Messages', [])
        except Exception as e:
            logging.error(f"Error receiving from search queue: {e}")
            message_search = []
        
        if message_search:
            message = message_search[0]
            body = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']
            
            query = body.get('query')
            response_queue = body.get('response_queue')
            
            if query and response_queue:
                try:
                    results = self.search_index(query)
                    
                    # Send results back
                    self.sqs_response.send_message(
                        QueueUrl=response_queue,
                        MessageBody=json.dumps(results)
                    )
                    
                    logging.info(f"Processed search query: {query}")
                except Exception as e:
                    logging.error(f"Error processing search query: {e}")
                
                try:
                    self.sqs_search.delete_message(
                        QueueUrl=self.search_queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logging.info(f"Deleted search message for query: {query}")
                except Exception as e:
                    logging.error(f"Error deleting message from search queue: {e}")



    def start_indexing(self):
        logging.info("Indexer started. Waiting for SQS messages...")
        while True:
            self.index_process()
            self.search_process()
            time.sleep(self.delay)
        