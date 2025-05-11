import boto3
import json
import logging
import re
import time
from datetime import datetime, timezone


class Indexer:
    def __init__(self, indexer_id, dynamodb_table, es, index_name, s3_bucket,	content_queue_url, 
                 search_queue_url, response_queue_url, region='us-east-1', delay=2):

        self.indexer_id = indexer_id

        self.dynamodb_table = dynamodb_table
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.heartbeat_table = self.dynamodb.Table(self.dynamodb_table)


        self.statuses = ['index_success', 'index_failure', 'search_success', 'search_failure']
        
        
        # Initialize Elasticsearch
        self.index_name = index_name
        self.es = self.initialize_elasticsearch(es)
        

        # Initialize S3
        self.s3_bucket = s3_bucket

        # Initialize SQS
        self.content_queue_url = content_queue_url
        self.search_queue_url = search_queue_url
        self.response_queue_url = response_queue_url
        
        
        # Initialize clients
        self.region = region
        self.sqs = boto3.client('sqs', region_name=region)
        self.s3 = boto3.client('s3', region_name=region)
        self.delay = delay

        logging.basicConfig(filename='indexer.log', level=logging.INFO,format='%(asctime)s [%(levelname)s] %(message)s')

    def heartbeat(self):
        try:
            self.heartbeat_table.put_item(
                Item={
                    'indexer_id': self.indexer_id,
                    'status': 'running',
                    'last_heartbeat': datetime.now(timezone.utc).isoformat()
                }
            )
            logging.info(f"Heartbeat sent for indexer {self.indexer_id} at {time.time()}")
        except Exception as e:
            logging.error(f"Failed to send heartbeat: {e}")

    def initialize_elasticsearch(self, es):
        if not es.indices.exists(index=self.index_name):
            index_settings = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 2,
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
                        "url": {"type": "keyword"},
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
            self.send_to_master(url, self.statuses[0], url=url)
            logging.info(f"Indexed document: {url}")
        except Exception as e:
            self.send_to_master(url, self.statuses[1], str(e), url=url)
            logging.error(f"Failed to index {url}: {e}")

    def read_from_s3(self, s3_key, url):
        try:
            response = self.s3.get_object(Bucket=self.s3_bucket, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            if not content :
                return 
            content = json.loads(content)
            logging.info(f"Read from S3: {s3_key}")
            return content

        except Exception as e:
            self.send_to_master(url, self.statuses[1], str(e), url=url)
            logging.error(f"Failed to read from S3: {e}")
            
        return None

    def search_index(self, query, mode="and", client_id=None):
        
        mode = mode.lower() if mode else "and"
        
        if mode == "phrase":
            # Phrase match (exact match in order)
            match_query = {
                "match_phrase": {
                    "content": query
                }
            }
            boost_query = {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "meta_description^2"],
                    "type": "phrase"
                }
            }
        elif mode == "and" or mode == "or":
            # Normal match with AND/OR and fuzziness
            match_query = {
                "multi_match": {
                    "query": query,
                    "fields": ["content"],
                    "operator": mode,  # "and" or "or"
                    "fuzziness": "AUTO"
                }
            }
            boost_query = {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "meta_description^2"],
                    "operator": mode,
                    "fuzziness": "AUTO"
                }
            }

        search_body = {
            "query": {
                "bool": {
                    "must": [match_query],
                    "should": [boost_query],
                    "minimum_should_match": 0
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
            return self.format_search_results(results, client_id)
        except Exception as e:
            self.send_to_master(client_id, self.statuses[3], str(e), query=query)
            logging.error(f"Search failed: {e}")
            return []

    def format_search_results(self, results, client_id):
        formatted = []
        for hit in results['hits']['hits']:
            source = hit['_source']
            formatted.({
                "client_id": client_id,
                "url": hit["_id"],
                "title": source.get("title", ""),
                #"content": source.get("content", ""),
                "highlight": hit.get("highlight", {})
            })
        return formatted



    def send_to_master(self, message, status, error=None, url=None, query=None, client_id=None):
        # Send crawl results (urls and status) to the master queue.
        message = {
            "indexer_id": self.indexer_id,
            "message": message,
            "status": status,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "client_id": client_id
        }
                    
        try:
            self.sqs.send_message(
                        QueueUrl=self.response_queue_url,
                        MessageBody=json.dumps(message)
                    )
            if url:
                logging.info(f"Reported index result to master for URL: {url}")
            elif query:
                logging.info(f"Reported search result to master for Query: {query}")
        except Exception as e:
            if url:
                logging.error(f"Failed to report index result to master for URL: {url} | Error: {e}")
            elif query:
                logging.error(f"Failed to report search result to master for Query: {query} | Error: {e}")

    def index_process(self):
        try:
            response_content = self.sqs.receive_message(
                QueueUrl=self.content_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=2
            )
            message_content = response_content.get('Messages', [])
            if not message_content:
                logging.debug("Content queue is empty.")
        except Exception as e:
            logging.error(f"Error receiving from content queue: {e}")
            message_content = []

        if message_content:
            message = message_content[0]
            body = json.loads(message['Body'])
             
            
            s3_key = body.get('s3_key')
            url = body.get('url')

            # READ S3 CONTENT
            content = self.read_from_s3(s3_key, url)
            if not content:
                return
            title = content.get('title', '')
            meta_description = content.get('meta_description', '')
            canonical_url = content.get('canonical_url', '')
            text_content = content.get('text_content', '')



            self.index_document(
                url=canonical_url if canonical_url else url,
                title=title,
                content=text_content,
                meta_description=meta_description
            )
            
            try:
                self.sqs.delete_message(
                    QueueUrl=self.content_queue_url, 
                    ReceiptHandle=message['ReceiptHandle']
                )
                logging.info(f"Deleted content message for URL: {url}")
            except Exception as e:
                logging.error(f"Error deleting message from content queue: {e}")

    def search_process(self):
        try:
            response_search = self.sqs.receive_message(
                QueueUrl=self.search_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=2
            )
            message_search = response_search.get('Messages', [])
            if not message_search:
                logging.debug("Search queue is empty.")

        except Exception as e:
            logging.error(f"Error receiving from search queue: {e}")
            message_search = []
        
        if message_search:
            message = message_search[0]
            body = json.loads(message['Body'])
             
            
            query = body.get('query')
            client_id = body.get('client_id')
            mode = body.get('mode')
            
            if query:
               
                search_result = self.search_index(query, mode, client_id)

                if search_result:
                    self.send_to_master(search_result, self.statuses[2], query=query, client_id=client_id)
                    logging.info(f"{datetime.now()} - Processed search query: {query}")


                
                try:
                    self.sqs.delete_message(
                        QueueUrl=self.search_queue_url,
                        ReceiptHandle= message['ReceiptHandle']
                    )
                    logging.info(f"Deleted search message for query: {query}")
                except Exception as e:
                    logging.error(f"Error deleting message from search queue: {e}")

    def start_indexing(self):
        logging.info("Indexer started. Waiting for SQS messages...")
        while True:
            self.heartbeat()
            self.index_process()
            self.search_process()
            time.sleep(self.delay)



