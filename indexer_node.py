import boto3
import json

import time
import logging
import re
import nltk

from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import Schema, ID, TEXT, DATETIME
from whoosh.qparser import QueryParser

nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')

from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

STOPWORDS = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()


index_dir = "index_dir"

schema = Schema(
    url=ID(stored=True, unique=True),
    title=TEXT(stored=True),
    content=TEXT(stored=True)
)


def initialize_index():
    if exists_in(index_dir):
        return open_dir(index_dir)
    return create_in(index_dir, schema)
    



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

def preprocessing(text):
    text = text.lower()
    text = clean_text(text)
    tokens = text.split()
    tokens = [word for word in tokens if word not in STOPWORDS]
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in tokens]
    return " ".join(lemmatized_tokens)

def clean_text(text):
    text = re.sub(r'<[^>]+>', '', text) # Remove HTML tags
    text = re.sub(r'[^a-z\s0-9]', '', text) # Remove special characters and punctuation
    text = re.sub(r'\s+', ' ', text).strip() # Remove extra spaces
    return text



def search_index(ix, query_str):
    processed_query = preprocessing(query_str)

    with ix.searcher() as searcher:
        # Define the query parser and specify the fields to search (e.g., title, content)
        query_parser = QueryParser("content", schema=ix.schema)
        query = query_parser.parse(processed_query)
        
        # Perform the search
        results = searcher.search(query, limit=None)  # Adjust limit based on your needs

        # Return results as a list of dictionaries
        search_results = [
            {"url": result["url"], "title": result["title"], "content": result["content"]}
            for result in results
        ]
        
        return search_results
def indexer_process():
    ix = initialize_index()

    sqs_content = boto3.client('sqs', region_name='your-region', aws_access_key_id='your-access-key', aws_secret_access_key='your-secret-key')
    sqs_search = boto3.client('sqs', region_name='your-region', aws_access_key_id='your-access-key', aws_secret_access_key='your-secret-key')
    sqs_response = boto3.client('sqs', region_name='your-region', aws_access_key_id='your-access-key', aws_secret_access_key='your-secret-key')

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

        if messages_content:

            message = messages_content[0]
            body = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']

            content_to_index = body.get('content')
            url_recv = body.get('url')
            title_recv = body.get('title')
            timestamp = body.get('timestamp')

            

            # logging.info(f"Indexer received content from Crawler {source_rank} to index.")
            if content_to_index and url_recv:
                try:
                
                    with ix.writer() as writer:
                        writer.add_document(
                            url= url_recv,
                            title=title_recv,
                            content=preprocessing(content_to_index)
                        )


                    logging.info(f"Successfully indexed content for URL: {url_recv}")                #comm.send(f"Indexer {rank} - Indexed content from Crawler {source_rank}", dest=0, tag=99) # Send status update to master (tag 99)
                except Exception as e:
                    logging.error(f"Error indexing content for URL {url_recv}: {e}")                #comm.send(f"Indexer {rank} - Error indexing: {e}", dest=0, tag=999) # Report error to master (tag 999)
        

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
        
        if messages_search:
            message = messages_search[0]
            body = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']

            query = body.get('query')
            response_queue = body.get('response_queue')


            if query and response_queue:
                # Perform the search
                try:
                    results = search_index(ix, query)

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


if __name__ == '__main__':
    indexer_process()