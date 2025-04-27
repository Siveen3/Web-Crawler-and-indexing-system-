from mpi4py import MPI
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
    content=TEXT(stored=True),
    timestamp=DATETIME(stored=True)
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
            {"url": result["url"], "title": result["title"], "content": result["content"], "timestamp": result["timestamp"]}
            for result in results
        ]
        
        return search_results

def indexer_process():

    ix = initialize_index()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Indexer node started with rank {rank} of {size}")


    while True:
        status = MPI.Status()
        content_to_index = comm.recv(source=MPI.ANY_SOURCE, tag=2, status=status) # Receive content from crawlers (tag 2)
        source_rank = status.Get_source()

        if not content_to_index: # Could be a shutdown signal
            logging.info(f"Indexer {rank} received shutdown signal.Exiting.")
            break

        logging.info(f"Indexer {rank} received content from Crawler {source_rank} to index.")

        try:
            
            with ix.writer() as writer:
                writer.add_document(
                    url=content_to_index['url'],
                    title=content_to_index['title'],
                    content=preprocessing(content_to_index['content']),
                    timestamp=content_to_index['timestamp']
                )




            time.sleep(1) # Simulate indexing delay
            logging.info(f"Indexer {rank} indexed content from Crawler {source_rank}.")
            comm.send(f"Indexer {rank} - Indexed content from Crawler {source_rank}", dest=0, tag=99) # Send status update to master (tag 99)
        except Exception as e:
            logging.error(f"Indexer {rank} error indexing content from Crawler {source_rank}: {e}")
            comm.send(f"Indexer {rank} - Error indexing: {e}", dest=0, tag=999) # Report error to master (tag 999)
    

        # Handle search requests
        search_request = comm.recv(source=MPI.ANY_SOURCE, tag=1)  # Receive search query (tag 1)
        if search_request:
            logging.info(f"Indexer {rank} received search query: {search_request}")
            search_results = search_index(ix, search_request)  # Perform search on the index

            # Send search results back to the master or requesting node
            comm.send(search_results, dest=0, tag=101)  # Send search results to master (tag 101)

if __name__ == '__main__':
    indexer_process()

