from indexer import Indexer
from elasticsearch import Elasticsearch
import certifi

host = "https://vpc-opensearch-domain-4xiamr7tymr4563vc4hxuvszia.us-east-1.es.amazonaws.com"
port = 443
username = "Dist"
password = "2004#Dist" 

es = Elasticsearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=(username, password),
    use_ssl=True,
    verify_certs=True,
    ca_certs=certifi.where()
    )

indexer = Indexer(es=es,
                  indexer_id="indexer_1",
                  index_name="content_index", 
                  dynamodb_table="CrawlerHeartbeatTable", 
                  s3_bucket="crawler-indexer-bucket", 
                  content_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/IndexQueue', 
                  search_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/SearchQueue', 
                  response_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/FeedbackQueue'
                  )


indexer.start_indexing()