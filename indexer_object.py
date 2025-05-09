from indexer import Indexer
from opensearchpy import OpenSearch
import certifi

host = "vpc-opensearch-domain-4xiamr7tymr4563vc4hxuvszia.us-east-1.es.amazonaws.com"
port = 443
username = "dist"
password = "Hager#2004" 


es = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=(username, password),
    use_ssl=True,
    verify_certs=True,
    ca_certs=certifi.where()
)

indexer = Indexer(es=es,
                  index_name="content_index", 
                  dynamodb_table="IndexerHeartbeatTable", 
                  s3_bucket="crawler-indexer-buckets", 
                  content_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/IndexQueue', 
                  search_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/SearchQueue', 
                  response_queue_url='https://sqs.us-east-1.amazonaws.com/138749495090/FeedbackQueue',
                  indexer_id = 'indexer_id'
                  )


indexer.start_indexing()
