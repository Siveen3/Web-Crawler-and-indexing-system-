from indexer_node import Indexer

indexer = Indexer(es_host="localhost", dynamodb_table="", es_port=9200, index_name="content_index", 
                  s3_bucket="", content_queue_url='', search_queue_url='', response_queue_url='')

indexer.start_indexing()
