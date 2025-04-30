from indexer_node import Indexer

indexer = Indexer(es_host="localhost", es_port=9200, index_name="", s3_bucket="",
            aws_key="", aws_secret="", content_queue_url='', search_queue_url='')

indexer.start_indexing()
