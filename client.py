from flask import Flask, render_template, request
import boto3
import json
import time
import uuid
import logging
from botocore.exceptions import ClientError


class Client:
    def __init__(self, request_queue_url, response_queue_url, region='us-east-1', timeout=10):
        self.request_queue_url = request_queue_url
        self.response_queue_url = response_queue_url
        self.region = region
        self.timeout = timeout
        self.client_id = f"client_{uuid.uuid4()}"
        self.last_crawl_count = 0
        self.last_crawl_time = time.time()
        self.last_indexed_count = 0
        self.last_indexed_time = time.time()

        try:
            self.sqs = boto3.client('sqs', region_name=self.region)
        except ClientError as e:
            raise Exception(f"Failed to initialize SQS client: {str(e)}")

    def send_search_query(self, query, mode="and"):
        message = {
            "type": "search",
            "query": query,
            "url": None,
            "mode": mode,
            "depth": None,
            "max_depth": None,
            "client_id": self.client_id 
        }
        self.sqs.send_message(
            QueueUrl=self.request_queue_url,
            MessageBody=json.dumps(message)
        )

    def receive_search_results(self):
        start = time.time()
        # Poll response queue for result
        while time.time() - start < self.timeout:
            try:
                response = self.sqs.receive_message(
                    QueueUrl=self.response_queue_url,
                    MaxNumberOfMessages=1,  # Get one message at a time
                    WaitTimeSeconds=2,
                    VisibilityTimeout=30  #!Set visibility timeout not sure to prevent other clients from seeing the message
                )
                messages = response.get('Messages', [])
                if messages:
                    message = messages[0]
                    body = json.loads(message['Body'])
                    # Check if this result belongs to this client
                    if (body.get('client_id') == self.client_id):
                        try:
                            # Try to delete the message
                            self.sqs.delete_message(
                                QueueUrl=self.response_queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            return body
                        except Exception as e:
                            # If delete fails, message will return to queue after visibility timeout
                            logging.error(f"Failed to delete message: {str(e)}")
                            continue
                    else:
                        # If not our message, put it back in the queue immediately
                        self.sqs.change_message_visibility(
                            QueueUrl=self.response_queue_url,
                            ReceiptHandle=message['ReceiptHandle'],
                            VisibilityTimeout=0
                        )
            except Exception as e:
                logging.error(f"Error receiving message: {str(e)}")
                time.sleep(1)  # Add delay before retrying
        return []

    def submit_seed_urls(self, seed_urls, max_depth=2, domain=None):
        for url in seed_urls:
            message = {
                "type": "crawl",
                "query": None,
                "url": url,
                "depth": 0,
                "max_depth": max_depth,
                "domain": domain,
                "client_id": self.client_id
            }
            self.sqs.send_message(
                QueueUrl=self.request_queue_url,
                MessageBody=json.dumps(message)
            )

    def receive_search_results(self):
        start = time.time()
        while time.time() - start < self.timeout:
            response = self.sqs.receive_message(
                QueueUrl=self.response_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=2,
                VisibilityTimeout=30
            )
            messages = response.get('Messages', [])
            if messages:
                message = messages[0]
                body = json.loads(message['Body'])

                if body.get('client_id') == self.client_id:
                    try:
                        self.sqs.delete_message(
                            QueueUrl=self.response_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                    except Exception as e:
                        logging.error(f"Failed to delete message: {str(e)}")

                    return body.get("results", [])
                else:
                    self.sqs.change_message_visibility(
                        QueueUrl=self.response_queue_url,
                        ReceiptHandle=message['ReceiptHandle'],
                        VisibilityTimeout=0
                    )
        return []

    def count_active_crawlers(self):
        response = self.dynamodb.Table('CrawlerHeartbeatTable').scan()
        return sum(1 for item in response['Items'] if item.get('status') == 'running')

    def count_crawled_urls(self):
        response = self.dynamodb.Table('CrawlerTaskAssignmets').scan()
        return sum(1 for item in response['Items'] if item['status'] == 'done')

    def get_crawl_rate(self):
        now = time.time()
        elapsed = now - self.last_crawl_time
        if elapsed > 0:
            current_count = self.count_crawled_urls()
            rate = (current_count - self.last_crawl_count) / elapsed
            self.last_crawl_count = current_count
            self.last_crawl_time = now
            return round(rate, 2)
        return 0

    def get_indexing_rate(self):
        response = self.dynamodb.Table('IndexerTaskAssignments').scan()
        indexed = sum(1 for item in response['Items'] if item['status'] == 'index_success')
        now = time.time()
        elapsed = now - self.last_indexed_time
        if elapsed > 0:
            rate = (indexed - self.last_indexed_count) / elapsed
            self.last_indexed_count = indexed
            self.last_indexed_time = now
            return round(rate, 2)
        return 0

    def get_error_rates(self):
        response = self.dynamodb.Table('IndexerTaskAssignments').scan()
        items = response['Items']
        
        index_success = sum(1 for item in items if item['status'] == 'index_success')
        index_failed = sum(1 for item in items if item['status'] == 'index_failed')
        search_success = sum(1 for item in items if item['status'] == 'search_success')
        search_failed = sum(1 for item in items if item['status'] == 'search_failed')

        total_index = index_success + index_failed
        total_search = search_success + search_failed

        return {
            'index_error_rate': round((index_failed / total_index * 100), 2) if total_index > 0 else 0,
            'search_error_rate': round((search_failed / total_search * 100), 2) if total_search > 0 else 0,
            'total_indexed': index_success
        }

    def get_crawler_status(self):
        response = self.dynamodb.Table('CrawlerHeartbeatTable').scan()
        return {
            'active': sum(1 for item in response['Items'] if item.get('status') == 'running'),
            'failed': sum(1 for item in response['Items'] if item.get('status') == 'failed'),
            'shutdown': sum(1 for item in response['Items'] if item.get('status') == 'shutdown')
        }

    def get_queue_status(self):
        response = self.sqs.get_queue_attributes(
            QueueUrl=self.request_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(response['Attributes']['ApproximateNumberOfMessages'])


client = Client("https://sqs.us-east-1.amazonaws.com/138749495090/RequestQueue", 
                "https://sqs.us-east-1.amazonaws.com/138749495090/ResponseQueue")


# Flask app setup
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    query = request.form['query']
    mode = request.form.get('search_mode', 'and')
    client.send_search_query(query, mode)
    results = client.receive_search_results()
    if not results:
        message = "No results found or request timed out."
    else:
        message = f"{len(results)} result(s) found."
    return render_template('index.html', query=query, results=results, message=message, selected_mode=mode)



@app.route('/crawl', methods=['POST'])
def crawl():
    seed_urls = request.form['seeds'].splitlines()
    depth = int(request.form.get('depth', 2))
    domain = request.form.get('domain', None)
    client.submit_seed_urls(seed_urls, max_depth=depth, domain=domain)
    message = f"{len(seed_urls)} seed URL(s) submitted for crawling."
    return render_template('index.html', message=message, seed_urls=seed_urls)

@app.route('/dashboard')
def dashboard():
    # Get monitoring information
    stats = {
        'active_crawlers': client.count_active_crawlers(),
        'crawled_urls': client.count_crawled_urls(),
        'crawl_rate': client.get_crawl_rate(),
        'indexing_rate': client.get_indexing_rate(),
        'error_rates': client.get_error_rates(),
        'crawler_status': client.get_crawler_status(),
        'queue_status': client.get_queue_status()
    }
    return render_template('dashboard.html', stats=stats)


if __name__ == '__main__':
    app.run(debug=True)
