from flask import Flask, render_template, request
import boto3
import json
import time
from botocore.exceptions import ClientError


class Client:
    def __init__(self, request_queue_url, response_queue_url, region='us-east-1', timeout=10):
        self.request_queue_url = request_queue_url
        self.response_queue_url = response_queue_url
        self.region = region
        self.timeout = timeout
        self.client_id = f"client_{int(time.time())}"

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
            response = self.sqs.receive_message(
                QueueUrl=self.response_queue_url,
                MaxNumberOfMessages=1,  # Get one message at a time
                WaitTimeSeconds=2
            )
            messages = response.get('Messages', [])
            if messages:
                message = messages[0]
                body = json.loads(message['Body'])
                # Check if this result belongs to this client
                if body.get('client_id') == self.client_id:
                    # Delete the message we found
                    self.sqs.delete_message(
                        QueueUrl=self.response_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    return body
                else:
                    # If not our message, put it back in the queue immediately
                    self.sqs.change_message_visibility(
                        QueueUrl=self.response_queue_url,
                        ReceiptHandle=message['ReceiptHandle'],
                        VisibilityTimeout=0
                    )
        return []

    def submit_seed_urls(self, seed_urls, max_depth=2, domain=None):
        for url in seed_urls:
            message = {
                "type": "crawl",
                "query": None,
                "url": url,
                "depth": 0,
                "max_depth": max_depth,
                "domain": domain
            }
            self.sqs.send_message(
                QueueUrl=self.request_queue_url,
                MessageBody=json.dumps(message)
            )



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
    return render_template('index.html', query=query, results=results, selected_mode=mode)

@app.route('/crawl', methods=['POST'])
def crawl():
    seed_urls = request.form['seeds'].splitlines()
    depth = int(request.form.get('depth', 2))
    domain = request.form.get('domain', None)
    client.submit_seed_urls(seed_urls, max_depth=depth, domain=domain)
    return render_template('index.html', message="Seed URLs submitted!", seed_urls=seed_urls)

if __name__ == '__main__':
    app.run(debug=True)
