from flask import Flask, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import boto3
import json
import time
import uuid
import logging
from botocore.exceptions import ClientError
from werkzeug.security import generate_password_hash, check_password_hash


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

    def request_monitoring_data(self):
        """Request monitoring data from master node via SQS"""
        message = {
            "type": "monitoring_request",
            "client_id": self.client_id
        }
        self.sqs.send_message(
            QueueUrl=self.request_queue_url,
            MessageBody=json.dumps(message)
        )
        
        # Wait for response
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
                if body.get('client_id') == self.client_id and body.get('type') == 'monitoring_data':
                    try:
                        self.sqs.delete_message(
                            QueueUrl=self.response_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        return body.get('data', {})
                    except Exception as e:
                        logging.error(f"Failed to delete message: {str(e)}")
                else:
                    self.sqs.change_message_visibility(
                        QueueUrl=self.response_queue_url,
                        ReceiptHandle=message['ReceiptHandle'],
                        VisibilityTimeout=0
                    )
        return {}

    def count_active_crawlers(self):
        raise NotImplementedError("Use request_monitoring_data() instead")

    def count_crawled_urls(self):
        raise NotImplementedError("Use request_monitoring_data() instead")

    def get_crawl_rate(self):
        raise NotImplementedError("Use request_monitoring_data() instead")

    def get_indexing_rate(self):
        raise NotImplementedError("Use request_monitoring_data() instead")

    def get_error_rates(self):
        raise NotImplementedError("Use request_monitoring_data() instead")

    def get_crawler_status(self):
        raise NotImplementedError("Use request_monitoring_data() instead")

    def get_queue_status(self):
        raise NotImplementedError("Use request_monitoring_data() instead")


client = Client("https://sqs.us-east-1.amazonaws.com/353176954707/RequestQueue", 
                "https://sqs.us-east-1.amazonaws.com/353176954707/ResponseQueue")


# Flask app setup
app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this to a secure secret key

# Flask-Login setup
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Simple user model
class User(UserMixin):
    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

# In-memory user storage (replace with database in production)
users = {
    'admin': User('1', 'admin', generate_password_hash('admin123'))  # Change this password
}

@login_manager.user_loader
def load_user(user_id):
    for user in users.values():
        if user.id == user_id:
            return user
    return None

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = users.get(username)
        
        if user and check_password_hash(user.password_hash, password):
            login_user(user)
            next_page = request.args.get('next')
            return redirect(next_page or url_for('dashboard'))
        flash('Invalid username or password')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

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
@login_required
def dashboard():
    # Get monitoring information via SQS
    monitoring_data = client.request_monitoring_data()
    if not monitoring_data:
        monitoring_data = {
            'active_crawlers': 0,
            'crawled_urls': 0,
            'crawl_rate': 0,
            'indexing_rate': 0,
            'error_rates': {
                'index_error_rate': 0,
                'search_error_rate': 0,
                'total_indexed': 0
            },
            'crawler_status': {
                'active': 0,
                'failed': 0,
                'shutdown': 0
            },
            'queue_status': 0
        }
    return render_template('dashboard.html', stats=monitoring_data)


if __name__ == '__main__':
    app.run(host = '0.0.0.0', port = 5000, debug=True)
