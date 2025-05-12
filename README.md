# Distributed Web Crawler and Indexer

This project implements a scalable, distributed web crawler and indexer system using AWS services. The system consists of multiple components that work together to crawl web pages, extract links, index content, and provide search functionality.

## System Architecture

The system is built using the following components:

- **Master Node**: Coordinates the entire system, managing crawler instances, monitoring task status, and handling client requests.
- **Crawler Nodes**: Distributed instances responsible for fetching web pages, extracting links, and sending data to the indexer.
- **Indexer Nodes**: Process and index the content retrieved by crawlers for search functionality.
- **Client Interface**: Allows users to interact with the system by submitting crawl requests and search queries.


### AWS Services Used

- **SQS (Simple Queue Service)**: Used for communication between components
- **DynamoDB**: Used for storing crawler state, task assignments, and system metadata
- **EC2**: Used for running the distributed crawler and indexer nodes
- **S3**: Used for storing crawled content

## Queues and Communication

The system uses various SQS queues for communication:

- **CrawlQueue**: URLs to be crawled
- **ReportQueue**: Results from crawlers back to master
- **IndexQueue**: Content to be indexed
- **RequestQueue**: Client requests
- **SearchQueue**: Search queries
- **ResponseQueue**: Responses to clients
- **FeedbackQueue**: Indexer feedback
- **DeadLetterQueue**: Failed tasks

## DynamoDB Tables

- **CrawlerHeartbeatTable**: Stores crawler node health status
- **IndexerHeartbeatTable**: Stores indexer node health status
- **CrawlerTaskAssignments**: Tracks URL assignments and statuses
- **IndexerTaskAssignments**: Tracks indexing tasks
- **BlockedUrlsTable**: Stores URLs blocked by robots.txt

## System Features

- **Fault Tolerance**: Automatically recovers from node failures
- **Dynamic Scaling**: Adjusts number of crawler instances based on workload
- **Configurable Crawl Depth**: Controls how deep to follow links
- **Politeness Policies**: Respects robots.txt and implements rate limiting
- **Error Handling**: Retries failed tasks and uses dead letter queues
- **Monitoring**: Provides comprehensive system metrics and status


## Configuration

The system is configured with the following parameters:

- `region_name`: AWS region 
- `max_depth`: Maximum crawl depth (default: 2)
- `TIMEOUT_SECONDS`: Timeout for tasks (default: 120 seconds)

## Monitoring

The master provides a dashboard with metrics including:

- Crawl rate and coverage
- Indexing rate and status
- Error rates
- Active crawler and indexer counts


# Setup

## 1. Install Dependencies

Clone the repository:

```bash
git clone https://github.com/your-username/web-crawler.git
cd web-crawler
```

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## 2. Configure AWS Services

Make sure you have an AWS account and the necessary services (SQS, DynamoDB, Elasticsearch, S3) set up.

- **SQS**: Create two queues: `request_queue` (for search queries) and `response_queue` (for search results and crawler status).
- **DynamoDB**: Create a table to store the indexer status.
- **Elasticsearch**: Set up an Elasticsearch domain for storing indexed documents.
- **S3**: Set up a bucket to store content that will be indexed.

Ensure your AWS credentials are set up using the AWS CLI or environment variables.

## 3. Set Up the Flask Application

Create a `.env` file in the root directory of the project to configure environment variables:

```env
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
AWS_REGION=us-west-2
SQS_REQUEST_QUEUE_URL=your-request-queue-url
SQS_RESPONSE_QUEUE_URL=your-response-queue-url
DYNAMODB_TABLE_NAME=your-dynamodb-table-name
S3_BUCKET_NAME=your-s3-bucket-name
ELASTICSEARCH_HOST=your-elasticsearch-host
```

## 4. Run the Application

Run the Flask development server:

```bash
python app.py
```

Visit `http://127.0.0.1:5000` in your browser.
