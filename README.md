# Distributed Web Crawling and Indexing System

This project is a cloud-based distributed web crawling and indexing system 

## Overview

The system distributes web crawling and indexing tasks across multiple cloud-based virtual machines to efficiently collect and process large volumes of web data. It emphasizes scalability, fault tolerance, and respect for web crawling policies.

## Features

- **Distributed Crawling**: Crawling tasks are spread across multiple worker nodes.
- **Indexing**: Extracted content is indexed to support basic keyword search.
- **Scalability**: System can scale horizontally by adding more crawler/indexer nodes.
- **Fault Tolerance**: Includes heartbeat monitoring and task reassignment for failed nodes.
- **Cloud Integration**: Designed to work with AWS services (EC2, S3, and SQS).
- **Search Interface**: Basic CLI/web interface for querying indexed data.

## System Components

- **Master Node**: Controls task distribution, monitors node status, and handles failures.
- **Crawler Nodes**: Fetch web pages and extract links and text content.
- **Indexer Nodes**: Build and store the search index.
- **Task Queue**: Message queue system (e.g., AWS SQS) for distributing work.
- **Persistent Storage**: Cloud-based storage (e.g., AWS S3) for crawled content and index data.

## Technologies Used

- Python
- `mpi4py` for distributed process communication
- `requests`, `BeautifulSoup` for web crawling
- `Whoosh` (or Elasticsearch) for indexing and search
- AWS EC2, S3, SQS (or equivalent cloud services)

## Getting Started

### Prerequisites

- Python 3.x
- `mpi4py`, `requests`, `beautifulsoup4`, `whoosh`
- An MPI implementation (Open MPI or MPICH)
- AWS CLI setup (if deploying to AWS)

### Running Locally Using MPI

```bash
pip install mpi4py requests beautifulsoup4 whoosh
mpiexec -n 4 python master_node.py
