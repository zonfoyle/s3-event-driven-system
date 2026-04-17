# Event-Driven File Processing System

This project demonstrates an event-driven AWS workflow using S3, SQS, SNS, and Python.

The goal was to build a loosely coupled file-processing system where uploads to S3 trigger asynchronous processing through a queue.

## Overview

The workflow looks like this:

1. A file is uploaded to Amazon S3
2. S3 sends an event notification to Amazon SQS
3. A Python worker reads the message from the queue
4. The worker processes the file
5. SNS sends a notification about the result
6. Failed messages are retried automatically
7. Messages that continue to fail are moved to a Dead Letter Queue (DLQ)

## Architecture

```text
S3 Bucket
    ↓
S3 Event Notification
    ↓
SQS Main Queue
    ↓
Python Worker
    ↓
SNS Notification

Failed messages
    ↓
Dead Letter Queue (DLQ)
```

You can also include your architecture diagram here:

```markdown
![Event-Driven Architecture](architecture.png)
```

## Design Decisions

- Used S3 event notifications so file uploads automatically trigger downstream processing
- Used SQS to separate file uploads from processing, making the system more scalable and resilient
- Used SNS to publish processing results so other systems could subscribe later
- Added a Dead Letter Queue to safely capture messages that repeatedly fail
- Simulated failures with a test file to demonstrate retry behavior and DLQ handling

## Tradeoffs

- SQS improves reliability and decoupling, but adds more components and configuration
- SNS makes it easier to notify other systems, but is not necessary for a very small workflow
- Retries improve fault tolerance, but can delay failure detection if retry settings are too high
- Using a DLQ prevents bad messages from blocking the queue, but requires additional monitoring

## Features

- S3 bucket creation
- SQS queue and Dead Letter Queue creation
- SNS topic creation
- S3 → SQS event integration
- Python worker for queue polling
- Automatic retry behavior
- SNS notifications for success and failure
- Dead Letter Queue handling

## Tech Stack

- Python
- boto3
- PyYAML
- AWS S3
- AWS SQS
- AWS SNS

## Running the Project

```bash
aws configure
pip install -r requirements.txt
python main.py
```

## Example Workflow

```text
Uploaded: sample.txt
↓
S3 sends event to SQS
↓
Python worker receives message
↓
Processing fails
↓
Message retried by SQS
↓
After max retries, message moves to DLQ
```

## What This Project Demonstrates

- Event-driven architecture
- Asynchronous processing
- Decoupled cloud services
- Queue-based retry behavior
- Dead Letter Queue handling
- AWS messaging services with Python

## Future Improvements

- Add Lambda support instead of a long-running Python worker
- Add CloudWatch monitoring and alarms
- Store processing results in DynamoDB
- Add support for multiple file types
- Containerize the worker with Docker
