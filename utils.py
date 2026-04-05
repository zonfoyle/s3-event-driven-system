import boto3
import json


def get_clients(region):
    """Create AWS service clients for this project."""
    s3_client = boto3.client("s3", region_name=region)
    sqs_client = boto3.client("sqs", region_name=region)
    sns_client = boto3.client("sns", region_name=region)

    return s3_client, sqs_client, sns_client


def create_bucket(s3_client, bucket_name, region):
    """Create an S3 bucket if it does not already exist."""
    existing_buckets = s3_client.list_buckets()["Buckets"]

    # Reuse the bucket if it already exists so the script is safe to rerun.
    for bucket in existing_buckets:
        if bucket["Name"] == bucket_name:
            print(f"Using existing bucket: {bucket_name}")
            return bucket_name

    # us-east-1 uses a slightly different bucket creation format.
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region}
        )

    print(f"Created bucket: {bucket_name}")
    return bucket_name


def create_queue(sqs_client, queue_name):
    """Create an SQS queue and return its URL and ARN."""
    response = sqs_client.create_queue(QueueName=queue_name)
    queue_url = response["QueueUrl"]

    attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )

    queue_arn = attributes["Attributes"]["QueueArn"]

    print(f"Created or reused queue: {queue_name}")
    return queue_url, queue_arn


def create_topic(sns_client, topic_name):
    """Create an SNS topic and return its ARN."""
    response = sns_client.create_topic(Name=topic_name)
    topic_arn = response["TopicArn"]

    print(f"Created or reused topic: {topic_name}")
    return topic_arn

def upload_file_to_s3(s3_client, bucket_name, file_name):
    """Upload a local file to the S3 bucket."""
    s3_client.upload_file(file_name, bucket_name, file_name)
    print(f"Uploaded {file_name} to bucket: {bucket_name}")


def send_message_to_queue(sqs_client, queue_url, bucket_name, file_name):
    """Send a processing message to the SQS queue."""
    message_body = {
        "bucket_name": bucket_name,
        "file_name": file_name,
        "action": "process_file"
    }

    import json
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_body)
    )

    print(f"Sent message to queue for file: {file_name}")


def receive_message_from_queue(sqs_client, queue_url):
    """Receive one message from the SQS queue."""
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=2
    )

    messages = response.get("Messages", [])

    if not messages:
        print("No messages in queue.")
        return None

    message = messages[0]
    print("Received message from queue.")
    return message


def delete_message_from_queue(sqs_client, queue_url, receipt_handle):
    """Delete a processed message from the SQS queue."""
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    print("Deleted message from queue.")


    import json


def allow_s3_to_send_to_sqs(sqs_client, queue_url, queue_arn, bucket_name, account_id):
    """Allow this S3 bucket to send messages to the SQS queue."""
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowS3SendMessage",
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:::{bucket_name}"
                    },
                    "StringEquals": {
                        "aws:SourceAccount": account_id
                    }
                }
            }
        ]
    }

    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "Policy": json.dumps(policy)
        }
    )

    print("Updated SQS queue policy to allow S3 events.")

def configure_s3_event_notification(s3_client, bucket_name, queue_arn):
    """Configure S3 to send object-created events to the SQS queue."""
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"]
                }
            ]
        }
    )

    print("Configured S3 bucket notification to send events to SQS.")

def get_account_id():
    """Return the current AWS account ID."""
    sts_client = boto3.client("sts")
    return sts_client.get_caller_identity()["Account"]

def create_queue_with_dlq(sqs_client, queue_name, dlq_name, max_receive_count):
    """Create a main queue with a dead-letter queue attached."""
    # Create or reuse DLQ
    dlq_response = sqs_client.create_queue(QueueName=dlq_name)
    dlq_url = dlq_response["QueueUrl"]

    dlq_attributes = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["QueueArn"]
    )
    dlq_arn = dlq_attributes["Attributes"]["QueueArn"]

    print(f"Created or reused DLQ: {dlq_name}")

    # Configure main queue redrive policy
    redrive_policy = {
        "deadLetterTargetArn": dlq_arn,
        "maxReceiveCount": str(max_receive_count)
    }

    main_response = sqs_client.create_queue(
        QueueName=queue_name,
        Attributes={
            "RedrivePolicy": json.dumps(redrive_policy)
        }
    )
    queue_url = main_response["QueueUrl"]

    main_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )
    queue_arn = main_attributes["Attributes"]["QueueArn"]

    print(f"Created or reused main queue: {queue_name}")
    return queue_url, queue_arn, dlq_url, dlq_arn

def publish_sns_message(sns_client, topic_arn, subject, message):
    """Publish a message to an SNS topic."""
    sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message
    )
    print(f"Published SNS notification: {subject}")