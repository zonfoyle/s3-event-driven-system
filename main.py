import json
import yaml
from utils import (
    get_clients,
    get_account_id,
    create_bucket,
    create_queue_with_dlq,
    create_topic,
    upload_file_to_s3,
    receive_message_from_queue,
    delete_message_from_queue,
    allow_s3_to_send_to_sqs,
    configure_s3_event_notification,
    publish_sns_message,
)


def load_config(config_path="config.yaml"):
    """Load project settings from config.yaml."""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def main():
    # Load project settings
    config = load_config()

    region = config["aws_region"]
    bucket_name = config["s3"]["bucket_name"]
    file_name = config["s3"]["file_to_upload"]

    queue_name = config["sqs"]["queue_name"]
    dlq_name = config["sqs"]["dlq_name"]
    max_receive_count = config["sqs"]["max_receive_count"]

    topic_name = config["sns"]["topic_name"]

    # Create AWS clients
    s3_client, sqs_client, sns_client = get_clients(region)
    account_id = get_account_id()

    # Create base resources
    create_bucket(s3_client, bucket_name, region)

    queue_url, queue_arn, dlq_url, dlq_arn = create_queue_with_dlq(
        sqs_client,
        queue_name,
        dlq_name,
        max_receive_count
    )

    topic_arn = create_topic(sns_client, topic_name)

    print(f"Queue URL: {queue_url}")
    print(f"Queue ARN: {queue_arn}")
    print(f"DLQ URL: {dlq_url}")
    print(f"DLQ ARN: {dlq_arn}")
    print(f"Topic ARN: {topic_arn}")

    # Allow S3 to send events to SQS
    allow_s3_to_send_to_sqs(
        sqs_client,
        queue_url,
        queue_arn,
        bucket_name,
        account_id
    )

    # Configure S3 event notifications
    configure_s3_event_notification(
        s3_client,
        bucket_name,
        queue_arn
    )

    # Upload file to trigger the event flow
    upload_file_to_s3(s3_client, bucket_name, file_name)

    print("Waiting for S3 event to arrive in SQS...")

    # Worker reads one message from the queue
    message = receive_message_from_queue(sqs_client, queue_url)

    if message:
        body = json.loads(message["Body"])
        print("Received raw S3 event message:")
        print(json.dumps(body, indent=2))

        # Handle S3 test event
        if body.get("Event") == "s3:TestEvent":
            print("Received S3 test event. Notification setup is working.")
            delete_message_from_queue(
                sqs_client,
                queue_url,
                message["ReceiptHandle"]
            )

        # Handle real S3 object-created event
        elif "Records" in body:
            record = body["Records"][0]
            event_bucket_name = record["s3"]["bucket"]["name"]
            object_key = record["s3"]["object"]["key"]

            print(f"Processing real S3 event for file: {object_key}")
            print(f"Bucket: {event_bucket_name}")

            # Simulate failure for sample.txt so SQS can retry it
            if object_key == "sample.txt":
                print("Simulated processing failure. Message will stay in queue for retry.")

                publish_sns_message(
                    sns_client,
                    topic_arn,
                    "File Processing Failed",
                    f"Processing failed for file '{object_key}' in bucket '{event_bucket_name}'. "
                    "The message was left in the queue for retry."
                )

            else:
                print("Processing succeeded.")

                publish_sns_message(
                    sns_client,
                    topic_arn,
                    "File Processing Succeeded",
                    f"Processing succeeded for file '{object_key}' in bucket '{event_bucket_name}'."
                )

                delete_message_from_queue(
                    sqs_client,
                    queue_url,
                    message["ReceiptHandle"]
                )
    else:
        print("No messages available in the queue right now.")


if __name__ == "__main__":
    main()