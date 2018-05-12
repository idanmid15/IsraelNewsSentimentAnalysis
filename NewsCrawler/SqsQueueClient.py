import boto3

# Create SQS client
sqs = boto3.client(
    'sqs',
    aws_access_key_id="***",
    aws_secret_access_key="***",
    region_name='us-west-2'
)

queue_url = 'https://sqs.us-west-2.amazonaws.com/744087497734/NewsWebsitesQueue'

def receive_next_url():

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=2
    )
    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    return message['Body'], receipt_handle


def delete_received_url(receipt_handle):
    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
