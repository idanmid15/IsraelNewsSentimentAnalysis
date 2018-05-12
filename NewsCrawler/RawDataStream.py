import boto3
import hashlib
from threading import Thread
stream_name = "RawDataQueue"
# Create Kinesis client
kinesis_client = boto3.client(
    'kinesis',
    aws_access_key_id="****",
    aws_secret_access_key="****",
    region_name='us-west-2'
)


def send_raw_text(url, text, pool):
    hash_object = hashlib.md5(url.encode('base64'))
    hex_digest = hash_object.hexdigest()
    pool.submit(send_request, text, hex_digest)



def send_request(data, partition_key):
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=data,
        PartitionKey=partition_key
    )
