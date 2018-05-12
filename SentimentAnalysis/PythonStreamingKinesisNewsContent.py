import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from textblob import TextBlob
import boto3
from random import randint
import hashlib


def upload_records_step(rdd, aws_result_bucket, region_name, aws_key, aws_secret):
    rdd.foreach(lambda x: send_to_s3(x, aws_result_bucket, region_name, aws_key, aws_secret))


def send_to_s3(israel_negative_sentences, aws_result_bucket, region_name, aws_key, aws_secret):
    if israel_negative_sentences:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=region_name
        )
        for url, sentence in israel_negative_sentences:
            # Urls may contain more than one result, we save all of them by appending this int
            url_for_hex = url + ", " + str(randint(0, 100000))
            url_with_data = url + ", " + sentence
            hash_object = hashlib.md5(url_for_hex)
            hex_digest = hash_object.hexdigest()
            s3_client.put_object(Body=url_with_data,
                                 Bucket=aws_result_bucket,
                                 Key=hex_digest)


def tokenize_text(str):
    index_seperating_url_text = str.index(", ")
    url = str[:index_seperating_url_text]
    text = str[index_seperating_url_text + 2:]
    import nltk
    if not nltk.data.path.__contains__('/home/hadoop/nltk_data'):
        nltk.data.path.append('/home/hadoop/nltk_data')
    tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    return [(url, sentence) for sentence in tokenizer.tokenize(text)]


def analyze_sentence(url_sentence_tuple):
    url, input_sentence = url_sentence_tuple
    import nltk
    if not nltk.data.path.__contains__('/home/hadoop/nltk_data'):
        nltk.data.path.append('/home/hadoop/nltk_data')
    sentence_analyzed = TextBlob(input_sentence)
    israel_negative_sentences = []
    for sentence in sentence_analyzed.sentences:
        lower_string_sentence = sentence.string.lower()
        if ("israel" in lower_string_sentence or "zionism" in lower_string_sentence) \
                and sentence.sentiment.polarity <= -0.2:
            israel_negative_sentences.append((url, sentence.string))

    return israel_negative_sentences


if __name__ == '__main__':
    if len(sys.argv) != 10:
        print("Usage: <app-name> <stream-name> <endpoint-url> <region-name> <aws-result-bucket> <aws-access-key> <aws-secret-key>")
        sys.exit(-1)

    app_Name, streamName, end_point_url, region_name, aws_result_bucket, kinesis_key, kinesis_secret, bucket_key, bucket_secret = sys.argv[1:]
    sparkContext = SparkContext(appName=app_Name)
    streamingContext = StreamingContext(sparkContext, 2)
    dstream = KinesisUtils.createStream(streamingContext, app_Name, streamName, end_point_url, region_name,
                                        InitialPositionInStream.TRIM_HORIZON, 10, awsAccessKeyId=kinesis_key,
                                        awsSecretKey=kinesis_secret)
    dstream\
        .flatMap(tokenize_text)\
        .map(analyze_sentence)\
        .foreachRDD(lambda x: upload_records_step(x, aws_result_bucket, region_name, bucket_key, bucket_secret))
    streamingContext.start()
    streamingContext.awaitTermination()
