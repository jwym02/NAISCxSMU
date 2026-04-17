import os
import boto3

dynamo_client = boto3.client(
    "dynamodb",
    endpoint_url=os.getenv("DYNAMODB_ENDPOINT", "http://dynamodb-local:8000"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
)