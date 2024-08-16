import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3', region_name='us-west-1')
bucket_name = 'user-log-data-bucket'

# Try to create the bucket
try:
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': 'us-west-1'}
    )
    print("Bucket created in us-west-1.")
except ClientError as e:
    if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        print("Bucket already exists in us-west-1. Uploading file.")
    else:
        raise e  # Re-raise the exception if it is not a 'BucketAlreadyOwnedByYou' error

# Code to upload a file to a specific folder within the bucket
file_name = 'enhanced_log_data.json'
folder_name = 'log_data'
try:
    response = s3.upload_file(file_name, bucket_name, f"{folder_name}/{file_name}")
    print("File uploaded successfully into the log_data folder.")
except ClientError as e:
    print(f"Failed to upload file: {e}")
