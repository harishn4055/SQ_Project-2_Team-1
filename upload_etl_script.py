import boto3

s3 = boto3.client('s3')

bucket_name = 'user-log-data-bucket'  

s3.upload_file("etl_script.py", bucket_name, "scripts/etl_script.py")

print("ETL script uploaded successfully.")
