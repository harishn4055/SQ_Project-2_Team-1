import json
import boto3
import base64

def lambda_handler(event, context):
    # Initialize clients for S3 and SNS
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    
    # Extract the bucket name, file content, S3 key, and SNS topic ARN from the event
    bucket_name = event['bucket_name']
    file_content = event['file_content']  # Base64 encoded content
    s3_key = event['s3_key']  # S3 key (path in S3 where the file will be uploaded)
    sns_topic_arn = event['sns_topic_arn']  # SNS topic ARN
    
    try:
        # Decode the file content
        decoded_file_content = base64.b64decode(file_content)
        
        # Upload the file to S3
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=decoded_file_content)
        
        # Create a message for SNS notification
        message = f'File "{s3_key}" was uploaded to S3 bucket "{bucket_name}".'
        
        # Publish the message to the SNS topic
        sns.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject='New S3 File Uploaded'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('File uploaded successfully to S3 and SNS notification sent!')
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
