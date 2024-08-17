import json
import boto3
import base64

def lambda_handler(event, context):
    # Initialize clients for S3 and SNS
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    
    # Extract the bucket name and SNS topic ARN from the event
    bucket_name = event['bucket_name']
    sns_topic_arn = event['sns_topic_arn']  
    
    files = event['files'] 
    
    upload_status = []
    
    try:
        for file in files:
            # Extract file content and S3 key for each file
            file_content = file['file_content']  
            s3_key = file['s3_key']  
            
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
            
            # Log the success for this file
            upload_status.append({
                'file': s3_key,
                'status': 'Success',
                'message': message
            })
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Files uploaded successfully to S3 and SNS notifications sent!',
                'status': upload_status
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error occurred while uploading files',
                'error': str(e),
                'status': upload_status
            })
        }
