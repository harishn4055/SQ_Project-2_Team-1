import boto3
import uuid
# Initialize boto3 clients
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Configuration Parameters
bucket_name = 'user-log-data-bucket'
lambda_function_arn = 'arn:aws:lambda:us-west-1:637423377183:function:InitiateGlueJob'
statement_id = str(uuid.uuid4())
source_arn = f'arn:aws:s3:::{bucket_name}'  # ARN of the S3 bucket

def add_lambda_permission():
    """Add permission for S3 to invoke the Lambda function."""
    try:
        response = lambda_client.add_permission(
            FunctionName=lambda_function_arn,
            Principal='s3.amazonaws.com',
            StatementId=statement_id,
            Action='lambda:InvokeFunction',
            SourceArn=source_arn
        )
        print('Lambda permission added:', response)
    except Exception as e:
        print('Error adding permission:', str(e))

def configure_s3_notification():
    """Configure S3 bucket notification to trigger Lambda."""
    notification_configuration = {
        "LambdaFunctionConfigurations": [
            {
                "LambdaFunctionArn": lambda_function_arn,
                "Events": ["s3:ObjectCreated:*"]
            }
        ]
    }
    
    try:
        response = s3_client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification_configuration
        )
        print('S3 bucket notification configuration set:', response)
    except Exception as e:
        print('Error configuring S3 notification:', str(e))

if __name__ == '__main__':
    # Run the functions to set up the configuration
    add_lambda_permission()
    configure_s3_notification()
