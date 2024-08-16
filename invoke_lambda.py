import boto3
import base64
import json

# Initialize the Lambda client
lambda_client = boto3.client('lambda')

# Read and encode the file content
with open('enhanced_log_data.json', 'rb') as file:
    file_content = file.read()
    encoded_file_content = base64.b64encode(file_content).decode('utf-8')

# Define the payload
payload = {
    'bucket_name': 'user-log-data-bucket',
    'file_content': encoded_file_content,
    's3_key': 'log_data/enhanced_log_data.json',
    'sns_topic_arn': 'arn:aws:sns:us-west-1:637423377183:SecurityAlertsTopic'
}

# Invoke the Lambda function
response = lambda_client.invoke(
    FunctionName='LambdaIngestionFunction',
    InvocationType='RequestResponse',
    Payload=json.dumps(payload)
)

# Print the response
print(response['Payload'].read().decode('utf-8'))
