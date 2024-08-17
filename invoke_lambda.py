import boto3
import base64
import json

# Initialize the Lambda client
lambda_client = boto3.client('lambda')

# Function to read and encode a file
def encode_file(file_path):
    with open(file_path, 'rb') as file:
        file_content = file.read()
        return base64.b64encode(file_content).decode('utf-8')

# List of files to upload
files_to_upload = [
    {
        'file_path': 'log_data_2019.json',
        's3_key': 'log_data/log_data_2019.json'
    },
    {
        'file_path': 'log_data_2020.json',
        's3_key': 'log_data/log_data_2020.json'
    },
    {
        'file_path': 'log_data_2021.json',
        's3_key': 'log_data/log_data_2021.json'
    },
    {
        'file_path': 'log_data_2022.json',
        's3_key': 'log_data/log_data_2022.json'
    },
    {
        'file_path': 'log_data_2023.json',
        's3_key': 'log_data/log_data_2023.json'
    }

]

# Encode the content for each file and prepare the list for the payload
files_payload = []
for file in files_to_upload:
    encoded_content = encode_file(file['file_path'])
    files_payload.append({
        'file_content': encoded_content,
        's3_key': file['s3_key']
    })

# Define the payload for Lambda, including the list of files
payload = {
    'bucket_name': 'user-log-data-bucket',
    'sns_topic_arn': 'arn:aws:sns:us-west-1:637423377183:SecurityAlertsTopic',
    'files': files_payload  
}

# Invoke the Lambda function
response = lambda_client.invoke(
    FunctionName='LambdaIngestionFunction',
    InvocationType='RequestResponse',
    Payload=json.dumps(payload)
)

# Print the response from Lambda
print(response['Payload'].read().decode('utf-8'))
