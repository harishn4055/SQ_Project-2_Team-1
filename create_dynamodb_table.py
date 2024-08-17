import boto3
from botocore.exceptions import ClientError

# Initialize a DynamoDB client using boto3
dynamodb = boto3.client('dynamodb')

def create_table():
    try:
        # Create a DynamoDB table with the name 'ProcessedFiles'
        response = dynamodb.create_table(
            TableName='ProcessedFiles',
            KeySchema=[
                {
                    'AttributeName': 'file_name',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'file_name',
                    'AttributeType': 'S'  # String
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Table created successfully!")
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("Table already exists.")
        else:
            print(f"Unexpected error: {e}")

def insert_processed_file(file_name):
    # Initialize DynamoDB resource for inserting data
    dynamodb_resource = boto3.resource('dynamodb')
    table = dynamodb_resource.Table('ProcessedFiles')
    
    try:
        # Insert a processed file entry into the DynamoDB table
        response = table.put_item(
            Item={
                'file_name': file_name
            }
        )
        print(f"File {file_name} inserted into DynamoDB.")
        return response
    except ClientError as e:
        print(f"Error inserting file {file_name}: {e}")

def insert_multiple_processed_files(file_list):
    for file_name in file_list:
        insert_processed_file(file_name)

# Main execution
if __name__ == "__main__":
    # Create the DynamoDB table
    create_table()
    