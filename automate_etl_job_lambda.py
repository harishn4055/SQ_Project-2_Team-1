import boto3
from datetime import datetime

def lambda_handler(event, context):
    # Configuration
    bucket_name = 'user-log-data-bucket'  
    folder_name = 'log_data/'             
    job_name = 'LogDataETLJob'            
    glue = boto3.client('glue')
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ProcessedFiles')  # DynamoDB table name for processed files

    # List files in the specified folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    files = response.get('Contents', [])
    
    # Retrieve already processed files from DynamoDB
    processed_files = set()
    try:
        dynamo_response = table.scan()
        for item in dynamo_response['Items']:
            processed_files.add(item['file_name'])  # 'file_name' is the partition key in DynamoDB
    except Exception as e:
        print(f"Error retrieving processed files from DynamoDB: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error retrieving processed files: {str(e)}"
        }

    # Find new (unprocessed) files
    new_files = []
    for file in files:
        file_name = file['Key']
        if file_name not in processed_files:
            new_files.append(file_name)

    # Check if there are at least 5 new files
    if len(new_files) >= 5:
        try:
            # Start Glue job
            response = glue.start_job_run(JobName=job_name)
            job_run_id = response['JobRunId']
            print(f"Started Glue job {job_name} with job run ID: {job_run_id}")
            
            # Mark the new files as processed in DynamoDB
            with table.batch_writer() as batch:
                for file_name in new_files:
                    batch.put_item(Item={'file_name': file_name})
            
            return {
                'statusCode': 200,
                'body': f"Glue job {job_name} started for 5 new files. Job run ID: {job_run_id}"
            }
        except Exception as e:
            print(f"Error starting Glue job: {str(e)}")
            return {
                'statusCode': 500,
                'body': f"Error starting Glue job: {str(e)}"
            }
    else:
        return {
            'statusCode': 200,
            'body': "Not enough new files to start Glue job."
        }
