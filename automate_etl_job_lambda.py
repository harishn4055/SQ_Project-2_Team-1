import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    glue = boto3.client('glue')
    job_name = 'LogDataETLJob' 
    try:
        # Start the Glue job
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        print(f"Started Glue job {job_name} with job run ID: {job_run_id}")
        return {
            'statusCode': 200,
            'body': f"Glue job {job_name} started. Job run ID: {job_run_id}"
        }
    except ClientError as e:
        print(f"Error starting Glue job: {e}")
        return {
            'statusCode': 500,
            'body': f"Error starting Glue job: {e}"
        }
