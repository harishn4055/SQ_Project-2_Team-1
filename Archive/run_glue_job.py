import boto3

glue = boto3.client('glue')

glue.start_job_run(JobName='LogDataETLJob')

print("Glue ETL job 'LogDataETLJob' started successfully.")
