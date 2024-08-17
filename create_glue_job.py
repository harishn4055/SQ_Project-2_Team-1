import boto3

glue = boto3.client('glue')

response = glue.create_job(
    Name='LogDataETLJob',
    Role='arn:aws:iam::637423377183:role/proj5role',  
    Command={
        'Name': 'glueetl',
        'ScriptLocation': f's3://user-log-data-bucket/scripts/etl_script.py',
        'PythonVersion': '3'
    },
    DefaultArguments={
        '--job-bookmark-option': 'job-bookmark-enable',
    },
    MaxRetries=1
)

print("Glue ETL job 'LogDataETLJob' created successfully.")
