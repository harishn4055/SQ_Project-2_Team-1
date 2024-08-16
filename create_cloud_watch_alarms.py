import boto3

# Initialize boto3 clients
cloudwatch = boto3.client('cloudwatch')
sns_arn = 'arn:aws:sns:us-west-1:637423377183:SecurityAlertsTopic'  # Replace with your SNS Topic ARN

# CloudWatch Alarm for Glue Job Failures
def create_glue_failure_alarm():
    response = cloudwatch.put_metric_alarm(
        AlarmName='GlueJobFailureAlarm',
        MetricName='GlueJobRunFailures',
        Namespace='AWS/Glue',
        Statistic='Sum',
        Dimensions=[
            {
                'Name': 'JobName',
                'Value': 'LogDataETLJob'  # Replace with your Glue job name
            },
        ],
        Period=300,  # 5-minute period
        EvaluationPeriods=1,
        Threshold=1,  # Alarm on any failure
        ComparisonOperator='GreaterThanOrEqualToThreshold',
        AlarmActions=[
            sns_arn  # Send SNS notification when alarm is triggered
        ],
    )
    print("Glue Job Failure Alarm created successfully.")

# CloudWatch Alarm for High Volume of Security-Related Events (Authentication Failures)
def create_authentication_failure_alarm():
    response = cloudwatch.put_metric_alarm(
        AlarmName='HighAuthenticationFailures',
        MetricName='AuthenticationFailures',  # Assuming you're tracking this metric
        Namespace='SecurityMonitoring',
        Statistic='Sum',
        Period=300,  # 5-minute period
        EvaluationPeriods=1,
        Threshold=5,  # Set threshold based on your requirements
        ComparisonOperator='GreaterThanOrEqualToThreshold',
        AlarmActions=[
            sns_arn  # Send SNS notification when alarm is triggered
        ],
    )
    print("Authentication Failure Alarm created successfully.")

# CloudWatch Alarm for S3 Object Upload Spikes
def create_s3_upload_spike_alarm():
    response = cloudwatch.put_metric_alarm(
        AlarmName='S3ObjectUploadSpikeAlarm',
        MetricName='NumberOfObjects',
        Namespace='AWS/S3',
        Dimensions=[
            {
                'Name': 'BucketName',
                'Value': 'user-log-data-bucket'  # Replace with your S3 bucket name
            },
            {
                'Name': 'StorageType',
                'Value': 'AllStorageTypes'
            },
        ],
        Statistic='Sum',
        Period=300,  # 5-minute period
        EvaluationPeriods=1,
        Threshold=1,  # Set threshold based on expected volume
        ComparisonOperator='GreaterThanOrEqualToThreshold',
        AlarmActions=[
            sns_arn  # Send SNS notification when alarm is triggered
        ],
    )
    print("S3 Upload Spike Alarm created successfully.")

# Create all alarms
def create_all_alarms():
    create_glue_failure_alarm()
    create_authentication_failure_alarm()
    create_s3_upload_spike_alarm()

# Run the script to create alarms
create_all_alarms()
