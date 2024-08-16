import boto3

cloudwatch = boto3.client('cloudwatch')

def simulate_glue_job_failure():
    response = cloudwatch.put_metric_data(
        Namespace='Glue',  # Correct namespace for AWS Glue metrics
        MetricData=[
            {
                'MetricName': 'GlueJobRunFailures',  # Make sure this matches the metric name in your alarm
                'Dimensions': [
                    {
                        'Name': 'JobName',
                        'Value': 'LogDataETLJob'  # Replace with your Glue job name
                    },
                ],
                'Value': 1,
                'Unit': 'Count'
            },
        ]
    )
    print("Simulated Glue Job Failure.")

simulate_glue_job_failure()
