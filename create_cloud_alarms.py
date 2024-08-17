import boto3

# Initialize the CloudWatch client
cloudwatch_client = boto3.client('cloudwatch')

# Create CloudWatch alarm for Glue job failures
def create_glue_failure_alarm(glue_job_name, sns_topic_arn):
    alarm_name = f"GlueJobFailureAlarm-{glue_job_name}"
    
    response = cloudwatch_client.put_metric_alarm(
        AlarmName=alarm_name,
        MetricName='GlueJobFailed',
        Namespace='AWS/Glue',
        Statistic='Sum',
        Period=60,  # 1 minute
        Threshold=1,
        ComparisonOperator='GreaterThanOrEqualToThreshold',
        EvaluationPeriods=1,
        AlarmActions=[sns_topic_arn],
        Dimensions=[
            {
                'Name': 'JobName',
                'Value': glue_job_name  
            }
        ]
    )
    print(f"CloudWatch alarm '{alarm_name}' created for Glue job failures.")

# Create CloudWatch alarm for Glue job being stopped
def create_glue_stopped_alarm(glue_job_name, sns_topic_arn):
    alarm_name = f"GlueJobStoppedAlarm-{glue_job_name}"
    
    response = cloudwatch_client.put_metric_alarm(
        AlarmName=alarm_name,
        MetricName='GlueJobStopped',
        Namespace='AWS/Glue',
        Statistic='Sum',
        Period=60,  # 1 minutes
        Threshold=1,
        ComparisonOperator='GreaterThanOrEqualToThreshold',
        EvaluationPeriods=1,
        AlarmActions=[sns_topic_arn],
        Dimensions=[
            {
                'Name': 'JobName',
                'Value': glue_job_name  
            }
        ]
    )
    print(f"CloudWatch alarm '{alarm_name}' created for Glue job being stopped.")

# Main execution
if __name__ == "__main__":

    glue_job_name = 'LogDataETLJob'
    sns_topic_arn = 'arn:aws:sns:us-west-1:637423377183:SecurityAlertsTopic'

    # Step 1: Create a CloudWatch alarm for Glue job failures
    create_glue_failure_alarm(glue_job_name, sns_topic_arn)

    # Step 2: Create a CloudWatch alarm for Glue job being stopped
    create_glue_stopped_alarm(glue_job_name, sns_topic_arn)

    print("Glue job failure and stopped alarms created successfully.")
