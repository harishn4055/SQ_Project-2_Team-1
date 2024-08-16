import boto3
from botocore.exceptions import ClientError

def create_sns_topic(topic_name):
    sns = boto3.client('sns')
    try:
        response = sns.create_topic(Name=topic_name)
        topic_arn = response['TopicArn']
        print(f"SNS topic '{topic_name}' created successfully. ARN: {topic_arn}")
        return topic_arn
    except ClientError as e:
        print(f"Failed to create SNS topic: {e}")
        return None

def subscribe_to_topic(topic_arn, email):
    sns = boto3.client('sns')
    try:
        response = sns.subscribe(
            TopicArn=topic_arn,
            Protocol='email',
            Endpoint=email
        )
        subscription_arn = response['SubscriptionArn']
        print(f"Subscription created successfully. Subscription ARN: {subscription_arn}")
        print("Please check your email to confirm the subscription.")
    except ClientError as e:
        print(f"Failed to create subscription: {e}")

# Replace with your desired topic name and email
topic_name = "SecurityAlertsTopic"
email_address = "harishgaddam2k@gmail.com"

topic_arn = create_sns_topic(topic_name)

if topic_arn:
    subscribe_to_topic(topic_arn, email_address)
