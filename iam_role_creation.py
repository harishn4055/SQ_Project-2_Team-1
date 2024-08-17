import boto3
import json
from botocore.exceptions import ClientError

def create_iam_role(role_name):
    # Create the IAM client
    iam_client = boto3.client('iam')

    # Check if the role already exists
    try:
        response = iam_client.get_role(RoleName=role_name)
        role_arn = response['Role']['Arn']
        print(f"Role {role_name} already exists, using the existing role.")
        return role_arn
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print(f"Role {role_name} does not exist, creating a new one.")
            
            # Define the IAM policy
            assume_role_policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }

            # Create the IAM role
            role = iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
            )

            # Attach the AWSLambdaBasicExecutionRole policy for Lambda execution and logging
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
            )

            # Attach the AmazonS3FullAccess policy for S3 access
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
            )

            # Attach the AmazonSNSFullAccess policy for SNS access
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonSNSFullAccess'
            )

            # Attach the AWSGlueServiceRole policy for interacting with AWS Glue
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
            )

            # Attach the AWSGlueConsoleFullAccess policy for interacting with AWS Glue
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess'
            )

            # Attach the AWSLambdaExecute policy to allow S3 to invoke the Lambda function
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/AWSLambdaExecute'
            )

            # Attach the AWSGlueServiceRole policy for interacting with Amazon DynamoDb
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess'
            )
            
            # Attach the CloudWatchFullAccess policy for extended CloudWatch interactions
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/CloudWatchFullAccess'
            )

            role_arn = role['Role']['Arn']
            print(f"Role {role_name} created successfully.")
            return role_arn
        else:
            print(f"Unexpected error: {e}")
            raise

# Example of how to use the function
if __name__ == "__main__":
    role_name = 'proj5role'
    role_arn = create_iam_role(role_name)
    print(f"Role ARN: {role_arn}")
