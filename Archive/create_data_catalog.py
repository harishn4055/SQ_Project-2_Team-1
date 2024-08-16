import boto3

glue = boto3.client('glue')

# Create Glue database
database_name = "log_data_db"
glue.create_database(DatabaseInput={'Name': database_name})

print(f"Glue database '{database_name}' created successfully.")
