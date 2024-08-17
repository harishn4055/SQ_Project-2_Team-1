import boto3
from botocore.exceptions import ClientError

# Initialize the Glue client
glue = boto3.client('glue')

# Step 1: Create Glue Database
database_name = "log_data_db"

def create_database_if_not_exists(db_name):
    try:
        existing_databases = glue.get_databases()
        if not any(db['Name'] == db_name for db in existing_databases['DatabaseList']):
            glue.create_database(DatabaseInput={'Name': db_name})
            print(f"Glue database '{db_name}' created successfully.")
        else:
            print(f"Glue database '{db_name}' already exists.")
    except ClientError as e:
        print(f"Failed to create or check database {db_name}: {e}")

# Step 2: Create Glue Crawler
crawler_name = 'log-data-crawler'
s3_path = 's3://user-log-data-bucket/log_data/' 
role_arn = 'arn:aws:iam::637423377183:role/proj5role'

def create_crawler_if_not_exists(name, role, db_name, s3_path):
    try:
        existing_crawlers = glue.get_crawlers()
        if not any(crawler['Name'] == name for crawler in existing_crawlers['Crawlers']):
            response = glue.create_crawler(
                Name=name,
                Role=role,  
                DatabaseName=db_name,
                Targets={'S3Targets': [{'Path': s3_path}]},
                TablePrefix='', 
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                }
            )
            print(f"Glue Crawler '{name}' created successfully.")
        else:
            print(f"Glue Crawler '{name}' already exists.")
    except ClientError as e:
        print(f"Error creating crawler {name}: {e}")

# Step 3: Start the Glue Crawler
def start_crawler(name):
    try:
        glue.start_crawler(Name=name)
        print(f"Glue Crawler '{name}' started. It will scan the data and update the Glue Data Catalog.")
    except ClientError as e:
        print(f"Failed to start crawler {name}: {e}")

if __name__ == '__main__':
    create_database_if_not_exists(database_name)
    create_crawler_if_not_exists(crawler_name, role_arn, database_name, s3_path)
    start_crawler(crawler_name)
