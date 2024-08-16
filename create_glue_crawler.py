import boto3

# Initialize the Glue client
glue = boto3.client('glue')

# Step 1: Create Glue Database
database_name = "log_data_db"

# Check if the database already exists to avoid errors
existing_databases = glue.get_databases()
if not any(db['Name'] == database_name for db in existing_databases['DatabaseList']):
    glue.create_database(DatabaseInput={'Name': database_name})
    print(f"Glue database '{database_name}' created successfully.")
else:
    print(f"Glue database '{database_name}' already exists.")

# Step 2: Create Glue Crawler
crawler_name = 'log-data-crawler'
s3_path = 's3://user-log-data-bucket/log_data/' 

# Check if the Crawler already exists
existing_crawlers = glue.get_crawlers()
if not any(crawler['Name'] == crawler_name for crawler in existing_crawlers['Crawlers']):
    response = glue.create_crawler(
        Name=crawler_name,
        Role='arn:aws:iam::637423377183:role/proj5role',  
        DatabaseName=database_name,
        Targets={
            'S3Targets': [
                {
                    'Path': s3_path
                }
            ]
        },
        TablePrefix='', 
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        }
    )
    print(f"Glue Crawler '{crawler_name}' created successfully.")
else:
    print(f"Glue Crawler '{crawler_name}' already exists.")

# Step 3: Start the Glue Crawler
glue.start_crawler(Name=crawler_name)
print(f"Glue Crawler '{crawler_name}' started. It will scan the data and update the Glue Data Catalog.")
