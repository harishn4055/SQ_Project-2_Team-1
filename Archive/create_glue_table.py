import boto3

glue = boto3.client('glue')

bucket_name = 'user-log-data-bucket'  # Replace with your S3 bucket name

table_input = {
    'Name': 'log_data_table',
    'StorageDescriptor': {
        'Columns': [
            {'Name': 'timestamp', 'Type': 'bigint'},
            {'Name': 'log_level', 'Type': 'string'},
            {'Name': 'event_type', 'Type': 'string'},
            {'Name': 'message', 'Type': 'string'},
            {'Name': 'user_id', 'Type': 'int'},
            {'Name': 'source_ip', 'Type': 'string'},
            {'Name': 'destination_ip', 'Type': 'string'},
            {'Name': 'location', 'Type': 'string'},
            {'Name': 'user_agent', 'Type': 'string'},
            {'Name': 'action_taken', 'Type': 'string'}
        ],
        'Location': f's3://{bucket_name}/log_data/',
        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        'Compressed': False,
        'SerdeInfo': {
            'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe'
        },
    },
    'TableType': 'EXTERNAL_TABLE'
}

response = glue.create_table(DatabaseName='log_data_db', TableInput=table_input)

print("Glue table 'log_data_table' created successfully.")
