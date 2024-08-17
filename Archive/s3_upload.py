import boto3

def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Create an S3 client
    s3_client = boto3.client('s3')

    try:
        # Upload the file
        s3_client.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True

# Define your bucket name
bucket_name = 'user-log-data-bucket'

# Define files and corresponding S3 keys
files_to_upload = {
    'log_data_1999.json': 'log_data/log_data_1999.json',
    'log_data_2000.json': 'log_data/log_data_2000.json',
    'log_data_2001.json': 'log_data/log_data_2001.json',
    'log_data_2002.json': 'log_data/log_data_2002.json'
}

# Upload files
upload_success = True
for local_file, s3_key in files_to_upload.items():
    success = upload_file_to_s3(local_file, bucket_name, s3_key)
    if not success:
        print(f"Failed to upload {local_file}")
        upload_success = False

if upload_success:
    print("All files uploaded successfully!")
else:
    print("Failed to upload one or more files.")
