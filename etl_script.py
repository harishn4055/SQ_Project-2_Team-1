import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from the Glue catalog
log_data = glueContext.create_dynamic_frame.from_catalog(database = "log_data_db", table_name = "log_data")

# Filter log data for security-related events
filtered_log_data = Filter.apply(frame = log_data, f = lambda x: x["event_type"] in ["Authentication Failure", "Unauthorized Access", "Suspicious IP"])

# Write filtered data back to S3
glueContext.write_dynamic_frame.from_options(
    frame = filtered_log_data, 
    connection_type = "s3", 
    connection_options = {"path": "s3://user-log-data-bucket/processed_security_logs/"}, 
    format = "json"
)

job.commit()

print("Glue ETL job completed successfully.")
