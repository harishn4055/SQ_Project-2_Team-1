import sys
from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def process_data():
    try:
        # Load data from the Glue catalog
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="log_data_db", table_name="log_data")
        
        # Convert DynamicFrame to DataFrame
        dataframe = dynamic_frame.toDF()

        # Convert UNIX timestamp to a datetime type and create new columns for partitioning
        dataframe = dataframe.withColumn("datetime", from_unixtime(col("timestamp")))
        dataframe = dataframe.withColumn("year", year("datetime"))
        dataframe = dataframe.withColumn("month", month("datetime"))
        dataframe = dataframe.withColumn("day", dayofmonth("datetime"))

        # Filter for security-related events
        filtered_dataframe = dataframe.filter(
            (col("event_type") == "Authentication Failure") |
            (col("event_type") == "Unauthorized Access") |
            (col("event_type") == "Suspicious IP")
        )

        # Write the partitioned DataFrame back to S3 in Parquet format
        # filtered_dataframe.write.partitionBy("year", "month", "day", "event_type").format("parquet").save(
        #     "s3://user-log-data-bucket/processed_security_logs/"
        # )
        filtered_dataframe.write.partitionBy("year", "month", "day", "event_type") \
            .format("parquet") \
            .mode("append") \
            .save("s3://user-log-data-bucket/processed_security_logs")


        job.commit()
        print("Glue ETL job completed successfully.")
    except Exception as e:
        # Log the error and ensure the job marks as failed appropriately
        print(f"An error occurred: {e}")
        raise  # Raising the exception will mark the job as failed in Glue

if __name__ == '__main__':
    process_data()
