# When an AWS Glue job is restarted after failure, it always starts again from the beginning by default. 
# The job does not automatically resume from where it failed, but you can configure it to process only new data by using Job Bookmarks
# or implementing custom checkpointing. Follow below steps
# Job bookmarks work best with append-only datasets where new files or partitions are added over time in source end. 
# If your data is frequently modified or overwritten in source, bookmarks may not prevent reprocessing.

# Set the Job bookmark option to Enable in Glue Job Properties in Console.

#Below is sample code to read s3 folder(no matter the files) and load it into jdbc target.
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.setBookmarkEnable(True)

# S3 source path and JDBC target details
s3_input_path = "s3://your-s3-bucket/your-s3-folder/"
jdbc_connection_name = "your-jdbc-connection-name" # Name of your Glue JDBC connection
jdbc_database_name = "your_database_name"
jdbc_table_name = "your_table_name"

# Read data from S3 with job bookmarks enabled
# 'connectionType': 's3' is implicitly handled when reading from an S3 path
# 'format' should match your S3 data format (e.g., 'csv', 'parquet', 'json')
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_input_path], "recurse": True},
    format="csv", # Adjust this based on your S3 file format
    format_options={"withHeader": True, "separator": ","}, # Example for CSV
    transformation_ctx="datasource0",
    additional_options={"jobBookmarkKeys": ["s3Path"], "jobBookmarkKeysPredicate": "enable"}
)

# You can add transformations here if needed, e.g., mapping, filtering, etc.
# Example: Apply a simple mapping if column names need to be adjusted for the JDBC table
# mapped_frame = ApplyMapping.apply(frame=datasource0, mappings=[
#     ("s3_col1", "string", "jdbc_col1", "string"),
#     ("s3_col2", "int", "jdbc_col2", "int")
# ])

# Write data to JDBC table
datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=datasource0, # Use mapped_frame if transformations were applied
    catalog_connection=jdbc_connection_name,
    connection_options={"dbtable": jdbc_table_name, "database": jdbc_database_name},
    redshift_tmp_dir=args["TempDir"], # Required for Redshift connections
    transformation_ctx="datasink1"
)

job.commit()

job.commit()
