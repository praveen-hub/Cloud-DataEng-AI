import sys
import boto3
import logging
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Spark session in Glue
spark = SparkSession.builder.appName("parquettocsv").getOrCreate()

# Retrieving parameters
args = getResolvedOptions(sys.argv, ['source_path', 'destination_path'])
read_bucket_name = args['source_path']
write_bucket_name = args['destination_path']

# read bucket Configuration
s3_path_read = read_bucket_name
print("The Read Bucket Path is ", s3_path_read)

# write bucket Configuration
s3_path_write = write_bucket_name
print("The Write Bucket Path is ", s3_path_write)

# Parsing the read path
s3_url = urlparse(args['source_path'])
bucket = s3_url.netloc
prefix = s3_url.path.lstrip('/')

# Using boto3 to list all the csv files in the s3 bucket.
s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
parquet_files = []
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get('Contents', []):
        key = obj['Key']
        if key.endswith('.parquet'):
            parquet_files.append(f"s3://{bucket}/{key}")

print(f"Found {len(parquet_files)} Parquet files in to process:\n" + "\n".join(f" - {f}" for f in parquet_files))

if not parquet_files:
    logger.info(f"No files to process in {s3_url}, exiting gracefully")
else:
    df = spark.read.parquet(s3_path_read, sep='|~|', header=False, nullValue="NULL", quote='\u0000')

    # calculating the number of partitions
    print("calculating the number of partitions for writing csv file.....")
    df_sample = df.sample(fraction=0.01)
    sample_size = df_sample.rdd.map(lambda row: sys.getsizeof(row)).sum()
    estimated_df_size = sample_size/(0.01)
    estimated_csv_size = estimated_df_size * 0.3
    num_partitions = max(1, int(estimated_csv_size/(256*1024*1024)))

    # write data to s3 bucket
    print("writing data into S3 path.....")
    df.repartition(num_partitions).write.csv(s3_path_write, mode="append")
    print(f"Parquet files generated successfully for {s3_path_read}.....")
