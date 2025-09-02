#Some Performance Improvements Techniques for Reading Large CSV Files in PySpark

#Define Schema Manually
#Avoid inferSchema=True — it's slow because Spark scans part of the data.

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
    # Add other fields...
])

df = spark.read.csv("path/to/file.csv", header=True, schema=schema)


#Increase Parallelism via Repartitioning
#Repartition to utilize more CPU cores across nodes.

df = df.repartition(200)  # Tune based on your cluster size
If reading from HDFS/S3, Spark will automatically parallelize based on block size (~128MB per block), but repartitioning can help for skewed data or uneven workloads.

#Tune File Input Settings
#Optimize Spark reader settings:

df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("multiLine", "false") \
    .option("mode", "DROPMALFORMED") \
    .schema(schema) \
    .csv("path/to/100gb.csv")


#Use Column Pruning
#If you don’t need all columns, select only the ones you need when reading:


selected_columns = ["id", "amount"]
df = spark.read.csv("path/to/file.csv", header=True, schema=schema).select(*selected_columns)


#Use .persist() or .cache() Wisely
#If you're reusing the DataFrame multiple times in your job:


df.cache()  # or df.persist(StorageLevel.MEMORY_AND_DISK)

#Filter Early (Predicate Pushdown)
#Apply filters immediately after loading to reduce data size early:


df_filtered = df.filter("amount > 1000")


#Avoid Wide Transformations Early
#Narrow transformations (like filter, select) are preferred early on. Avoid expensive groupBy or join operations until data is filtered down.


#Tune Executor and Driver Configs
#In Spark config:


--executor-memory 8G
--executor-cores 4
--num-executors 50
Or in code:


SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "200") \


#Use Data Skipping (with Parquet/Delta)
#If you're repeatedly reading the data, consider converting the CSV to Parquet or Delta for much faster reads:


df.write.parquet("path/output_parquet/")
df_parquet = spark.read.parquet("path/output_parquet/")
Parquet supports columnar storage and predicate pushdown.

#Avoid collect() or .show() on Big Data
#Never use:


df.show(100000)
df.collect()
These bring data to the driver and can crash your job.

#Optional: Chunk the Input
#If possible, split the 100GB file into multiple smaller CSV files, and place them in a directory. Spark will read them in parallel automatically.

bash
Copy
Edit
/path/to/csvs/
  ├── part-000.csv
  ├── part-001.csv
  └── ...
Then read like:


df = spark.read.schema(schema).csv("/path/to/csvs/*.csv", header=True)
🧪 Final Tip: Monitor with Spark UI
Check your job in the Spark UI (port 4040) to identify:

Stage time bottlenecks

Skewed partitions

Task failures or GC overhead